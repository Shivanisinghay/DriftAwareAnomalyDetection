import time
import json
import random
import pandas as pd
from kafka import KafkaProducer

# --- CONFIGURATION ---
TOPIC_NAME = 'ids-stream'
TOTAL_DURATION = 100000  # Increased from 50,000 to 100,000

# Helper function to find specific attacks deep in the file
def load_specific_traffic(filepath, target_label, limit=25000): # Increased limit
    print(f"🔎 Deep Scanning {filepath} for '{target_label}'...")
    collected = []
    
    try:
        for chunk in pd.read_csv(filepath, chunksize=50000):
            chunk.columns = chunk.columns.str.strip()
            if 'Label' not in chunk.columns: continue
            chunk['Label'] = chunk['Label'].astype(str).str.strip()
            
            attacks = chunk[chunk['Label'] == target_label]
            if not attacks.empty:
                collected.extend(attacks.to_dict('records'))
                print(f"   ...found {len(collected)} / {limit} packets...")
                
            if len(collected) >= limit:
                break
    except Exception as e:
        print(f"⚠️ Warning reading file: {e}")

    print(f"✅ Total found: {len(collected)} '{target_label}' packets.")
    return collected[:limit]

# 1. Load Datasets
print("⏳ Initializing Data Loader...")

# Load Normal (Need more for a long run)
print("   Loading Normal traffic...")
df_normal = pd.read_csv('data/Friday-WorkingHours-Morning.pcap_ISCX.csv', nrows=50000)
df_normal.columns = df_normal.columns.str.strip()
normal_pool = df_normal.to_dict('records')

# Load Attacks (Fetch 25,000 of each to handle the longer stream)
scan_pool = load_specific_traffic('data/Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv', 'PortScan', limit=25000)
ddos_pool = load_specific_traffic('data/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv', 'DDoS', limit=25000)

if len(ddos_pool) == 0:
    print("❌ ERROR: No DDoS packets found.")
    exit()

print(f"\n🚀 DATA READY: {len(normal_pool)} Normal, {len(scan_pool)} Scans, {len(ddos_pool)} DDoS")

# 2. Setup Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def send_record(record, i, scenario_label):
    lbl = record.get('Label', 'BENIGN')
    ground_truth = 0 if lbl == 'BENIGN' else 1
    
    msg = {
        'id': i,
        'features': record,
        'ground_truth': ground_truth,
        'scenario': scenario_label
    }
    producer.send(TOPIC_NAME, value=msg)

# 3. The Extended Scenario Loop
print(f"▶️ Starting Extended Stream ({TOTAL_DURATION} samples)...")

for i in range(TOTAL_DURATION):
    
    # PHASE 1: Calibration (0 - 10,000)
    # Longer "Normal" phase to let the Sliding Window fully settle
    if i < 10000:
        record = random.choice(normal_pool)
        scenario = "Normal (Calibration)"
        
    # PHASE 2: Gradual Drift (10,000 - 30,000) -> PortScans
    # A very slow, sneaky attack buildup over 20,000 samples
    elif i < 30000:
        scenario = "Gradual Drift (PortScan)"
        # Scale progress from 0.0 to 1.0 over the 20k range
        progress = (i - 10000) / 20000 
        if random.random() < (progress * 0.5): 
            record = random.choice(scan_pool)
        else:
            record = random.choice(normal_pool)

    # PHASE 3: Recovery (30,000 - 40,000)
    # A break to let models recover before the storm
    elif i < 40000:
        scenario = "Normal (Recovery)"
        record = random.choice(normal_pool)

    # PHASE 4: Sudden Drift (40,000 - 60,000) -> DDoS
    # Massive storm for 20,000 samples
    elif i < 60000:
        scenario = "Sudden Drift (DDoS)"
        if random.random() < 0.8:
            record = random.choice(ddos_pool)
        else:
            record = random.choice(normal_pool)
            
    # PHASE 5: Long-Term Stability (60,000+)
    else:
        scenario = "Normal (End)"
        record = random.choice(normal_pool)

    send_record(record, i, scenario)

    # Print status less often to save terminal space
    if i % 1000 == 0:
        print(f"[{i}/{TOTAL_DURATION}] {scenario}")
        time.sleep(0.01)

producer.flush()
print("🏁 Extended Stream Complete.")