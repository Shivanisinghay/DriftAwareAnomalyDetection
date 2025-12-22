import time
import json
import pandas as pd
from kafka import KafkaProducer

# --- CONFIGURATION ---
TOPIC_NAME = 'ids-stream'
FILES = [
    ('data/Friday-WorkingHours-Morning.pcap_ISCX.csv', 'NORMAL'),
    ('data/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv', 'ATTACK')
]
ROWS_PER_FILE = 5000  # Smaller chunks for a faster, punchier demo

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("\n✨ SYSTEM READY: WAITING FOR COMMAND...")

for file_path, label_type in FILES:
    # --- THE INTERACTIVE PART ---
    if label_type == 'ATTACK':
        input(f"\n⚠️  WARNING: Ready to inject {label_type} traffic. Press [ENTER] to LAUNCH ATTACK >> ")
    else:
        input(f"\nℹ️  Ready to stream {label_type} traffic. Press [ENTER] to START >> ")
        
    print(f"🚀 Streaming {label_type} data from {file_path}...")
    
    try:
        df = pd.read_csv(file_path, nrows=ROWS_PER_FILE)
        df.columns = df.columns.str.strip() # Fix spaces
        
        for i, row in df.iterrows():
            record = row.to_dict()
            ground_truth = 0 if record.get('Label') == 'BENIGN' else 1
            
            msg = {
                'id': i,
                'features': record,
                'ground_truth': ground_truth,
                'type': label_type # Send the type so our dashboard knows!
            }
            producer.send(TOPIC_NAME, value=msg)
            
            # Fast but visible speed
            if i % 100 == 0:
                time.sleep(0.01) 
                
        producer.flush()
        print(f"✅ {label_type} Stream Complete.")

    except Exception as e:
        print(f"❌ Error: {e}")

print("\n🏁 DEMO COMPLETE.")