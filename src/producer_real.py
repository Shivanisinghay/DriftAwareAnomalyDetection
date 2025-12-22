import time
import json
import pandas as pd
from kafka import KafkaProducer

# --- CONFIGURATION ---
TOPIC_NAME = 'ids-stream'

# We chain these two files to force a massive drift event
FILES_TO_PLAY = [
    # File 1: Mostly BENIGN (The "Training" Phase)
    'data/Friday-WorkingHours-Morning.pcap_ISCX.csv',
    
    # File 2: Heavy DDoS Attack (The "Drift" Phase)
    'data/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv'
]

ROWS_PER_FILE = 20000  # How many rows to read from each file

# 1. Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

global_id_counter = 0

print(f"🚀 Starting Multi-Stage Data Stream to '{TOPIC_NAME}'...")

for file_path in FILES_TO_PLAY:
    print(f"\n📂 SWITCHING TRACK: Loading {file_path}...")
    
    try:
        # Read a chunk of the file
        df = pd.read_csv(file_path, nrows=ROWS_PER_FILE)
        
        # CLEANING: Strip spaces from column names (Fixes ' Label' issue)
        df.columns = df.columns.str.strip()
        
        # Verify Label Column Exists
        if 'Label' not in df.columns:
            print(f"⚠️  Skipping file {file_path} - 'Label' column not found!")
            continue

        print(f"▶️  Streaming {len(df)} rows...")

        for i, row in df.iterrows():
            record = row.to_dict()
            
            # Label Encoding
            # BENIGN = 0 (Normal)
            # Anything else (DDoS, Bot, PortScan) = 1 (Attack)
            raw_label = record.get('Label', 'UNKNOWN')
            ground_truth = 0 if raw_label == 'BENIGN' else 1
            
            # Prepare packet
            msg = {
                'id': global_id_counter,
                'features': record,
                'ground_truth': ground_truth,
                'source_file': file_path  # Helpful for debugging
            }
            
            producer.send(TOPIC_NAME, value=msg)
            global_id_counter += 1

            # Speed Control & Logging
            if i % 1000 == 0:
                print(f"Sent {i} records from current file... (Total: {global_id_counter})")
                time.sleep(0.05) # Fast enough for IEEE demo, slow enough to see the graph

        producer.flush()
        print(f"✅ Finished {file_path}")
        
        # Optional: Add a small pause between files to make the transition clear on the graph
        print("⏳ Switching context (Simulating time gap)...")
        time.sleep(2)

    except FileNotFoundError:
        print(f"❌ ERROR: File not found: {file_path}")
    except Exception as e:
        print(f"❌ ERROR processing {file_path}: {e}")

print("\n🎉 ALL DATA SENT. EXPERIMENT COMPLETE.")