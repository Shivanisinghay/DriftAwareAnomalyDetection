import pandas as pd
from kafka import KafkaProducer
import json
import time

# --- CONFIGURATION ---
TOPIC_NAME = 'network-stream'
PRODUCER_DELAY = 0.01  # Simulated real-time delay

# 1. Load and clean datasets
df_morning = pd.read_csv('data/Friday-WorkingHours-Morning.pcap_ISCX.csv')
df_afternoon = pd.read_csv('data/Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv')

# Remove leading spaces from headers
df_morning.columns = df_morning.columns.str.strip()
df_afternoon.columns = df_afternoon.columns.str.strip()

# Helper for labeling
def preprocess(df):
    df.replace([float('inf'), float('-inf')], 0, inplace=True)
    df.fillna(0, inplace=True)
    df['Label'] = df['Label'].apply(lambda x: 0 if x == 'BENIGN' else 1)
    return df

df_morning = preprocess(df_morning)
df_afternoon = preprocess(df_afternoon)

# 2. Define the Multi-Phase Stream
# Phase 1: Pure Benign (10,000 samples)
phase_1 = df_morning.iloc[:10000]

# Phase 2: Sudden Drift - PortScan Attack (10,000 samples)
phase_2 = df_afternoon.iloc[:10000]

# Phase 3: Recovery - Return to Benign (5,000 samples)
phase_3 = df_morning.iloc[10000:15000]

# Phase 4: Intermittent/Mixed (10,000 samples)
phase_4 = pd.concat([df_morning.iloc[15000:20000], df_afternoon.iloc[10000:15000]]).sample(frac=1)

# Combine into one master stream
full_stream = pd.concat([phase_1, phase_2, phase_3, phase_4], ignore_index=True)

# 3. Stream to Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092')

print("🚀 Starting Multi-Phase Research Stream...")
for i, row in full_stream.iterrows():
    data = {
        "id": i, 
        "features": row.drop('Label').to_dict(), 
        "ground_truth": int(row['Label'])
    }
    producer.send(TOPIC_NAME, json.dumps(data).encode('utf-8'))
    
    if i % 1000 == 0:
        print(f"Sent {i} flows. Current Phase size: {len(full_stream)}")
    time.sleep(PRODUCER_DELAY)