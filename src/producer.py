import time
import json
import random
from kafka import KafkaProducer
from river import datasets

# --- CONFIGURATION ---
TOPIC_NAME = 'network-stream'
DRIFT_POSITION = 1000  # The point where the "rules" change
TOTAL_SAMPLES = 2000   # Total data points to send

# 1. Setup Kafka Producer
# This connects to your local server and prepares to send JSON data
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# 2. Define the Data Concepts (The "Science" Part)
# We use the 'Agrawal' dataset generator, commonly used in research papers for drift.
# Concept A: The initial "Normal" behavior (Function 0)
concept_a = datasets.synth.Agrawal(classification_function=0, seed=42)

# Concept B: The "Drifted" behavior (Function 2 - different rules)
concept_b = datasets.synth.Agrawal(classification_function=2, seed=42)

# 3. Create the Drifting Stream
# This utility automatically switches from A to B at 'position=1000'
stream = datasets.synth.ConceptDriftStream(
    stream=concept_a,
    drift_stream=concept_b,
    position=DRIFT_POSITION,
    width=50  # The transition happens over 50 samples (smooth drift)
)

print(f"🚀 Starting Data Stream to topic '{TOPIC_NAME}'...")
print(f"ℹ️  Normal Phase: 0 - {DRIFT_POSITION}")
print(f"⚠️  Drift Phase: {DRIFT_POSITION} - {TOTAL_SAMPLES}")

# 4. Generate and Send Data
for i, (x, y) in enumerate(stream.take(TOTAL_SAMPLES)):
    
    # Inject Artificial Anomalies (1% chance)
    is_anomaly = False
    if random.random() < 0.01:
        # Create a "weird" data point (Outlier)
        x['salary'] = 999999  # Example: Impossible salary
        x['age'] = 200        # Example: Impossible age
        is_anomaly = True

    # Construct the message packet
    data_packet = {
        'id': i,
        'features': x,       # The data (age, salary, loan, etc.)
        'ground_truth': y,   # The actual label (for checking accuracy later)
        'is_anomaly': is_anomaly, 
        'drift_active': i > DRIFT_POSITION # A flag just for our debugging
    }

    # Send to Kafka
    producer.send(TOPIC_NAME, value=data_packet)

    # Print progress every 100 samples
    if i % 100 == 0:
        status = "NORMAL" if i < DRIFT_POSITION else "DRIFTED"
        print(f"[{i}/{TOTAL_SAMPLES}] Status: {status} | Sent: {x}")
        
    # Sleep slightly to simulate real-time streaming (0.05s)
    time.sleep(0.05) 

producer.flush()
print("✅ Stream Finished! All data sent to Kafka.")
