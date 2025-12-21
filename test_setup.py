import river
import kafka
import numpy as np

print("--- System Check ---")
print(f"River Version: {river.__version__}")
print(f"NumPy Version: {np.__version__}")
print(f"Kafka Python Version: {kafka.__version__}")

try:
    # Try to connect to the running Kafka server
    producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092')
    print("SUCCESS: Connected to Kafka Server!")
    producer.close()
except Exception as e:
    print("ERROR: Could not connect to Kafka. Is the server running in the other terminal?")
    print(f"Details: {e}")

print("--------------------")