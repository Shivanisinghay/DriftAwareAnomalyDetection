import json
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
from river import forest, metrics

# --- CONFIGURATION ---
TOPIC_NAME = 'network-stream'
WINDOW_SIZE = 50  # Update the graph every 50 samples

# 1. Setup the Adaptive Model (The "Brain")
# We use Adaptive Random Forest (ARF). It is an "Ensemble" (many trees).
# It has built-in Drift Detection (ADWIN) inside every tree.
model = forest.ARFClassifier(n_models=10, seed=42)

# 2. Setup Metrics (The "Report Card")
# We track Accuracy. Prequential means "Test-then-Train" accuracy.
metric = metrics.Accuracy()

# 3. Setup Kafka Consumer (The "Ear")
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest' # Start reading from new data
)

# 4. Setup Live Plotting (The "Dashboard")
plt.ion()  # Interactive mode on
fig, ax = plt.subplots(figsize=(10, 5))
x_data, y_data = [], []
line, = ax.plot(x_data, y_data, '-g', label='Model Accuracy')

plt.title('Real-Time Adaptive Anomaly Detection')
plt.xlabel('Samples Processed')
plt.ylabel('Accuracy')
plt.ylim(0, 1.1)
plt.grid(True)
plt.legend()

print(f"👀 Listening for data on topic '{TOPIC_NAME}'...")

# 5. Main Processing Loop
for msg in consumer:
    record = msg.value
    
    # Extract data from the packet
    X = record['features']       # The input (Age, Salary, etc.)
    y_true = record['ground_truth'] # The actual answer (0 or 1)
    
    # --- CORE LOGIC START ---
    
    # A. Predict (Before seeing the answer)
    y_pred = model.predict_one(X)
    
    # B. Update the Metric (Did we get it right?)
    metric.update(y_true, y_pred)
    
    # C. Learn (Train the model on this new sample)
    model.learn_one(X, y_true)
    
    # --- CORE LOGIC END ---

    # Update the graph periodically
    sample_id = record['id']
    if sample_id % WINDOW_SIZE == 0:
        current_acc = metric.get()
        print(f"sample {sample_id} | Accuracy: {current_acc:.2%}")
        
        # Append data to plot
        x_data.append(sample_id)
        y_data.append(current_acc)
        
        # Refresh the chart
        line.set_xdata(x_data)
        line.set_ydata(y_data)
        ax.relim()
        ax.autoscale_view()
        plt.draw()
        plt.pause(0.01)

    # Stop if we hit the limit (optional)
    if sample_id >= 1999:
        print("Experiment Complete.")
        break

plt.ioff()
plt.show()
