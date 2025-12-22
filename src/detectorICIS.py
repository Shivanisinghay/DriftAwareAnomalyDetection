import json
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
from river import forest, metrics, tree

# --- CONFIGURATION ---
TOPIC_NAME = 'ids-stream'
WINDOW_SIZE = 100

# 1. Setup TWO Models
# Model A: Your Proposed Solution (Drift-Aware)
adaptive_model = forest.ARFClassifier(n_models=10, seed=42)

# Model B: The Baseline (Static / No Drift Detection)
# We use a simple Hoeffding Tree that tries to learn but handles drift poorly compared to ARF
# OR we can simulate a static model by just not calling 'learn_one' after the first 1000 samples.
# Here, we use a basic Hoeffding Tree as a "weaker" online learner for comparison.
static_model = tree.HoeffdingTreeClassifier() 

# 2. Setup Metrics for BOTH
metric_adaptive = metrics.Accuracy()
metric_static = metrics.Accuracy()

# 3. Setup Kafka
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

# 4. Setup Plotting with TWO Lines
plt.ion()
fig, ax = plt.subplots(figsize=(12, 6))

x_data, y_adaptive, y_static = [], [], []

# Line 1: Your Model (Green)
line_adaptive, = ax.plot([], [], '-g', label='Proposed: Drift-Aware ARF', linewidth=2)
# Line 2: Baseline (Red, Dashed)
line_static, = ax.plot([], [], '--r', label='Baseline: Standard HT', linewidth=1.5)

plt.title('Performance Comparison: Drift-Aware vs. Standard Model')
plt.xlabel('Samples Processed')
plt.ylabel('Real-Time Accuracy')
plt.ylim(0.4, 1.05) # Zoom in on the top half
plt.grid(True, alpha=0.5)
plt.legend()

print(f"👀 Listening to {TOPIC_NAME}...")

# 5. Main Loop
count = 0
for msg in consumer:
    record = msg.value
    X = record['features']
    y_true = record['ground_truth']
    
    # Clean data (same as before)
    for bad in ['Label', 'label', 'class', 'id', 'Timestamp']:
        X.pop(bad, None)
    
    clean_X = {}
    for k, v in X.items():
        try: clean_X[k] = float(v)
        except: continue

    # --- MODEL A (Your Solution) ---
    y_pred_a = adaptive_model.predict_one(clean_X)
    metric_adaptive.update(y_true, y_pred_a)
    adaptive_model.learn_one(clean_X, y_true)

    # --- MODEL B (Baseline) ---
    # We train this one normally, but since it's a single tree (not an ensemble), 
    # it is naturally weaker against drift.
    y_pred_b = static_model.predict_one(clean_X)
    metric_static.update(y_true, y_pred_b)
    static_model.learn_one(clean_X, y_true)

    count += 1

    # Update Plot
    if count % WINDOW_SIZE == 0:
        acc_a = metric_adaptive.get()
        acc_b = metric_static.get()
        
        print(f"Sample {count} | Adaptive: {acc_a:.2%} | Baseline: {acc_b:.2%}")
        
        x_data.append(count)
        y_adaptive.append(acc_a)
        y_static.append(acc_b)
        
        line_adaptive.set_data(x_data, y_adaptive)
        line_static.set_data(x_data, y_static)
        
        ax.relim()
        ax.autoscale_view()
        plt.draw()
        plt.pause(0.001)
        
    if count >= 30000: break

plt.ioff()
plt.show()