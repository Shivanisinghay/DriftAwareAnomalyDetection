import json
import time
import matplotlib.pyplot as plt
from collections import deque
from kafka import KafkaConsumer
from river import forest, metrics, tree

# --- CONFIGURATION ---
TOPIC_NAME = 'ids-stream'
WINDOW_SIZE = 50

# --- CUSTOM UTILS ---
class SlidingWindowModel:
    """
    Manually implements a Sliding Window approach.
    It stores the last N samples and periodically retrains a fresh model 
    on that buffer. This simulates a standard 'Batch Retraining' system.
    """
    def __init__(self, model_builder, window_size=1000, retrain_every=50):
        self.model_builder = model_builder # Function to create a fresh model
        self.window_size = window_size
        self.retrain_every = retrain_every
        self.buffer = deque(maxlen=window_size)
        self.model = model_builder()
        self.counter = 0

    def predict_one(self, x):
        # Predict using the current active model
        return self.model.predict_one(x)

    def learn_one(self, x, y):
        # 1. Add new sample to the sliding window buffer
        self.buffer.append((x, y))
        self.counter += 1
        
        # 2. Periodically Retrain (Simulating Batch Update)
        # We don't retrain on EVERY sample (too slow), but every 50.
        if self.counter % self.retrain_every == 0:
            # Create a brand new model
            self.model = self.model_builder()
            # Train it on everything currently in the window
            for bx, by in self.buffer:
                self.model.learn_one(bx, by)

# --- SETUP MODELS ---

# 1. Adaptive Model (Proposed Solution)
# ARF with internal drift detection (ADWIN)
model_adaptive = forest.ARFClassifier(n_models=10, seed=42)
metric_adaptive = metrics.CohenKappa()

# 2. Static Baseline (Frozen after 2000 samples)
model_static = forest.ARFClassifier(n_models=10, seed=42)
metric_static = metrics.CohenKappa()

# 3. Sliding Window Baseline (The Industry Standard)
# Uses our custom class above. Retrains a Hoeffding Tree on last 1000 samples.
model_window = SlidingWindowModel(
    model_builder=lambda: tree.HoeffdingTreeClassifier(), 
    window_size=1000,
    retrain_every=50
)
metric_window = metrics.CohenKappa()

# --- SETUP PLOT ---
plt.style.use('dark_background')
plt.ion()
fig, ax = plt.subplots(figsize=(10, 6))
ax.set_title("IEEE Benchmark: Adaptive vs Static vs Sliding Window")
ax.set_ylabel("Cohen's Kappa Score")
ax.set_xlabel("Stream Processing Timeline (Samples)")
ax.set_ylim(-0.1, 1.1)
ax.grid(True, alpha=0.2)

# Create the 3 lines
line_adapt, = ax.plot([], [], '-g', label='Adaptive ARF (Proposed)', linewidth=2.5)
line_static, = ax.plot([], [], '--r', label='Static Baseline', linewidth=1.5, alpha=0.8)
line_window, = ax.plot([], [], ':y', label='Sliding Window (Batch)', linewidth=1.5, alpha=0.9)
ax.legend(loc='lower left')

x_data, y_adapt, y_static, y_window = [], [], [], []

# --- KAFKA CONSUMER ---
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

print("🚀 Listening for Complex Stream...")
count = 0

for msg in consumer:
    record = msg.value
    X = record['features']
    y_true = record['ground_truth']
    
    # Data Cleaning
    for bad in ['Label', 'label', 'class', 'id', 'Timestamp']:
        X.pop(bad, None)
    clean_X = {k: float(v) for k, v in X.items() if isinstance(v, (int, float, str)) and str(v).replace('.','',1).isdigit()}

    # --- 1. PREDICT & UPDATE METRICS (TEST THEN TRAIN) ---
    
    # Adaptive
    y_p_a = model_adaptive.predict_one(clean_X)
    metric_adaptive.update(y_true, y_p_a)
    model_adaptive.learn_one(clean_X, y_true)
    
    # Static
    y_p_s = model_static.predict_one(clean_X)
    metric_static.update(y_true, y_p_s)
    if count < 2000: # Train only initially
        model_static.learn_one(clean_X, y_true)
        
    # Sliding Window
    y_p_w = model_window.predict_one(clean_X)
    metric_window.update(y_true, y_p_w)
    model_window.learn_one(clean_X, y_true) # Updates buffer & retrains

    count += 1

    # --- 2. UPDATE GRAPH ---
    if count % WINDOW_SIZE == 0:
        ka = metric_adaptive.get()
        ks = metric_static.get()
        kw = metric_window.get()
        
        # Log to terminal
        print(f"Sample {count} | Adapt: {ka:.3f} | Static: {ks:.3f} | Window: {kw:.3f}")
        
        # Update lists
        x_data.append(count)
        y_adapt.append(ka)
        y_static.append(ks)
        y_window.append(kw)
        
        # Keep plot clean (limit history)
        if len(x_data) > 2000:
            x_data.pop(0); y_adapt.pop(0); y_static.pop(0); y_window.pop(0)

        # Update Lines
        line_adapt.set_data(x_data, y_adapt)
        line_static.set_data(x_data, y_static)
        line_window.set_data(x_data, y_window)
        
        # Auto-scale axis
        ax.relim()
        ax.autoscale_view()
        plt.draw()
        plt.pause(0.001)