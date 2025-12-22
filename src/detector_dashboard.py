import json
import time
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
from river import forest, metrics
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.console import Console
from rich import box

# --- CONFIGURATION ---
TOPIC_NAME = 'ids-stream'
WINDOW_SIZE = 50

# Setup Models
adaptive_model = forest.ARFClassifier(n_models=10, seed=42)
static_model = forest.ARFClassifier(n_models=10, seed=42)
metric_adapt = metrics.CohenKappa()
metric_static = metrics.CohenKappa()

# Setup Plot
plt.style.use('dark_background') # Looks cooler/hacker-like
plt.ion()
fig, ax = plt.subplots(figsize=(8, 4))
fig.canvas.manager.set_window_title("Real-Time Accuracy Monitor")
x_data, y_adapt, y_static = [], [], []
line_adapt, = ax.plot([], [], '-g', label='Adaptive (Pro)', linewidth=2)
line_static, = ax.plot([], [], '--r', label='Static (Base)', linewidth=1.5, alpha=0.7)
ax.set_ylim(-0.1, 1.1)
ax.legend(loc='lower right')
ax.set_title("Drift Adaptation Performance")
ax.grid(True, alpha=0.3)

# Kafka
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

# --- RICH UI HELPERS ---
def generate_layout(k_adapt, k_static, status, last_packet_type):
    layout = Layout()
    
    # 1. Header
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body", ratio=1)
    )
    layout["header"].update(Panel("🛡️  DRIFT-AWARE INTRUSION DETECTION SYSTEM", style="bold white on blue"))
    
    # 2. Body (Metrics vs Status)
    layout["body"].split_row(
        Layout(name="metrics"),
        Layout(name="status")
    )
    
    # Metrics Table
    table = Table(title="Live Model Performance", box=box.SIMPLE)
    table.add_column("Model", style="cyan")
    table.add_column("Kappa Score", justify="right")
    table.add_column("Status", justify="center")
    
    # Color logic
    color_a = "green" if k_adapt > 0.6 else "yellow"
    color_b = "green" if k_static > 0.6 else "red"
    
    table.add_row("Adaptive ARF", f"[{color_a}]{k_adapt:.4f}[/]", "✅ ONLINE")
    table.add_row("Static Baseline", f"[{color_b}]{k_static:.4f}[/]", "❄️ FROZEN")
    
    layout["metrics"].update(Panel(table, title="Analytics"))
    
    # Status Panel
    if status == "ATTACK":
        status_panel = Panel(f"\n⚠️  DDoS ATTACK DETECTED\n\nPacket Type: {last_packet_type}", style="bold white on red", title="SYSTEM ALERT")
    else:
        status_panel = Panel(f"\n✅  SYSTEM NORMAL\n\nPacket Type: {last_packet_type}", style="bold green", title="SYSTEM STATUS")
        
    layout["status"].update(status_panel)
    
    return layout

# --- MAIN LOOP ---
count = 0
static_frozen = False
current_status = "NORMAL"

print("Waiting for stream...")

# Start the Live Dashboard
with Live(generate_layout(0, 0, "NORMAL", "WAITING"), refresh_per_second=4) as live:
    
    for msg in consumer:
        record = msg.value
        X = record['features']
        y_true = record['ground_truth']
        pkt_type = record.get('type', 'UNKNOWN')
        
        # Clean Data
        for bad in ['Label', 'label', 'class', 'id', 'Timestamp']:
            X.pop(bad, None)
        clean_X = {k: float(v) for k, v in X.items() if isinstance(v, (int, float, str)) and str(v).replace('.','',1).isdigit()}

        # Model Updates
        # Adaptive
        y_p_a = adaptive_model.predict_one(clean_X)
        adaptive_model.learn_one(clean_X, y_true)
        metric_adapt.update(y_true, y_p_a)
        
        # Static
        y_p_b = static_model.predict_one(clean_X)
        metric_static.update(y_true, y_p_b)
        if count < 2000:
            static_model.learn_one(clean_X, y_true)
        else:
            static_frozen = True

        count += 1
        
        # Update UI every 50 frames
        if count % WINDOW_SIZE == 0:
            ka = metric_adapt.get()
            kb = metric_static.get()
            
            # Determine Status for UI (Simple logic: if Kappa drops fast, it's an attack)
            if pkt_type == 'ATTACK':
                current_status = "ATTACK"
            else:
                current_status = "NORMAL"

            # Update Terminal UI
            live.update(generate_layout(ka, kb, current_status, pkt_type))
            
            # Update Graph
            x_data.append(count)
            y_adapt.append(ka)
            y_static.append(kb)
            if len(x_data) > 200: # Keep graph moving
                x_data.pop(0); y_adapt.pop(0); y_static.pop(0)
            
            line_adapt.set_data(x_data, y_adapt)
            line_static.set_data(x_data, y_static)
            ax.relim(); ax.autoscale_view()
            plt.draw(); plt.pause(0.001)