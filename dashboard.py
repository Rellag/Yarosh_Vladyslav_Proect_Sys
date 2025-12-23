import time
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.console import Console
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import os

# Налаштування підключення
def get_data():
    try:
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect('wind_energy')
        session.row_factory = dict_factory
        
        # Отримуємо останні 15 записів
        logs = list(session.execute("SELECT * FROM transaction_log LIMIT 15"))
        stats = list(session.execute("SELECT * FROM performance_stats"))
        
        cluster.shutdown()
        return logs, stats
    except Exception:
        return [], []

def generate_layout():
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body", ratio=1)
    )
    layout["body"].split_row(
        Layout(name="left", ratio=1),
        Layout(name="right", ratio=1)
    )
    return layout

def make_header():
    return Panel("🌪️  Wind Farm Monitoring System (Variant 2.B) | 🟢 Live Connected", style="bold white on blue")

def make_stats_table(stats):
    table = Table(title="📊 Turbine Performance", expand=True, border_style="cyan")
    table.add_column("Turbine ID", style="cyan", no_wrap=True)
    table.add_column("Avg Power (MW)", justify="right", style="green")
    table.add_column("Avg Wind (m/s)", justify="right", style="magenta")

    for row in stats:
        table.add_row(
            row['turbine_id'], 
            f"{row['avg_power']:.2f}", 
            f"{row['avg_wind_speed']:.2f}"
        )
    return Panel(table, title="Aggregation Results", border_style="cyan")

def make_log_table(logs):
    table = Table(title="📝 2PC Transaction Log", expand=True, border_style="yellow")
    table.add_column("Time", style="dim", width=12)
    table.add_column("Status", justify="center")
    table.add_column("Details")

    # Сортуємо (нові зверху)
    sorted_logs = sorted(logs, key=lambda x: x['timestamp'], reverse=True)

    for row in sorted_logs:
        # Колір статусу
        status_style = "bold green" if row['status'] == 'COMMIT' else "bold red"
        
        # Форматуємо час (тільки години:хвилини:секунди)
        time_str = row['timestamp'].strftime("%H:%M:%S")
        
        table.add_row(
            time_str,
            f"[{status_style}]{row['status']}[/{status_style}]",
            row['details']
        )
    return Panel(table, title="Audit Trail", border_style="yellow")

# --- Головний цикл ---
console = Console()
layout = generate_layout()

layout["header"].update(make_header())

print("Запуск термінального дашборду... (Ctrl+C для виходу)")

with Live(layout, refresh_per_second=1, screen=True) as live:
    while True:
        logs, stats = get_data()
        
        layout["left"].update(make_stats_table(stats))
        layout["right"].update(make_log_table(logs))
        
        time.sleep(1)