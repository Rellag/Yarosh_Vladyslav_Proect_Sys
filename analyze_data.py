import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('wind_energy')

TARGET_TURBINE = 1

print(f"Fetching data for Turbine #{TARGET_TURBINE}...")


rows = session.execute(f"""
    SELECT timestamp, power_output 
    FROM turbine_readings 
    WHERE turbine_id = {TARGET_TURBINE} 
    LIMIT 24
""")

data = list(reversed(list(rows)))

if not data:
    print("No data found! Run generate_data.py first.")
    exit()

timestamps = [row.timestamp for row in data]
power_values = [row.power_output for row in data]

max_power = max(power_values)
min_power = min(power_values)
max_idx = power_values.index(max_power)
min_idx = power_values.index(min_power)

plt.figure(figsize=(10, 6)) # Розмір вікна

plt.plot(timestamps, power_values, marker='o', linestyle='-', color='b', label='Power Output')

plt.title(f'Doily Generation Profile: Turbine #{TARGET_TURBINE}', fontsize=14)
plt.xlabel('Time', fontsize=12)
plt.ylabel('Power (kW)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)

plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
plt.gcf().autofmt_xdate() # Повертає підписи під кутом, щоб не налазили один на одного

plt.annotate(f'MAX: {max_power:.1f} kW',
             xy=(timestamps[max_idx], max_power),
             xytext=(timestamps[max_idx], max_power + 100),
             arrowprops=dict(facecolor='green', shrink=0.05))

plt.annotate(f'MIN: {min_power:.1f} kW',
             xy=(timestamps[min_idx], min_power),
             xytext=(timestamps[min_idx], min_power + 100),
             arrowprops=dict(facecolor='red', shrink=0.05))

print("Displaying graph...")
plt.legend()
plt.tight_layout()
plt.show()

cluster.shutdown()