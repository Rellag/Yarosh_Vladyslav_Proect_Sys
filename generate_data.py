import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('wind_energy')

print("Generating data for 30 turbines...")


NUM_TURBINES = 30
PARK_ID = 1
BASE_TIME = datetime.now().replace(minute=0, second=0, microsecond=0)

insert_reading = session.prepare("""
    INSERT INTO turbine_readings (turbine_id, timestamp, wind_speed, angle, power_output, vibration)
    VALUES (?, ?, ?, ?, ?, ?)
""")

for hour in range(24):
    current_time = BASE_TIME - timedelta(hours=hour)

    batch = BatchStatement()

    for t_id in range(1, NUM_TURBINES + 1):
        wind = random.uniform(2.0, 15.0)
        angle = random.uniform(0, 360)
        power = 0 if wind < 3 else (wind ** 3) * 0.5
        vibration = random.uniform(0.0, 5.0) + (wind * 0.1)

        batch.add(insert_reading, (t_id, current_time, wind, angle, power, vibration))

    session.execute(batch)
    print(f"Inserted data for hour: {current_time}")

print("Data generation complete.")
cluster.shutdown()