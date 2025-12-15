import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel

# Основні налаштування підключення та обсягу даних
CLUSTER_IP = ['127.0.0.1']
KEYSPACE = 'wind_energy'
NUM_TURBINES = 50
DAYS = 30
RECORDS_PER_HOUR = 30  # 1 млн записів сумарно

cluster = Cluster(CLUSTER_IP)
session = cluster.connect(KEYSPACE)

# Підготовка запитів (PreparedStatement) для трьох різних схем моделювання даних
insert_simple = session.prepare(
    "INSERT INTO wind_telemetry_simple (turbine_id, timestamp, wind_speed, rotor_speed, blade_angle) VALUES (?, ?, ?, ?, ?)")
insert_hourly = session.prepare(
    "INSERT INTO wind_telemetry_hourly (turbine_id, hour, timestamp, wind_speed, rotor_speed, blade_angle) VALUES (?, ?, ?, ?, ?, ?)")
insert_daily = session.prepare(
    "INSERT INTO wind_telemetry_daily (turbine_id, day, timestamp, wind_speed, rotor_speed, blade_angle) VALUES (?, ?, ?, ?, ?, ?)")

def generate_data():
    start_time = datetime.now() - timedelta(days=DAYS)
    total_records = 0

    print("Початок генерації даних...")

    for day_offset in range(DAYS):
        current_day = start_time + timedelta(days=day_offset)
        day_date = current_day.date()

        for hour in range(24):
            hour_start = current_day.replace(hour=hour, minute=0, second=0, microsecond=0)

            for t_idx in range(NUM_TURBINES):
                turbine_id = f"WIND_ZAP_{t_idx:03d}"

                # Створюємо окремі батчі для кожної таблиці
                batch_simple = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                batch_hourly = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                batch_daily = BatchStatement(consistency_level=ConsistencyLevel.ONE)

                for _ in range(RECORDS_PER_HOUR):
                    ts = hour_start + timedelta(minutes=random.randint(0, 59), seconds=random.randint(0, 59))

                    # Генерація випадкових показників (вітер, ротор, кут)
                    wind = max(0, random.normalvariate(10, 5))
                    rotor = wind * 1.5 + random.uniform(-1, 1)
                    angle = max(0, 90 - wind * 3)

                    # Додавання даних у чергу (батч)
                    batch_simple.add(insert_simple, (turbine_id, ts, wind, rotor, angle))
                    batch_hourly.add(insert_hourly, (turbine_id, hour_start, ts, wind, rotor, angle))
                    batch_daily.add(insert_daily, (turbine_id, day_date, ts, wind, rotor, angle))

                    total_records += 1

                # Фізичний запис пакету даних у базу
                try:
                    session.execute(batch_simple)
                    session.execute(batch_hourly)
                    session.execute(batch_daily)
                except Exception as e:
                    print(f"Error writing batch: {e}")

            print(f"День {day_offset + 1}/{DAYS}, Година {hour}: оброблено {total_records} записів")

    print(f"Генерація завершена. Всього записів: {total_records}")

if __name__ == "__main__":
    generate_data()
