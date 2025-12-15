import time
import statistics
import random
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel

CLUSTER_IP = ['127.0.0.1']
KEYSPACE = 'wind_energy'
ITERATIONS = 50         
WRITE_BATCH_SIZE = 50    
WRITE_TOTAL_BATCHES = 100

cluster = Cluster(CLUSTER_IP)
session = cluster.connect(KEYSPACE)

results = {
    'read_latency': {},
    'write_throughput': {},
    'mv_comparison': {}
}

def measure_query(name, query_str, params, schema_key=None):
    """Вимірює час виконання запиту (Latency)."""
    latencies = []
    try:
        prepared = session.prepare(query_str)
        session.execute(prepared, params)

        for _ in range(ITERATIONS):
            start = time.time()
            session.execute(prepared, params)
            latencies.append((time.time() - start) * 1000)

        avg = statistics.mean(latencies)
        # Розрахунок перцентилів (P95, P99) для оцінки стабільності
        sorted_lat = sorted(latencies)
        p95 = sorted_lat[int(len(sorted_lat) * 0.95)]
        p99 = sorted_lat[int(len(sorted_lat) * 0.99)]

        print(f"| {name:<35} | Avg: {avg:6.2f}ms | P95: {p95:6.2f}ms | P99: {p99:6.2f}ms |")

        if schema_key:
            results['read_latency'][schema_key] = avg
        return avg

    except Exception as e:
        print(f"| {name:<35} | ERROR: {e}")
        return 0

def measure_write_throughput(schema_name, insert_query):
    """Вимірює пропускну здатність запису (Throughput)."""
    print(f"--- Тестування запису для: {schema_name} ---")
    prepared = session.prepare(insert_query)
    total_records = WRITE_BATCH_SIZE * WRITE_TOTAL_BATCHES

    start_time = time.time()

    for i in range(WRITE_TOTAL_BATCHES):
        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        for _ in range(WRITE_BATCH_SIZE):
            # Генерація тестових даних "на льоту"
            t_id = f"TEST_TURBINE_{random.randint(1, 100)}"
            ts = datetime.now()
            wind = random.uniform(0, 25)
            
            # Додавання в batch залежно від схеми
            if schema_name == 'Simple':
                batch.add(prepared, (t_id, ts, wind, wind * 1.5, 90 - wind * 3))
            elif schema_name == 'Hourly':
                batch.add(prepared, (t_id, ts.replace(minute=0, second=0, microsecond=0), ts, wind, wind * 1.5, 90 - wind * 3))
            elif schema_name == 'Daily':
                batch.add(prepared, (t_id, ts.date(), ts, wind, wind * 1.5, 90 - wind * 3))

        session.execute(batch)

    duration = time.time() - start_time
    ops_sec = total_records / duration
    print(f"   Throughput: {ops_sec:.0f} ops/sec")
    results['write_throughput'][schema_name] = ops_sec

def generate_charts():
    """Створює та зберігає графіки результатів."""
    print("\n--- Генерація графіків... ---")

    # 1. Read Latency
    if results['read_latency']:
        plt.figure(figsize=(10, 6))
        plt.bar(list(results['read_latency'].keys()), list(results['read_latency'].values()), color=['#e74c3c', '#2ecc71', '#3498db'])
        plt.title('Середня затримка читання (менше - краще)')
        plt.ylabel('ms')
        plt.savefig('benchmark_latency.png')

    # 2. Write Throughput
    if results['write_throughput']:
        plt.figure(figsize=(10, 6))
        plt.bar(list(results['write_throughput'].keys()), list(results['write_throughput'].values()), color=['#e74c3c', '#2ecc71', '#3498db'])
        plt.title('Пропускна здатність запису (більше - краще)')
        plt.ylabel('ops/sec')
        plt.savefig('benchmark_throughput.png')

    # 3. MV Comparison
    if results['mv_comparison']:
        plt.figure(figsize=(8, 6))
        plt.bar(list(results['mv_comparison'].keys()), list(results['mv_comparison'].values()), color=['#95a5a6', '#9b59b6'])
        plt.title('Materialized View vs Allow Filtering')
        plt.ylabel('ms (Log Scale)')
        plt.yscale('log') # Логарифмічна шкала, бо різниця може бути в порядках
        plt.savefig('benchmark_mv_impact.png')


#Tests

turbine_id = "WIND_ZAP_001"
test_time = datetime.now() - timedelta(days=1)
hour_key = test_time.replace(minute=0, second=0, microsecond=0)

print(f"\n=== ЕТАП 1: ТЕСТУВАННЯ READ LATENCY ===")

# Simple: Партиція велика, пошук може бути повільнішим
measure_query("Simple: Range 6h", 
              "SELECT * FROM wind_telemetry_simple WHERE turbine_id=? AND timestamp > ? AND timestamp < ?",
              (turbine_id, hour_key, hour_key + timedelta(hours=6)), schema_key='Simple')

# Hourly: Оптимальний розмір партиції для погодинних запитів
measure_query("Hourly: Range 1h", 
              "SELECT * FROM wind_telemetry_hourly WHERE turbine_id=? AND hour=? AND timestamp > ? AND timestamp < ?",
              (turbine_id, hour_key, hour_key, hour_key + timedelta(hours=1)), schema_key='Hourly')

# Daily: Партиція більша за погодинну
measure_query("Daily: Range 1h", 
              "SELECT * FROM wind_telemetry_daily WHERE turbine_id=? AND day=? AND timestamp > ? AND timestamp < ?",
              (turbine_id, hour_key.date(), hour_key, hour_key + timedelta(hours=1)), schema_key='Daily')

print("\n--- MV Comparison ---")
# Порівняння неефективного сканування (ALLOW FILTERING) з Materialized View
results['mv_comparison']['No MV'] = measure_query("Filter: ALLOW FILTERING", 
    "SELECT * FROM wind_telemetry_hourly WHERE turbine_id=? AND hour=? AND wind_speed > 15 ALLOW FILTERING", 
    (turbine_id, hour_key))

results['mv_comparison']['With MV'] = measure_query("Filter: Materialized View", 
    "SELECT * FROM wind_by_speed WHERE turbine_id=? AND hour=? AND wind_speed > 15", 
    (turbine_id, hour_key))

print(f"\n=== ЕТАП 2: ТЕСТУВАННЯ WRITE THROUGHPUT ===")
measure_write_throughput('Simple', "INSERT INTO wind_telemetry_simple (turbine_id, timestamp, wind_speed, rotor_speed, blade_angle) VALUES (?, ?, ?, ?, ?)")
measure_write_throughput('Hourly', "INSERT INTO wind_telemetry_hourly (turbine_id, hour, timestamp, wind_speed, rotor_speed, blade_angle) VALUES (?, ?, ?, ?, ?, ?)")
measure_write_throughput('Daily', "INSERT INTO wind_telemetry_daily (turbine_id, day, timestamp, wind_speed, rotor_speed, blade_angle) VALUES (?, ?, ?, ?, ?, ?)")

generate_charts()
print("\n=== ВСІ ТЕСТИ ЗАВЕРШЕНО ===")
