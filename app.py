import faust
import uuid
import numpy as np
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

# --- 1. Налаштування підключення до Cassandra ---
print("Connecting to Cassandra...")
cluster = Cluster(['localhost'])
session = cluster.connect('wind_energy')

# Підготовка SQL-запитів (Prepared Statements) для швидкодії
# Таблиця сесій (Учасник 1)
insert_session = session.prepare("""
    INSERT INTO operation_sessions (session_id, turbine_id, start_time, end_time, stability_score, status)
    VALUES (?, ?, ?, ?, ?, ?)
""")
# Таблиця статистики (Учасник 2)
insert_stats = session.prepare("""
    INSERT INTO performance_stats (turbine_id, session_id, avg_power, total_energy, avg_wind_speed)
    VALUES (?, ?, ?, ?, ?)
""")
# Журнал транзакцій
insert_log = session.prepare("""
    INSERT INTO transaction_log (tx_id, timestamp, status, details)
    VALUES (?, ?, ?, ?)
""")
print("Cassandra connected.")

# --- 2. Налаштування Faust App ---
app = faust.App(
    'wind-farm-processor-v1',
    broker='kafka://localhost:9092',
    store='memory://', # Для лаби використовуємо пам'ять (у продакшені було б rocksdb://)
    topic_partitions=1,
)

# Опис вхідних даних (схема)
class TurbineReading(faust.Record):
    turbine_id: str
    timestamp: float
    power_output: float
    wind_speed: float
    vibration: float

# Створення топіку та таблиці для зберігання стану вікна
topic = app.topic('wind_sensors', value_type=TurbineReading)
session_store = app.Table('session_store', default=dict)

# Константа розриву сесії (Gap). 
# У завданні 2 хв, але ставимо 10 сек, щоб ви швидко побачили результат у звіті.
SESSION_GAP = 10.0 

# --- 3. Логіка Two-Phase Commit (2PC) ---
def execute_2pc_transaction(turbine_id, session_data):
    """
    Реалізація протоколу 2PC.
    Фаза 1: Перевірка даних (Prepare).
    Фаза 2: Атомарний запис (Commit) або відкат (Abort).
    """
    tx_id = uuid.uuid4()
    session_uuid = uuid.uuid4()
    
    # Витягуємо накопичені списки даних
    power_vals = session_data['power_values']
    wind_vals = session_data['wind_values']
    vibrations = session_data['vibrations']
    
    # Розрахунки агрегацій
    avg_power = np.mean(power_vals) if power_vals else 0.0
    avg_wind = np.mean(wind_vals) if wind_vals else 0.0
    total_energy = sum(power_vals)
    
    # Розрахунок Stability Score (чим менше "стрибає" вібрація, тим стабільніше)
    variance = np.var(vibrations) if vibrations else 0
    stability_score = max(0, 100 - variance * 10) # 100 - ідеал
    
    print(f"\n[2PC START] TxID: {tx_id} | Turbine: {turbine_id}")
    
    # --- ФАЗА 1: PREPARE (Валідація) ---
    vote = "COMMIT"
    fail_reason = ""
    
    # Правило валідації: не може бути енергії без вітру (аномалія датчика)
    if avg_wind < 1.0 and avg_power > 0.1:
        vote = "ABORT"
        fail_reason = "Anomaly: Power generation without wind"
    
    # Правило валідації: надто низька стабільність
    if stability_score < 10:
        vote = "ABORT"
        fail_reason = "Critically unstable vibration"

    # Логуємо рішення фази Prepare
    session.execute(insert_log, (tx_id, datetime.now(), 'PREPARE', f'{turbine_id}: {vote} {fail_reason}'))
    print(f"   [2PC PREPARE] Vote: {vote}")

    # --- ФАЗА 2: COMMIT або ABORT ---
    if vote == "COMMIT":
        try:
            # Використовуємо BATCH для атомарності (все або нічого)
            batch = BatchStatement()
            
            # Запис 1: Закриваємо сесію
            batch.add(insert_session, (
                session_uuid, turbine_id, 
                datetime.fromtimestamp(session_data['start']), 
                datetime.fromtimestamp(session_data['end']), 
                stability_score, 'CLOSED'
            ))
            
            # Запис 2: Зберігаємо статистику
            batch.add(insert_stats, (
                turbine_id, session_uuid, avg_power, 
                total_energy, avg_wind
            ))
            
            # Запис 3: Логуємо успіх транзакції
            batch.add(insert_log, (tx_id, datetime.now(), 'COMMIT', 'Data persisted successfully'))
            
            # Виконання батчу
            session.execute(batch)
            print(f"[2PC COMMIT] Successfully saved session for {turbine_id}.")
            
        except Exception as e:
            print(f"[2PC ERROR] DB Write Failed: {e}")
            session.execute(insert_log, (tx_id, datetime.now(), 'ABORT', str(e)))
    else:
        # Якщо ABORT - просто логуємо причину, дані в таблиці статистики не пишуться
        session.execute(insert_log, (tx_id, datetime.now(), 'ABORT', fail_reason))
        print(f"[2PC ABORT] Transaction cancelled. Reason: {fail_reason}")


# --- 4. Обробка потоку даних (Main Loop) ---
@app.agent(topic)
async def process_readings(readings):
    async for reading in readings:
        t_id = reading.turbine_id
        now = reading.timestamp
        
        # Отримуємо поточний стан сесії з таблиці Faust
        current = session_store[t_id]
        
        # 1. Перевірка Session Window (Gap Detection)
        # Якщо остання подія була давно (> SESSION_GAP), закриваємо попередню сесію
        if current and (now - current['last_seen'] > SESSION_GAP):
            print(f"--- Session Gap detected for {t_id} (> {SESSION_GAP}s) ---")
            execute_2pc_transaction(t_id, current)
            current = {} # Очищаємо стан
        
        # 2. Ініціалізація нової сесії, якщо порожня
        if not current:
            current = {
                'start': now,
                'power_values': [],
                'vibrations': [],
                'wind_values': [],
                'last_seen': now
            }
        
        # 3. Накопичення даних (Stateful processing)
        current['last_seen'] = now
        current['end'] = now
        current['power_values'].append(reading.power_output)
        current['vibrations'].append(reading.vibration)
        current['wind_values'].append(reading.wind_speed)
        
        # Зберігаємо оновлений стан назад у таблицю
        session_store[t_id] = current

if __name__ == '__main__':
    app.main()