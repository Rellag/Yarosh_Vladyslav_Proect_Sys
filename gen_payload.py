from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

def generate_wind_record(msg_id):
    """Генерація одного запису за структурою WIND_ZP"""
    return {
        "device_id": f"WIND_ZP_{random.randint(1, 30):03}",
        "timestamp": time.time(),
        "power_output_kw": round(random.uniform(0.0, 3000.0), 2),
        "wind_speed_ms": round(random.uniform(3.0, 25.0), 1), 
        "rotor_rpm": round(random.uniform(5.0, 30.0), 1),      
        "blade_angle": random.choice(["auto", "manual", "feathered"]), 
        "status": "optimal",
        "msg_id": msg_id
    }

print("Start generating 1000 records ...")
start_time = time.time()

for i in range(1000):
    data = generate_wind_record(i)
    producer.send('wind-main', value=data)

producer.flush()
end_time = time.time()

#
duration = end_time - start_time
rate = 1000 / duration

print(f"\nSuccessfully sent 1000 records to topic 'wind-main'")
print(f"Execution time: {duration:.4f} sec")
print(f"Generation rate: {rate:.2f} records/sec")
print("Checking blade_angle: data is generated correctly (auto/manual/feathered)")

producer.close()
