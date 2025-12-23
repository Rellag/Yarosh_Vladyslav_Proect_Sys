import json
import time
import random
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

TURBINE_IDS = [f"WT-{i:03d}" for i in range(1, 6)] 

def generate_turbine_data():
    """Генерує випадкові показники датчиків для однієї турбіни"""
    turbine_id = random.choice(TURBINE_IDS)
    
 
    if random.random() < 0.05:
        print(f"Simulating connection loss (Gap) for {turbine_id} ---")
        return None

    data = {
        "turbine_id": turbine_id,
        "timestamp": time.time(),
        "power_output": round(random.uniform(0, 2.5), 3), 
        "wind_speed": round(random.uniform(3, 25), 2),
        "wind_direction": random.randint(0, 360),
        "blade_pitch": round(random.uniform(0, 90), 2),
        "vibration": round(random.uniform(0, 10), 2),
        "temperature_generator": round(random.uniform(40, 95), 1),
        "temperature_gearbox": round(random.uniform(45, 100), 1)
    }
    return data

print("Starting Wind Farm Sensors Simulation...")
print("Press Ctrl+C to stop.")

try:
    while True:
        sensor_data = generate_turbine_data()
        
        if sensor_data:
            producer.send('wind_sensors', value=sensor_data)
            
            print(f"Sent [{sensor_data['turbine_id']}]: "
                  f"Power={sensor_data['power_output']}MW, "
                  f"Wind={sensor_data['wind_speed']}m/s")
        
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping producer...")
    producer.close()