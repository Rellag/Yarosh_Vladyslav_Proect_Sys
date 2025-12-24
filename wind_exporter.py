from prometheus_client import start_http_server, Gauge, Counter
import time
import random
import math

# === МЕТРИКИ PROMETHEUS ===
# 1. Поточна потужність (МВт)
POWER_OUTPUT = Gauge('wind_turbine_power_mw', 'Active Power Output', ['turbine_id', 'row'])
# 2. Швидкість вітру на турбіні (м/с)
WIND_SPEED = Gauge('wind_turbine_speed_mps', 'Wind Speed at Turbine', ['turbine_id'])
# 3. Теоретична потужність (для порівняння Power Curve)
THEORETICAL_POWER = Gauge('wind_theoretical_power_mw', 'Theoretical Power by Curve', ['turbine_id'])
# 4. Втрати через Wake Effect (МВт)
WAKE_LOSS = Gauge('wind_wake_loss_mw', 'Power lost due to wake effect', ['turbine_id'])
# 5. Напрямок вітру (градуси)
WIND_DIR = Gauge('wind_direction_deg', 'Wind Direction', ['site'])

# === КОНФІГУРАЦІЯ ВІТРОПАРКУ ===
# Емулюємо 3 ряди турбін. Ряд 0 - перший до вітру, Ряд 2 - останній (найбільший wake effect)
TURBINES = []
for i in range(1, 31):
    row_id = (i - 1) // 10  # 0, 1 або 2
    TURBINES.append({'id': f"WT-{i:02d}", 'row': row_id})

def calculate_theoretical_power(wind_mps):
    """
    Класична Power Curve:
    - Cut-in: 3 м/с
    - Rated: 12 м/с (2.5 МВт)
    - Cut-out: 25 м/с
    """
    if wind_mps < 3 or wind_mps > 25:
        return 0.0
    if wind_mps >= 12:
        return 2.5
    # Кубічна залежність на ділянці розгону
    return 2.5 * ((wind_mps - 3) / (12 - 3)) ** 3

def simulate_metrics():
    # Базовий вітер для всього парку (змінюється плавно)
    base_wind = random.normalvariate(10, 3) 
    base_wind = max(0, min(30, base_wind))
    
    # Напрямок вітру (0-360)
    direction = random.uniform(0, 360)
    WIND_DIR.labels(site='Zaporizhzhia').set(direction)

    for t in TURBINES:
        t_id = t['id']
        row = t['row']

        # 1. Розрахунок Wake Effect (Турбіни позаду отримують менше вітру)
        # Спрощена модель: кожен ряд втрачає 10% швидкості вітру
        local_wind = base_wind * (0.9 ** row)
        # Додамо трохи шуму
        local_wind += random.uniform(-0.5, 0.5)
        local_wind = max(0, local_wind)

        # 2. Розрахунок теоретичної потужності
        theo_p = calculate_theoretical_power(local_wind)
        
        # 3. Розрахунок реальної потужності
        # У реальності турбіна завжди має втрати (ККД генератора ~95%)
        base_efficiency = random.uniform(0.92, 0.96) 
        
        # Додаткові втрати або збої (іноді)
        if random.random() < 0.1: # Підвищимо шанс збою до 10%
            base_efficiency *= 0.7 
        
        real_p = theo_p * base_efficiency

        # 4. Запис метрик
        POWER_OUTPUT.labels(turbine_id=t_id, row=str(row)).set(real_p)
        WIND_SPEED.labels(turbine_id=t_id).set(local_wind)
        THEORETICAL_POWER.labels(turbine_id=t_id).set(theo_p)
        
        # Втрати = Теоретична (при ідеальному вітрі) - Реальна (через wake)
        # Але Wake Effect рахується відносно базового вітру
        ideal_power_no_wake = calculate_theoretical_power(base_wind)
        wake_loss = max(0, ideal_power_no_wake - real_p)
        WAKE_LOSS.labels(turbine_id=t_id).set(wake_loss)

        print(f"[{t_id}] Wind: {local_wind:.1f} m/s | Power: {real_p:.2f} MW | Wake Loss: {wake_loss:.2f} MW")

if __name__ == '__main__':
    # Запуск HTTP сервера на порту 8000 для Prometheus
    start_http_server(8000)
    print("🚀 Wind Farm Exporter running on port 8000...")
    
    while True:
        simulate_metrics()
        time.sleep(5)