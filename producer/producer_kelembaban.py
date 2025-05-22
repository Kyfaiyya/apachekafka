from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudang_ids = ["G1", "G2", "G3", "G4"]

def send_temperature_data():
    while True:
        for gudang_id in gudang_ids:
            kelembaban = round(random.uniform(60, 80), 1)
            data = {
                "gudang_id": gudang_id,
                "kelembaban": kelembaban,
            }
            producer.send('sensor-suhu-gudang', value=data)  # Pakai keyword argument value=
            print(f"Mengirim data suhu: {data}")
            producer.flush()  # Pastikan pesan terkirim sebelum lanjut
            time.sleep(1)     # Delay 1 detik setiap pengiriman per gudang

if __name__ == "__main__":
    print("Producer Sensor Suhu dimulai...")
    try:
        send_temperature_data()
    except KeyboardInterrupt:
        print("Producer Sensor Suhu dihentikan.")