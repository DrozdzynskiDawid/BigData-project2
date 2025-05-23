import os
import csv
import time
from kafka import KafkaProducer

# === KONFIGURACJA ===
CSV_FOLDER = 'data\\netflix-prize-data'  # Folder z plikami CSV
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'netflix'
DELAY_SECONDS = 1  # opóźnienie między rekordami (symulacja strumienia)

# === PRODUCENT KAFKA ===
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: v.encode('utf-8')  # serializacja tekstowa
)

def send_csv_file(file_path):
    print(f"➡️ Wysyłanie z pliku: {file_path}")
    with open(file_path, mode='r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader, None)  # pomiń nagłówek, jeśli jest

        for row in reader:
            date, film_id, user_id, rate = row
            message = f"{date},{film_id},{user_id},{rate}"
            print(message)
            producer.send(KAFKA_TOPIC, message)
            print(f"   📨 Wysłano: {message}")
            time.sleep(DELAY_SECONDS)

def produce_from_folder(folder_path):
    for filename in sorted(os.listdir(folder_path)):
        if filename.endswith(".csv"):
            full_path = os.path.join(folder_path, filename)
            send_csv_file(full_path)

    producer.flush()
    print("✅ Zakończono wysyłanie danych z folderu.")

if __name__ == '__main__':
    produce_from_folder(CSV_FOLDER)
