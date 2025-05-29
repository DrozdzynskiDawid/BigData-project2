from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'netflix-anomalies'

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='netflix-consumer-group',
        value_deserializer=lambda v: v.decode('utf-8')
    )

    print(f"Nasłuchiwanie wiadomości z tematu: {KAFKA_TOPIC}")

    try:
        for message in consumer:
            print(f"Odebrano: {message.value}")
    except KeyboardInterrupt:
        print("Zatrzymano konsumenta.")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()