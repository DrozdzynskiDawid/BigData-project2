pip install kafka-python


``` bash
/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 \
 --replication-factor 1 --partitions 3 --topic netflix
```

uruchomić skrypt kafka_producer.py
