# BigData - project2
**Platforma: Flink (DataStream API)**

**Zestaw 1 – Netflix-Prize-Data**

### Instrukcja uruchamiania projektu:
- pobierz dane dla zestawu 1 i umieść je w folderze data
- zainstaluj potrzebne biblioteki, w szczególności: `pip install kafka-python`

- ustaw wartości poszczególnych parametrów skryptu zgodnie ze swoją konfiguracją:
```
CSV_FOLDER = 'data\\netflix-prize-data'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'netflix'
DELAY_SECONDS = 1 
```

- utwórz temat producenta w kontenerze Kafki:
 ``` bash
/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 \
 --replication-factor 1 --partitions 3 --topic netflix
```
- uruchom skrypt kafka_producer.py i obserwuj temat producenta
