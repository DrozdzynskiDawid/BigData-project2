# BigData - project2
**Platforma: Flink (DataStream API)**

**Zestaw 1 – Netflix-Prize-Data**

### Instrukcja uruchamiania projektu:
- pobierz dane dla zestawu 1 [movie_titles.csv](https://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/movie_titles.csv) oraz [netflix-prize-data](https://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/netflix-prize-data.zip) i umieść je w folderze `data`
- zainstaluj potrzebne biblioteki, w szczególności: `pip install kafka-python`

- ustaw wartości poszczególnych parametrów skryptu zgodnie ze swoją konfiguracją:
```
CSV_FOLDER = 'data\\netflix-prize-data'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'netflix'
DELAY_SECONDS = 1 
```

- skorzystaj z kontenerów FlinkAndFriends2025, upewnij się, że wskazany parametr ma poprawną wartość w pliku `docker-compose.yml`:
``` bash
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
```

- utwórz temat producenta w kontenerze Kafki:
 ``` bash
/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 \
 --replication-factor 1 --partitions 3 --topic netflix
```
- utwórz temat, do którego trafią wykryte anomalie:
 ``` bash
/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 \
 --replication-factor 1 --partitions 3 --topic netflix-anomalies
```
- uruchom skrypt `kafka_producer.py` i obserwuj temat producenta, czy został zasilony
- uruchom skrypt `netflix_data_analysis.py` wraz z wybranymi parametrami D, L oraz O
- mointoruj temat `netflix-anomalies`
```bash
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic netflix-anomalies --
from-beginning 
```
Przykładowy wynik dla parametrów `D=30 L=2 O=2.5`:
```json
{"window_start": "1999-11-08T01:00:00", "window_end": "1999-12-08T01:00:00", "title": "Witness", "count": 2, "avg_rate": 5.0}
{"window_start": "1999-11-08T01:00:00", "window_end": "1999-12-08T01:00:00", "title": "The Piano", "count": 2, "avg_rate": 4.0}
{"window_start": "1999-11-09T01:00:00", "window_end": "1999-12-09T01:00:00", "title": "Witness", "count": 2, "avg_rate": 5.0}
{"window_start": "1999-11-09T01:00:00", "window_end": "1999-12-09T01:00:00", "title": "Legends of the Fall", "count": 2, "avg_rate": 4.5}
```
