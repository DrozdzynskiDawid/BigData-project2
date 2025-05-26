# BigData - project2
**Platforma: Flink (DataStream API)**

**Zestaw 1 – Netflix-Prize-Data**

## Instrukcja uruchamiania projektu:
- pobierz dane dla zestawu 1 [movie_titles.csv](https://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/movie_titles.csv) oraz [netflix-prize-data](https://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/netflix-prize-data.zip) i umieść je w folderze `data`
- zainstaluj potrzebne biblioteki, w szczególności: `pip install kafka-python`

- ustaw wartości poszczególnych parametrów skryptu zgodnie ze swoją konfiguracją:
```
CSV_FOLDER = 'data\\netflix-prize-data'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'netflix'
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
- uruchom skrypt `kafka_producer.py` i obserwuj temat producenta, czy został zasilony

### Wykrywanie anomalii:
- utwórz temat, do którego trafią wykryte anomalie:
 ``` bash
/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 \
 --replication-factor 1 --partitions 3 --topic netflix-anomalies
```
- zwróć uwagę na plik `flink.properties` i dostosuj poszczególne propsy, w szczególności zwróć uwagę na: `static.file.path` oraz `pipeline.jars` 
- uruchom skrypt `netflix_data_anomalies_detection.py` wraz z wybranymi parametrami D, L oraz O podanymi w run configuration IDE
- monitoruj temat `netflix-anomalies`:
```bash
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic netflix-anomalies --from-beginning 
```
Przykładowy wynik dla parametrów `D=30 L=2 O=2.5`:
```json
{"window_start": "1999-11-08T01:00:00", "window_end": "1999-12-08T01:00:00", "title": "Witness", "count": 2, "avg_rate": 5.0}
{"window_start": "1999-11-08T01:00:00", "window_end": "1999-12-08T01:00:00", "title": "The Piano", "count": 2, "avg_rate": 4.0}
{"window_start": "1999-11-09T01:00:00", "window_end": "1999-12-09T01:00:00", "title": "Witness", "count": 2, "avg_rate": 5.0}
{"window_start": "1999-11-09T01:00:00", "window_end": "1999-12-09T01:00:00", "title": "Legends of the Fall", "count": 2, "avg_rate": 4.5}
```

### ETL – obraz czasu rzeczywistego:
- utwórz kontener MongoDB:
```dockerfile
docker run -d -p 27017:27017 --name mongodb mongo
```
- sprawdź parametry dotyczące MongoDB w pliku `flink.properties`
- uruchom skrypt `netflix_data_ETL_analysis.py` z wybranym parametrem delay (wartość A lub C) podanym w run configuration IDE
- po chwili uruchom skrypt `mongodb_reader.py` i sprawdź wyniki w kolekcji MongoDB

Przykładowy wynik:
```json
{"_id": "683306f7474dca658742384a", "id": 4271, "title": "Bound", "month": "1999-11", "count_rate": 1, "sum_rate": 3.0, "unique_users": 1}
{"_id": "683306f7474dca658742384c", "id": 16668, "title": "A Few Good Men", "month": "1999-11", "count_rate": 2, "sum_rate": 9.0, "unique_users": 2}
{"_id": "683306f7474dca658742384e", "id": 6971, "title": "Ferris Bueller's Day Off", "month": "1999-11", "count_rate": 3, "sum_rate": 11.0, "unique_users": 3}
{"_id": "683306f7474dca6587423850", "id": 7155, "title": "The Pelican Brief", "month": "1999-11", "count_rate": 2, "sum_rate": 8.0, "unique_users": 2}
```
