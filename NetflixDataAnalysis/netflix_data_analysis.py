import sys
from pyflink.common import Configuration
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types

from NetflixDataAnalysis.connectors.kafka_sink import get_kafka_sink
from NetflixDataAnalysis.models.netflix_data_agg import NetflixDataAgg
from NetflixDataAnalysis.tools.enrich_with_movie_titles import EnrichWithMovieTitles
from connectors.kafka_source import get_kafka_source
from models.netflix_data import NetflixData
from tools.properties import load_properties


def reduce_movie_data(sd1: NetflixDataAgg, sd2: NetflixDataAgg) -> NetflixDataAgg:
    total_count = sd1.count + sd2.count
    total_rate_sum = sd1.avg_rate * sd1.count + sd2.avg_rate * sd2.count
    avg_rate = total_rate_sum / total_count
    return NetflixDataAgg(
        title=sd1.title,
        count=total_count,
        avg_rate=avg_rate
    )


def main():
    D = sys.argv[1]
    L = sys.argv[2]
    O = sys.argv[3]
    print("Parameters - D: " + D + " L: " + L + " O: " + O)
    props = load_properties("flink.properties")
    print("Input Kafka topic:", props.get("kafka.input.topic"))
    print("Static input file:", props.get("static.file.path"))
    config = Configuration()
    config.set_string("pipeline.jars",
                      "file:///C:/Users/dawid/Downloads/flink-connector-kafka-4.0.0-2.0.jar;" +
                      "file:///C:/Users/dawid/Downloads/kafka-clients-4.0.0.jar")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)

    # WCZYTYWANIE DANYCH STRUMIENIOWYCH Z TEMATU KAFKI
    source = get_kafka_source(props)
    data_stream = env.from_source(source, watermark_strategy=WatermarkStrategy.no_watermarks(), source_name="KafkaSource")
    netflix_data_stream = (
        data_stream
        .map(lambda line: line.split(","), output_type=Types.OBJECT_ARRAY(Types.STRING()))
        .filter(lambda array: len(array) == 4 and array[1].isdigit() and array[2].isdigit() and array[3].isdigit())
        .map(lambda array: NetflixData(array[0], int(array[1]), int(array[2]), int(array[3])),
             output_type=Types.PICKLED_BYTE_ARRAY())
    )

    # WCZYTYWANIE DANYCH DOTYCZĄCYCH FILMÓW Z PLIKU STATYCZNEGO I DOŁĄCZANIE ICH DO STRUMIENIA
    enriched_stream = netflix_data_stream.map(
        EnrichWithMovieTitles(props.get("static.file.path"))
    )

    # AGREGACJA - WYZNACZANIE ANOMALII
    netflix_data_agg_stream = enriched_stream.map(
        lambda es: NetflixDataAgg(
            title= es.title,
            count = 1,
            avg_rate= es.rate
        )
    )
    data_keyed_by_title = netflix_data_agg_stream.key_by(lambda nd: nd.title)
    result = data_keyed_by_title.reduce(reduce_movie_data)

    filtered_result = result.filter(lambda nd: nd.avg_rate >= float(O) and nd.count >= int(L))
    # filtered_result.print()

    # ZAPISANIE ANOMALII DO TEMATU KAFKI
    sink = get_kafka_sink(props)
    filtered_result.map(lambda sd: sd.to_json(), output_type=Types.STRING()).sink_to(sink)

    env.execute("NetflixDataAnalysis")

if __name__ == "__main__":
    main()
