import sys

from pyflink.common import Configuration, Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.common import Duration

from NetflixDataAnalysis.connectors.kafka_sink import get_kafka_sink
from NetflixDataAnalysis.models.netflix_data_agg import NetflixDataAgg
from NetflixDataAnalysis.tools.add_window_metadata import AddWindowMetadata
from NetflixDataAnalysis.tools.enrich_with_movie_titles import EnrichWithMovieTitles
from NetflixDataAnalysis.tools.timestamp_assigner import NetflixTimestampAssigner
from connectors.kafka_source import get_kafka_source
from models.netflix_data import NetflixData
from tools.properties import load_properties


def main():
    # PARAMETRY
    D = int(sys.argv[1])
    L = int(sys.argv[2])
    O = float(sys.argv[3])
    print(f"Parameters - D:{D} L:{L} O:{O}")
    props = load_properties("flink.properties")
    print("Input Kafka topic:", props.get("kafka.input.topic"))
    print("Static input file:", props.get("static.file.path"))
    config = Configuration()
    config.set_string("pipeline.jars", props.get("pipeline.jars"))
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)

    # WCZYTYWANIE DANYCH STRUMIENIOWYCH Z TEMATU KAFKI
    source = get_kafka_source(props)
    data_stream = env.from_source(source,  watermark_strategy=WatermarkStrategy.no_watermarks(), source_name="KafkaSource")

    netflix_data_stream = (
        data_stream
        .map(lambda line: line.split(","), output_type=Types.OBJECT_ARRAY(Types.STRING()))
        .filter(lambda array: len(array) == 4 and array[1].isdigit() and array[2].isdigit() and array[3].isdigit())
        .map(lambda array: NetflixData(array[0], int(array[1]), int(array[2]), int(array[3])),
             output_type=Types.PICKLED_BYTE_ARRAY())
    )

    # USTAWIENIE WATERMARKÓW
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_days(1)) \
        .with_timestamp_assigner(NetflixTimestampAssigner())
    netflix_data_stream = netflix_data_stream.assign_timestamps_and_watermarks(watermark_strategy)

    # WCZYTYWANIE DANYCH DOTYCZĄCYCH FILMÓW Z PLIKU STATYCZNEGO I DOŁĄCZANIE ICH DO STRUMIENIA
    enriched_stream = netflix_data_stream.map(
        EnrichWithMovieTitles(props.get("static.file.path"))
    )

    # AGREGACJA - WYZNACZANIE ANOMALII
    netflix_data_agg_stream = enriched_stream.map(
        lambda es: NetflixDataAgg(
            window_start=0,
            window_end=0,
            title=es.title,
            count=1,
            avg_rate=es.rate
        ),
        output_type=Types.PICKLED_BYTE_ARRAY()
    )

    # OKNO
    windowed_stream = (
        netflix_data_agg_stream
        .key_by(lambda x: x.title)
        .window(SlidingEventTimeWindows.of(Time.days(D), Time.days(1)))
    )
    result = windowed_stream.process(AddWindowMetadata(), output_type=Types.PICKLED_BYTE_ARRAY())
    filtered_result = result.filter(lambda nd: nd.avg_rate >= O and nd.count >= L)

    # PRINTOWANIE NA KONSOLE
    # final_anomalies = filtered_result.map(lambda x: x.to_json(), output_type=Types.STRING())
    # final_anomalies.print()

    # ZAPISANIE ANOMALII DO TEMATU KAFKI
    sink = get_kafka_sink(props)
    filtered_result.map(lambda x: x.to_json(), output_type=Types.STRING()).sink_to(sink)

    env.execute("NetflixDataAnalysisAnomalies")

if __name__ == "__main__":
    main()
