import sys
from pyflink.common import Configuration, Time, Duration
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingEventTimeWindows

from NetflixDataAnalysis.connectors.mongodb_sink import write_to_mongo
from NetflixDataAnalysis.tools.enrich_with_movie_titles import EnrichWithMovieTitles
from NetflixDataAnalysis.tools.timestamp_assigner import NetflixTimestampAssigner
from NetflixDataAnalysis.tools.unique_users_aggregator import UniqueUserAggregator
from NetflixDataAnalysis.triggers.InstantTrigger import InstantTrigger
from connectors.kafka_source import get_kafka_source
from models.netflix_data import NetflixData
from tools.properties import load_properties


def main():
    delay = sys.argv[1]
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

    # OKNO
    if delay == 'A':
        windowed_stream = enriched_stream.key_by(lambda x: (x.film_id, x.title)) \
            .window(TumblingEventTimeWindows.of(Time.days(31))) \
            .trigger(InstantTrigger.create())
    if delay == 'C':
        windowed_stream = enriched_stream.key_by(lambda x: (x.film_id, x.title)) \
            .window(TumblingEventTimeWindows.of(Time.days(31)))

    # WYNIK
    result = windowed_stream.process(UniqueUserAggregator(), output_type=Types.PICKLED_BYTE_ARRAY())
    result_json = result.map(lambda x: x.to_json())
    # PRINTOWANIE NA KONSOLE
    # result_json.print()

    #ZAPIS DO MONGODB
    result_json.map(write_to_mongo)

    env.execute("NetflixDataAnalysisETL")

if __name__ == "__main__":
    main()
