from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema

def get_kafka_source(properties: dict) -> KafkaSource:
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(properties.get("kafka.bootstrap.servers")) \
        .set_topics(properties.get("kafka.input.topic")) \
        .set_group_id(properties.get("kafka.group.id", "flink-group")) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    return kafka_source
