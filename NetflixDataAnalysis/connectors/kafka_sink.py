from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema

def get_kafka_sink(properties: dict) -> KafkaSink:

    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(properties.get("kafka.bootstrap.servers")) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(properties.get("kafka.output.topic"))
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_transactional_id_prefix("Flink") \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    return kafka_sink