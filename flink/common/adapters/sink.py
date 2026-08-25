from pyflink.common.serialization import RowFieldExtractorSchema
from pyflink.datastream.connectors.kafka import (
    KafkaRecordSerializationSchema,
    KafkaSink,
)

from flink.common.models import KafkaSinkParams
from flink.common.ports import EventSinkPort


class KafkaSinkAdapter(EventSinkPort):

    def __init__(self, params: KafkaSinkParams):
        super().__init__(self.__class__.__name__)
        self._params = params

    def build(self) -> KafkaSink:
        self.logger.info(f"Building Kafka sink for topic='{self._params.topic}'")
        return (
            KafkaSink.builder()
            .set_bootstrap_servers(self._params.bootstrap_servers)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(self._params.topic)
                .set_key_serialization_schema(RowFieldExtractorSchema(0))
                .set_value_serialization_schema(RowFieldExtractorSchema(1))
                .build()
            )
            .build()
        )
