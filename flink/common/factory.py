from flink.common.adapters.sink import KafkaSinkAdapter
from flink.common.adapters.source import KafkaSourceAdapter
from flink.common.models import KafkaSinkParams, KafkaSourceParams


class KafkaFactory:

    @staticmethod
    def create_source(params: KafkaSourceParams):
        return KafkaSourceAdapter(params).build()

    @staticmethod
    def create_sink(params: KafkaSinkParams):
        return KafkaSinkAdapter(params).build()
