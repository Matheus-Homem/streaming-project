from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource

from flink.common.models import KafkaSourceParams
from flink.common.ports import EventSourcePort


class KafkaSourceAdapter(EventSourcePort):

    def __init__(self, params: KafkaSourceParams):
        super().__init__(self.__class__.__name__)
        self._params = params

    def build(self) -> KafkaSource:
        self.logger.info(
            f"Building Kafka source for topic='{self._params.topic}' "
            f"group_id='{self._params.group_id}'"
        )
        return (
            KafkaSource.builder()
            .set_bootstrap_servers(self._params.bootstrap_servers)
            .set_topics(self._params.topic)
            .set_group_id(self._params.group_id)
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )
