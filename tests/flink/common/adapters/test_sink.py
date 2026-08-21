from unittest import TestCase
from unittest.mock import patch

from flink.common.adapters.sink import KafkaSinkAdapter
from flink.common.models import KafkaSinkParams


class TestKafkaSinkAdapterBuild(TestCase):

    def setUp(self):
        self.params = KafkaSinkParams(
            topic="events-normalized",
            bootstrap_servers="broker-1:19092",
        )
        self.adapter = KafkaSinkAdapter(self.params)

    def test_can_build_a_sink_wired_with_the_given_params(self):
        with patch("flink.common.adapters.sink.KafkaSink") as mock_kafka_sink, patch(
            "flink.common.adapters.sink.KafkaRecordSerializationSchema"
        ) as mock_serialization_schema, patch(
            "flink.common.adapters.sink.SimpleStringSchema"
        ):
            serializer_builder = mock_serialization_schema.builder.return_value
            sink_builder = mock_kafka_sink.builder.return_value
            result = self.adapter.build()
            expected_result = (
                sink_builder.set_bootstrap_servers.return_value.set_record_serializer.return_value.build.return_value
            )

        serializer_builder.set_topic.assert_called_once_with(self.params.topic)
        sink_builder.set_bootstrap_servers.assert_called_once_with(
            self.params.bootstrap_servers
        )
        self.assertIs(result, expected_result)

    def test_can_log_the_topic_it_builds_for(self):
        with patch("flink.common.adapters.sink.KafkaSink"), patch(
            "flink.common.adapters.sink.KafkaRecordSerializationSchema"
        ), patch("flink.common.adapters.sink.SimpleStringSchema"):
            with self.assertLogs("KafkaSinkAdapter", level="INFO") as context:
                self.adapter.build()

        self.assertTrue(
            any("events-normalized" in message for message in context.output)
        )
