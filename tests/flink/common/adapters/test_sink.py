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
            "flink.common.adapters.sink.RowFieldExtractorSchema"
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

    def test_can_wire_the_key_and_value_serializers_to_extract_by_row_index(self):
        key_schema, value_schema = object(), object()
        with patch("flink.common.adapters.sink.KafkaSink"), patch(
            "flink.common.adapters.sink.KafkaRecordSerializationSchema"
        ) as mock_serialization_schema, patch(
            "flink.common.adapters.sink.RowFieldExtractorSchema",
            side_effect=[key_schema, value_schema],
        ) as mock_row_field_extractor_schema:
            serializer_builder = mock_serialization_schema.builder.return_value
            self.adapter.build()

        mock_row_field_extractor_schema.assert_any_call(0)
        mock_row_field_extractor_schema.assert_any_call(1)
        set_key_serialization_schema = (
            serializer_builder.set_topic.return_value.set_key_serialization_schema
        )
        set_key_serialization_schema.assert_called_once_with(key_schema)
        set_value_serialization_schema = (
            set_key_serialization_schema.return_value.set_value_serialization_schema
        )
        set_value_serialization_schema.assert_called_once_with(value_schema)

    def test_can_log_the_topic_it_builds_for(self):
        with patch("flink.common.adapters.sink.KafkaSink"), patch(
            "flink.common.adapters.sink.KafkaRecordSerializationSchema"
        ), patch("flink.common.adapters.sink.RowFieldExtractorSchema"):
            with self.assertLogs("KafkaSinkAdapter", level="INFO") as context:
                self.adapter.build()

        self.assertTrue(
            any("events-normalized" in message for message in context.output)
        )
