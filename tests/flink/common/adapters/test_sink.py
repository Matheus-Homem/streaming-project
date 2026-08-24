import json
from unittest import TestCase
from unittest.mock import patch

from flink.common.adapters.sink import JsonFieldSerializationSchema, KafkaSinkAdapter
from flink.common.models import KafkaSinkParams


class TestKafkaSinkAdapterBuild(TestCase):

    def setUp(self):
        self.params = KafkaSinkParams(
            topic="events-normalized",
            bootstrap_servers="broker-1:19092",
            key_field="partition_key",
        )
        self.adapter = KafkaSinkAdapter(self.params)

    def test_can_build_a_sink_wired_with_the_given_params(self):
        with patch("flink.common.adapters.sink.KafkaSink") as mock_kafka_sink, patch(
            "flink.common.adapters.sink.KafkaRecordSerializationSchema"
        ) as mock_serialization_schema, patch(
            "flink.common.adapters.sink.SimpleStringSchema"
        ), patch(
            "flink.common.adapters.sink.JsonFieldSerializationSchema"
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

    def test_can_wire_the_key_serializer_with_the_configured_key_field(self):
        with patch("flink.common.adapters.sink.KafkaSink"), patch(
            "flink.common.adapters.sink.KafkaRecordSerializationSchema"
        ) as mock_serialization_schema, patch(
            "flink.common.adapters.sink.SimpleStringSchema"
        ), patch(
            "flink.common.adapters.sink.JsonFieldSerializationSchema"
        ) as mock_json_field_schema:
            serializer_builder = mock_serialization_schema.builder.return_value
            self.adapter.build()

        mock_json_field_schema.assert_called_once_with(self.params.key_field)
        set_key_serialization_schema = (
            serializer_builder.set_topic.return_value.set_key_serialization_schema
        )
        set_key_serialization_schema.assert_called_once_with(
            mock_json_field_schema.return_value
        )

    def test_can_log_the_topic_it_builds_for(self):
        with patch("flink.common.adapters.sink.KafkaSink"), patch(
            "flink.common.adapters.sink.KafkaRecordSerializationSchema"
        ), patch("flink.common.adapters.sink.SimpleStringSchema"), patch(
            "flink.common.adapters.sink.JsonFieldSerializationSchema"
        ):
            with self.assertLogs("KafkaSinkAdapter", level="INFO") as context:
                self.adapter.build()

        self.assertTrue(
            any("events-normalized" in message for message in context.output)
        )


class TestJsonFieldSerializationSchemaSerialize(TestCase):

    def setUp(self):
        self.schema = JsonFieldSerializationSchema(key_field="partition_key")

    def test_can_extract_the_configured_field_and_encode_it_to_bytes(self):
        element = json.dumps({"partition_key": "my-org/my-repo", "event_id": "1"})

        result = self.schema.serialize(element)

        self.assertEqual(result, b"my-org/my-repo")

    def test_raises_key_error_when_the_configured_field_is_missing(self):
        element = json.dumps({"event_id": "1"})

        with self.assertRaises(KeyError):
            self.schema.serialize(element)
