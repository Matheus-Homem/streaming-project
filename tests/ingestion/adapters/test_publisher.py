import os
import unittest
from unittest.mock import Mock, patch

from kafka.errors import KafkaError

from ingestion.adapters.publisher import IngestionPublisher, JsonSerializer
from ingestion.models import GitHubEventType, RawEvent, SourceType


def _raw_event(event_id="1"):
    return RawEvent(
        source=SourceType.GITHUB,
        source_event_id=event_id,
        source_event_type=GitHubEventType.PUSH,
        observed_at="2026-08-01T12:00:00",
        schema_version=1,
        payload={"id": event_id},
    )


class TestJsonSerializer(unittest.TestCase):

    def setUp(self):
        self.serializer = JsonSerializer()

    def test_serialize_returns_json_bytes(self):
        result = self.serializer.serialize("any-topic", {"a": 1})

        self.assertEqual(result, b'{"a": 1}')

    def test_serialize_raises_type_error_for_non_serializable_value(self):
        non_serializable_value = {"a-set-is-not-json-serializable"}

        with self.assertRaises(TypeError):
            self.serializer.serialize("any-topic", non_serializable_value)


class TestIngestionPublisher(unittest.TestCase):

    def setUp(self):
        patcher = patch("ingestion.adapters.publisher.KafkaProducer")
        self.kafka_producer_cls_mock = patcher.start()
        self.addCleanup(patcher.stop)

        self.producer_instance_mock = Mock()
        self.kafka_producer_cls_mock.return_value = self.producer_instance_mock

    def test_init_uses_explicit_bootstrap_servers(self):
        publisher = IngestionPublisher(
            bootstrap_servers=["broker:9092"],
            topic="events-raw",
        )

        publisher.publish([])

        _, kwargs = self.kafka_producer_cls_mock.call_args
        self.assertEqual(kwargs["bootstrap_servers"], ["broker:9092"])

    @patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": "localhost:29092"})
    def test_init_falls_back_to_env_var_when_not_provided(self):
        publisher = IngestionPublisher()

        publisher.publish([])

        _, kwargs = self.kafka_producer_cls_mock.call_args
        self.assertEqual(kwargs["bootstrap_servers"], ["localhost:29092"])

    @patch.dict(os.environ, {}, clear=True)
    def test_init_raises_key_error_when_no_servers_and_no_env_var(self):
        publisher = IngestionPublisher()

        with self.assertRaises(KeyError):
            publisher.publish([])

    def test_publish_sends_all_events_and_flushes(self):
        publisher = IngestionPublisher(
            bootstrap_servers=["broker:9092"],
            topic="events-raw",
        )

        events = [_raw_event("1"), _raw_event("2")]

        publisher.publish(events)

        self.assertEqual(self.producer_instance_mock.send.call_count, 2)
        self.producer_instance_mock.flush.assert_called_once()

    def test_publish_raises_on_kafka_error(self):
        publisher = IngestionPublisher(
            bootstrap_servers=["broker:9092"],
            topic="events-raw",
        )

        self.producer_instance_mock.send.side_effect = KafkaError("broker down")

        with self.assertRaises(KafkaError):
            publisher.publish([_raw_event("1")])

        self.producer_instance_mock.flush.assert_not_called()


if __name__ == "__main__":
    unittest.main()
