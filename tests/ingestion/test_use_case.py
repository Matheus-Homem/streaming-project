import unittest
from unittest.mock import Mock, call

from ingestion.adapters.client import IngestionClientBase
from ingestion.adapters.engine import IngestionEngineBase
from ingestion.adapters.publisher import IngestionPublisherBase
from ingestion.use_case import IngestionPipeline


class TestIngestionPipeline(unittest.TestCase):

    def setUp(self):
        self.client_mock = Mock(spec=IngestionClientBase)
        self.engine_mock = Mock(spec=IngestionEngineBase)
        self.producer_mock = Mock(spec=IngestionPublisherBase)
        self.pipeline = IngestionPipeline(
            client=self.client_mock,
            engine=self.engine_mock,
            producer=self.producer_mock,
        )
        self.manager = Mock()
        self.manager.attach_mock(self.client_mock, "client")
        self.manager.attach_mock(self.engine_mock, "engine")
        self.manager.attach_mock(self.producer_mock, "producer")

    def test_execute_calls_dependencies_in_order_with_correct_arguments(self):
        raw_events = [{"id": "1"}]
        processed_events = [Mock()]
        self.client_mock.get_events.return_value = raw_events
        self.engine_mock.process.return_value = processed_events

        self.pipeline.execute()

        self.assertEqual(
            self.manager.mock_calls,
            [
                call.client.get_events(),
                call.engine.process(raw_events),
                call.producer.publish(processed_events),
            ],
        )

    def test_execute_stops_when_get_events_fails(self):
        self.client_mock.get_events.side_effect = ConnectionError("falha de rede")

        with self.assertRaises(ConnectionError):
            self.pipeline.execute()

        self.engine_mock.process.assert_not_called()
        self.producer_mock.publish.assert_not_called()

    def test_execute_stops_when_process_fails(self):
        self.client_mock.get_events.return_value = [{"id": "1"}]
        self.engine_mock.process.side_effect = ValueError("evento inválido")

        with self.assertRaises(ValueError):
            self.pipeline.execute()

        self.producer_mock.publish.assert_not_called()

    def test_execute_propagates_publish_failure(self):
        self.client_mock.get_events.return_value = [{"id": "1"}]
        self.engine_mock.process.return_value = [Mock()]
        self.producer_mock.publish.side_effect = RuntimeError("kafka indisponível")

        with self.assertRaises(RuntimeError):
            self.pipeline.execute()


if __name__ == "__main__":
    unittest.main()
