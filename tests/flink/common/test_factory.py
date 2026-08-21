from unittest import TestCase
from unittest.mock import patch

from flink.common.factory import KafkaFactory
from flink.common.models import KafkaSinkParams, KafkaSourceParams


class TestKafkaFactoryCreateSource(TestCase):

    def test_can_build_a_source_from_the_given_params(self):
        params = KafkaSourceParams(
            topic="events-raw", group_id="group-1", bootstrap_servers="broker-1:19092"
        )

        with patch("flink.common.factory.KafkaSourceAdapter") as mock_adapter_class:
            result = KafkaFactory.create_source(params)

        mock_adapter_class.assert_called_once_with(params)
        mock_adapter_class.return_value.build.assert_called_once()
        self.assertIs(result, mock_adapter_class.return_value.build.return_value)


class TestKafkaFactoryCreateSink(TestCase):

    def test_can_build_a_sink_from_the_given_params(self):
        params = KafkaSinkParams(
            topic="events-normalized", bootstrap_servers="broker-1:19092"
        )

        with patch("flink.common.factory.KafkaSinkAdapter") as mock_adapter_class:
            result = KafkaFactory.create_sink(params)

        mock_adapter_class.assert_called_once_with(params)
        mock_adapter_class.return_value.build.assert_called_once()
        self.assertIs(result, mock_adapter_class.return_value.build.return_value)
