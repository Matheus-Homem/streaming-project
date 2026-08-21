from unittest import TestCase
from unittest.mock import patch

from flink.common.adapters.source import KafkaSourceAdapter
from flink.common.models import KafkaSourceParams


class TestKafkaSourceAdapterBuild(TestCase):

    def setUp(self):
        self.params = KafkaSourceParams(
            topic="events-raw",
            group_id="normalization-consumer-group",
            bootstrap_servers="broker-1:19092",
        )
        self.adapter = KafkaSourceAdapter(self.params)

    def test_can_build_a_source_wired_with_the_given_params(self):
        with patch(
            "flink.common.adapters.source.KafkaSource"
        ) as mock_kafka_source, patch(
            "flink.common.adapters.source.SimpleStringSchema"
        ):
            builder = mock_kafka_source.builder.return_value
            result = self.adapter.build()
            expected_result = (
                builder.set_bootstrap_servers.return_value.set_topics.return_value.set_group_id.return_value.set_value_only_deserializer.return_value.build.return_value
            )

        builder.set_bootstrap_servers.assert_called_once_with(
            self.params.bootstrap_servers
        )
        builder.set_bootstrap_servers.return_value.set_topics.assert_called_once_with(
            self.params.topic
        )
        self.assertIs(result, expected_result)

    def test_can_log_the_topic_and_group_id_it_builds_for(self):
        with patch("flink.common.adapters.source.KafkaSource"), patch(
            "flink.common.adapters.source.SimpleStringSchema"
        ):
            with self.assertLogs("KafkaSourceAdapter", level="INFO") as context:
                self.adapter.build()

        self.assertTrue(any("events-raw" in message for message in context.output))
        self.assertTrue(
            any("normalization-consumer-group" in message for message in context.output)
        )
