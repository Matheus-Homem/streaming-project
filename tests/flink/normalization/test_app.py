import runpy
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import flink.normalization
from flink.common.models import KafkaSinkParams, KafkaSourceParams

CONTRACTS_DIR = Path(flink.normalization.__file__).parent / "sources"


class TestNormalizationApp(TestCase):

    def setUp(self):
        self.patchers = [
            patch("dotenv.load_dotenv"),
            patch("shared.logger.setup_logging"),
            patch("pyflink.datastream.StreamExecutionEnvironment"),
            patch("pyflink.common.watermark_strategy.WatermarkStrategy"),
            patch("pyflink.common.typeinfo.Types"),
            patch("flink.common.KafkaFactory"),
            patch(
                "flink.normalization.adapters.contract_repository.YamlContractRepository"
            ),
            patch(
                "flink.normalization.domain.evaluator.NormalizationRulesEventEvaluator"
            ),
            patch("flink.normalization.domain.normalizer.EventNormalizer"),
            patch("flink.normalization.adapters.function.NormalizationFlatMapFunction"),
        ]
        (
            self.mock_load_dotenv,
            self.mock_setup_logging,
            self.mock_stream_execution_environment,
            self.mock_watermark_strategy,
            self.mock_types,
            self.mock_kafka_factory,
            self.mock_yaml_contract_repository,
            self.mock_event_evaluator_cls,
            self.mock_event_normalizer_cls,
            self.mock_flat_map_function_cls,
        ) = (patcher.start() for patcher in self.patchers)
        for patcher in self.patchers:
            self.addCleanup(patcher.stop)

    def run_app_as_main(self):
        runpy.run_module("flink.normalization.app", run_name="__main__")

    def test_raises_key_error_when_bootstrap_servers_env_var_is_missing(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(KeyError):
                self.run_app_as_main()

        self.mock_kafka_factory.create_source.assert_not_called()

    def test_can_build_the_source_and_sink_from_the_env_var(self):
        with patch.dict("os.environ", {"KAFKA_BOOTSTRAP_SERVERS": "broker-1:19092"}):
            self.run_app_as_main()

        self.mock_kafka_factory.create_source.assert_called_once_with(
            params=KafkaSourceParams(
                topic="events-raw",
                group_id="normalization-consumer-group",
                bootstrap_servers="broker-1:19092",
            )
        )
        self.mock_kafka_factory.create_sink.assert_called_once_with(
            params=KafkaSinkParams(
                topic="events-normalized",
                bootstrap_servers="broker-1:19092",
            )
        )

    def test_can_build_the_normalizer_from_a_yaml_contract_repository(self):
        with patch.dict("os.environ", {"KAFKA_BOOTSTRAP_SERVERS": "broker-1:19092"}):
            self.run_app_as_main()

        self.mock_yaml_contract_repository.assert_called_once_with(
            contracts_dir=CONTRACTS_DIR
        )
        self.mock_event_evaluator_cls.assert_called_once_with()
        self.mock_event_normalizer_cls.assert_called_once_with(
            event_evaluator=self.mock_event_evaluator_cls.return_value,
            contract_repository=self.mock_yaml_contract_repository.return_value,
        )
        self.mock_flat_map_function_cls.assert_called_once_with(
            normalizer=self.mock_event_normalizer_cls.return_value
        )

    def test_can_wire_the_source_through_flat_map_into_the_sink_and_execute(self):
        with patch.dict("os.environ", {"KAFKA_BOOTSTRAP_SERVERS": "broker-1:19092"}):
            self.run_app_as_main()

        mock_env = (
            self.mock_stream_execution_environment.get_execution_environment.return_value
        )
        mock_stream = mock_env.from_source.return_value
        mock_flat_mapped_stream = mock_stream.flat_map.return_value

        mock_env.from_source.assert_called_once_with(
            source=self.mock_kafka_factory.create_source.return_value,
            watermark_strategy=self.mock_watermark_strategy.no_watermarks.return_value,
            source_name="Normalization Stream",
        )
        mock_stream.flat_map.assert_called_once_with(
            self.mock_flat_map_function_cls.return_value,
            output_type=self.mock_types.ROW.return_value,
        )
        self.mock_types.ROW.assert_called_once_with(
            [
                self.mock_types.PRIMITIVE_ARRAY.return_value,
                self.mock_types.PRIMITIVE_ARRAY.return_value,
            ]
        )
        self.mock_types.PRIMITIVE_ARRAY.assert_called_with(
            self.mock_types.BYTE.return_value
        )
        mock_flat_mapped_stream.sink_to.assert_called_once_with(
            self.mock_kafka_factory.create_sink.return_value
        )
        mock_env.execute.assert_called_once_with("normalization-job")
