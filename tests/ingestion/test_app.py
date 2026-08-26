import argparse
import unittest
from datetime import datetime
from unittest.mock import Mock, patch

from ingestion.adapters import KafkaProducerAdapter, RequestsClientAdapter
from ingestion.app import (
    SOURCES_DIR,
    build_arguments,
    configure_ingestion_pipeline,
    main,
    parse_endpoint_params,
)
from ingestion.domain.formatter import ValidatingRawEventFormatter
from ingestion.domain.source_config_repository import YamlSourceConfigRepository
from ingestion.utils import RateLimitError

source_config_repository = YamlSourceConfigRepository(sources_dir=SOURCES_DIR)


def _args(**overrides):
    defaults = {
        "source": "github",
        "endpoint": None,
        "param": [],
        "poll_interval": 5,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class TestBuildArguments(unittest.TestCase):

    @patch("sys.argv", ["app.py", "--source", "github"])
    def test_defaults_when_only_source_is_given(self):
        args = build_arguments()

        self.assertEqual(args.source, "github")
        self.assertEqual(args.poll_interval, 5)
        self.assertIsNone(args.endpoint)
        self.assertEqual(args.param, [])

    @patch("sys.argv", ["app.py", "--source", "github", "--poll-interval", "10"])
    def test_explicit_poll_interval_is_used(self):
        args = build_arguments()

        self.assertEqual(args.poll_interval, 10)
        self.assertIsInstance(args.poll_interval, int)

    @patch(
        "sys.argv",
        [
            "app.py",
            "--source",
            "github",
            "--endpoint",
            "network",
            "--param",
            "owner=kubernetes",
            "--param",
            "repo=kubernetes",
        ],
    )
    def test_endpoint_and_repeated_params_are_collected(self):
        args = build_arguments()

        self.assertEqual(args.endpoint, "network")
        self.assertEqual(args.param, ["owner=kubernetes", "repo=kubernetes"])

    @patch("sys.argv", ["app.py"])
    def test_source_is_required(self):
        with self.assertRaises(SystemExit):
            build_arguments()


class TestParseEndpointParams(unittest.TestCase):

    def test_empty_list_returns_empty_dict(self):
        self.assertEqual(parse_endpoint_params([]), {})

    def test_key_value_pairs_are_parsed(self):
        result = parse_endpoint_params(["owner=kubernetes", "repo=kubernetes"])

        self.assertEqual(result, {"owner": "kubernetes", "repo": "kubernetes"})

    def test_value_containing_equal_sign_is_preserved(self):
        self.assertEqual(parse_endpoint_params(["token=a=b"]), {"token": "a=b"})

    def test_missing_separator_raises_value_error(self):
        with self.assertRaises(ValueError) as context:
            parse_endpoint_params(["owner"])

        self.assertIn("Expected KEY=VALUE", str(context.exception))


class TestConfigureIngestionPipeline(unittest.TestCase):

    def test_builds_pipeline_from_source_config(self):
        source_config = source_config_repository.get("github")

        pipeline = configure_ingestion_pipeline(source_config, ["broker:9092"])

        self.assertIsInstance(pipeline.client, RequestsClientAdapter)
        self.assertIsInstance(pipeline.engine, ValidatingRawEventFormatter)
        self.assertIsInstance(pipeline.producer, KafkaProducerAdapter)

    def test_client_and_engine_share_the_same_config(self):
        source_config = source_config_repository.get(
            "github", "organization", {"org": "anthropics"}
        )

        pipeline = configure_ingestion_pipeline(source_config, ["broker:9092"])

        self.assertIs(pipeline.client.source_config, source_config)
        self.assertIs(pipeline.engine.source_config, source_config)
        self.assertEqual(
            pipeline.client.url, "https://api.github.com/orgs/anthropics/events"
        )


class TestMain(unittest.TestCase):
    """main() now only orchestrates the polling loop from already-resolved
    arguments; resolving them (env var, CLI args, source config) is the
    __main__ block's job, covered by TestBuildArguments, TestParseEndpointParams
    and TestConfigureIngestionPipeline instead.
    """

    def setUp(self):
        self.source_config = Mock()
        self.source_config.variant = "github"
        self.source_config.url = "https://api.github.com/events"
        self.timer = Mock()

    @patch("ingestion.app.configure_ingestion_pipeline")
    def test_first_iteration_success_path(self, mock_configure_pipeline):
        pipeline = Mock()
        pipeline.execute.return_value = None
        mock_configure_pipeline.return_value = pipeline
        self.timer.reset.return_value = self.timer
        self.timer.sleep.side_effect = KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            main(["broker:9092"], self.timer, self.source_config)

        mock_configure_pipeline.assert_called_once_with(
            self.source_config, ["broker:9092"]
        )
        pipeline.execute.assert_called_once()
        self.timer.reset.assert_called_once()
        self.timer.sleep.assert_called_once()

    @patch("ingestion.app.configure_ingestion_pipeline")
    def test_exception_path_increases_backoff(self, mock_configure_pipeline):
        pipeline = Mock()
        pipeline.execute.side_effect = Exception("boom")
        mock_configure_pipeline.return_value = pipeline
        self.timer.sleep.return_value = self.timer
        self.timer.increase.side_effect = KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            main(["broker:9092"], self.timer, self.source_config)

        pipeline.execute.assert_called_once()
        self.timer.sleep.assert_called_once()
        self.timer.increase.assert_called_once()

    @patch("ingestion.app.configure_ingestion_pipeline")
    def test_rate_limit_error_path(self, mock_configure_pipeline):
        pipeline = Mock()
        reset_at = datetime(2028, 8, 7, 13, 48, 0)
        pipeline.execute.side_effect = RateLimitError(reset_at=reset_at)
        mock_configure_pipeline.return_value = pipeline
        self.timer.schedule_sleep.return_value = self.timer
        self.timer.reset.side_effect = KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            main(["broker:9092"], self.timer, self.source_config)

        pipeline.execute.assert_called_once()
        self.timer.schedule_sleep.assert_called_once_with(reset_at)
        self.timer.reset.assert_called_once()


if __name__ == "__main__":
    unittest.main()
