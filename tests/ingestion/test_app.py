import argparse
import unittest
from datetime import datetime
from unittest.mock import Mock, patch

from ingestion.adapters import (
    IngestionClient,
    IngestionEngine,
    IngestionProducer,
)
from ingestion.app import (
    build_arguments,
    configure_ingestion_pipeline,
    main,
    parse_endpoint_params,
)
from ingestion.models import get_source_config
from ingestion.utils import RateLimitError


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
        source_config = get_source_config("github")

        pipeline = configure_ingestion_pipeline(source_config)

        self.assertIsInstance(pipeline.client, IngestionClient)
        self.assertIsInstance(pipeline.engine, IngestionEngine)
        self.assertIsInstance(pipeline.producer, IngestionProducer)

    def test_client_and_engine_share_the_same_config(self):
        source_config = get_source_config(
            "github", "organization", {"org": "anthropics"}
        )

        pipeline = configure_ingestion_pipeline(source_config)

        self.assertIs(pipeline.client.source_config, source_config)
        self.assertIs(pipeline.engine.source_config, source_config)
        self.assertEqual(
            pipeline.client.url, "https://api.github.com/orgs/anthropics/events"
        )


class TestMain(unittest.TestCase):

    @patch("ingestion.app.RetryTimer")
    @patch("ingestion.app.configure_ingestion_pipeline")
    @patch("ingestion.app.build_arguments")
    def test_first_iteration_success_path(
        self,
        mock_build_arguments,
        mock_configure_pipeline,
        mock_retry_timer,
    ):
        mock_build_arguments.return_value = _args()
        pipeline = Mock()
        pipeline.execute.return_value = None
        mock_configure_pipeline.return_value = pipeline
        timer = Mock()
        timer.reset.return_value = timer
        timer.sleep.side_effect = KeyboardInterrupt()
        mock_retry_timer.return_value = timer

        with self.assertRaises(KeyboardInterrupt):
            main()

        pipeline.execute.assert_called_once()
        timer.reset.assert_called_once()
        timer.sleep.assert_called_once()

    @patch("ingestion.app.RetryTimer")
    @patch("ingestion.app.configure_ingestion_pipeline")
    @patch("ingestion.app.build_arguments")
    def test_resolved_config_is_passed_to_the_pipeline(
        self,
        mock_build_arguments,
        mock_configure_pipeline,
        mock_retry_timer,
    ):
        mock_build_arguments.return_value = _args(
            endpoint="network", param=["owner=kubernetes", "repo=kubernetes"]
        )
        pipeline = Mock()
        pipeline.execute.side_effect = KeyboardInterrupt()
        mock_configure_pipeline.return_value = pipeline
        mock_retry_timer.return_value = Mock()

        with self.assertRaises(KeyboardInterrupt):
            main()

        source_config = mock_configure_pipeline.call_args.args[0]
        self.assertEqual(source_config.source, "github")
        self.assertEqual(source_config.variant, "network")
        self.assertEqual(
            source_config.url,
            "https://api.github.com/networks/kubernetes/kubernetes/events",
        )

    @patch("ingestion.app.RetryTimer")
    @patch("ingestion.app.configure_ingestion_pipeline")
    @patch("ingestion.app.build_arguments")
    def test_unknown_source_fails_before_the_polling_loop(
        self,
        mock_build_arguments,
        mock_configure_pipeline,
        mock_retry_timer,
    ):
        mock_build_arguments.return_value = _args(source="bitbucket")
        mock_retry_timer.return_value = Mock()

        with self.assertRaises(NotImplementedError):
            main()

        mock_configure_pipeline.assert_not_called()

    @patch("ingestion.app.RetryTimer")
    @patch("ingestion.app.configure_ingestion_pipeline")
    @patch("ingestion.app.build_arguments")
    def test_missing_endpoint_param_fails_before_the_polling_loop(
        self,
        mock_build_arguments,
        mock_configure_pipeline,
        mock_retry_timer,
    ):
        mock_build_arguments.return_value = _args(
            endpoint="network", param=["owner=kubernetes"]
        )
        mock_retry_timer.return_value = Mock()

        with self.assertRaises(ValueError):
            main()

        mock_configure_pipeline.assert_not_called()

    @patch("ingestion.app.RetryTimer")
    @patch("ingestion.app.configure_ingestion_pipeline")
    @patch("ingestion.app.build_arguments")
    def test_exception_path_increases_backoff(
        self,
        mock_build_arguments,
        mock_configure_pipeline,
        mock_retry_timer,
    ):
        mock_build_arguments.return_value = _args()
        pipeline = Mock()
        pipeline.execute.side_effect = Exception("boom")
        mock_configure_pipeline.return_value = pipeline
        timer = Mock()
        timer.sleep.return_value = timer
        timer.increase.side_effect = KeyboardInterrupt()
        mock_retry_timer.return_value = timer

        with self.assertRaises(KeyboardInterrupt):
            main()

        pipeline.execute.assert_called_once()
        timer.sleep.assert_called_once()
        timer.increase.assert_called_once()

    @patch("ingestion.app.RetryTimer")
    @patch("ingestion.app.configure_ingestion_pipeline")
    @patch("ingestion.app.build_arguments")
    def test_rate_limit_error_path(
        self,
        mock_build_arguments,
        mock_configure_pipeline,
        mock_retry_timer,
    ):
        mock_build_arguments.return_value = _args()
        pipeline = Mock()
        reset_at = datetime(2028, 8, 7, 13, 48, 0)
        pipeline.execute.side_effect = RateLimitError(reset_at=reset_at)
        mock_configure_pipeline.return_value = pipeline
        timer = Mock()
        timer.schedule_sleep.return_value = timer
        timer.reset.side_effect = KeyboardInterrupt()
        mock_retry_timer.return_value = timer

        with self.assertRaises(KeyboardInterrupt):
            main()

        pipeline.execute.assert_called_once()
        timer.schedule_sleep.assert_called_once_with(reset_at)
        timer.reset.assert_called_once()


if __name__ == "__main__":
    unittest.main()
