import argparse
import unittest
from unittest.mock import Mock, patch

from ingestion.adapters import (
    IngestionClient,
    IngestionEngine,
    IngestionPublisher,
)
from ingestion.app import (
    build_arguments,
    configure_ingestion_pipeline,
    main,
)
from ingestion.models import SourceType


class TestBuildArguments(unittest.TestCase):

    @patch("sys.argv", ["app.py", "--source", "github"])
    def test_default_poll_interval_is_5(self):
        args = build_arguments()

        self.assertEqual(args.source, "github")
        self.assertEqual(args.poll_interval, 5)

    @patch("sys.argv", ["app.py"])
    def test_source_is_required(self):
        with self.assertRaises(SystemExit):
            build_arguments()

    @patch("sys.argv", ["app.py", "--source", "invalid"])
    def test_source_rejects_invalid_choice(self):
        with self.assertRaises(SystemExit):
            build_arguments()


class TestConfigureIngestionPipeline(unittest.TestCase):

    def test_builds_pipeline_with_correct_source_type(self):
        pipeline = configure_ingestion_pipeline(source_type=SourceType.GITHUB)

        self.assertIsInstance(pipeline.client, IngestionClient)
        self.assertIsInstance(pipeline.engine, IngestionEngine)
        self.assertIsInstance(pipeline.producer, IngestionPublisher)


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
        mock_build_arguments.return_value = argparse.Namespace(
            source="github",
            owner=None,
            repo=None,
            org=None,
            poll_interval=5,
        )
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
    def test_exception_path_increases_backoff(
        self,
        mock_build_arguments,
        mock_configure_pipeline,
        mock_retry_timer,
    ):
        mock_build_arguments.return_value = argparse.Namespace(
            source="github",
            owner=None,
            repo=None,
            org=None,
            poll_interval=5,
        )
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


if __name__ == "__main__":
    unittest.main()
