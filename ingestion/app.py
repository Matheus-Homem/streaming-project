import argparse
import os
from logging import getLogger

from dotenv import load_dotenv

from ingestion.adapters import (
    IngestionClient,
    IngestionEngine,
    IngestionProducer,
    IngestionTracker,
)
from ingestion.models import SourceConfig, get_source_config
from ingestion.use_case import IngestionPipeline
from ingestion.utils import RateLimitError, RetryTimer
from shared.logger import setup_logging

load_dotenv()
setup_logging(warning_level_loggers=["kafka"])


def build_arguments():
    parser = argparse.ArgumentParser(description="Data Ingestion Script")

    parser.add_argument(
        "--source",
        required=True,
        help="Data source (Required)",
    )
    parser.add_argument(
        "--endpoint",
        required=False,
        default=None,
        help="Endpoint variant (Optional)",
    )
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Endpoint parameter (can be specified multiple times)",
    )
    parser.add_argument(
        "--poll-interval",
        required=False,
        default=5,
        type=int,
        help="Interval between ingestion runs (Optional)",
    )

    return parser.parse_args()


def parse_endpoint_params(params: list[str]) -> dict[str, str]:
    result = {}
    for param in params:
        key, separator, value = param.partition("=")
        if not separator:
            raise ValueError(
                f"Invalid endpoint parameter: {param!r}. " "Expected KEY=VALUE."
            )
        result[key] = value

    return result


def configure_ingestion_pipeline(
    source_config: SourceConfig,
    bootstrap_servers: list[str],
) -> IngestionPipeline:
    return IngestionPipeline(
        client=IngestionClient(source_config),
        engine=IngestionEngine(source_config),
        producer=IngestionProducer(bootstrap_servers),
        tracker=IngestionTracker(
            120
        ),  # Cover 4 polls with 30 events/poll, which is the window that the API tend to resend the same event
    )


def main():
    logger = getLogger("IngestionApplication")
    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")

    args = build_arguments()
    source_type: str = args.source
    timer = RetryTimer(int(args.poll_interval))
    source_config = get_source_config(
        source=source_type,
        endpoint=args.endpoint,
        endpoint_params=parse_endpoint_params(args.param),
    )

    logger.info(
        f"Starting application for source={source_type} with poll_interval={args.poll_interval}"
    )
    logger.info(
        f"Using endpoint '{source_config.variant}' resolved to '{source_config.url}'"
    )

    ingestion_pipeline = configure_ingestion_pipeline(source_config, bootstrap_servers)

    while True:
        try:
            ingestion_pipeline.execute()
            logger.info("Event extraction process finished successfully")
            timer.reset().sleep()
        except RateLimitError as e:
            logger.warning(
                f"Extraction from {source_type.upper()} reached rate-limit: {e}"
            )
            timer.schedule_sleep(e.reset_at).reset()
        except Exception:
            logger.error(
                f"Extraction from {source_type.upper()} finished with unexpected errors"
            )
            logger.error(f"Sleeping for {timer} seconds")
            timer.sleep().increase()


if __name__ == "__main__":
    main()
