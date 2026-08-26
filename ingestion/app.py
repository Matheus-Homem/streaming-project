import argparse
import os
from logging import getLogger
from pathlib import Path

from dotenv import load_dotenv

from ingestion.adapters import (
    IngestionClient,
    IngestionEngine,
    IngestionProducer,
    IngestionTracker,
    YamlSourceConfigRepository,
)
from ingestion.models import SourceConfig
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
                f"Invalid endpoint parameter: {param!r}. Expected KEY=VALUE."
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
        tracker=IngestionTracker(120),
    )


def main(
    bootstrap_servers: list[str],
    timer: RetryTimer,
    source_config: SourceConfig,
):
    logger = getLogger("IngestionApplication")
    logger.info(
        f"Starting application for source={source_config.variant} with url={source_config.url}"
    )

    ingestion_pipeline = configure_ingestion_pipeline(source_config, bootstrap_servers)

    while True:
        try:
            ingestion_pipeline.execute()
            logger.info("Event extraction process finished successfully")
            timer.reset().sleep()
        except RateLimitError as e:
            logger.warning(
                f"Extraction from {source_config.variant.upper()} reached rate-limit: {e}"
            )
            timer.schedule_sleep(e.reset_at).reset()
        except Exception:
            logger.error(
                f"Extraction from {source_config.variant.upper()} finished with unexpected errors"
            )
            logger.error(f"Sleeping for {timer} seconds")
            timer.sleep().increase()


if __name__ == "__main__":
    args = build_arguments()
    source_config_repository = YamlSourceConfigRepository(
        sources_dir=Path(__file__).parent.parent / "interface" / "sources"
    )

    main(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(","),
        timer=RetryTimer(int(args.poll_interval)),
        source_config=source_config_repository.get(
            source=args.source,
            endpoint=args.endpoint,
            endpoint_params=parse_endpoint_params(args.param),
        ),
    )
