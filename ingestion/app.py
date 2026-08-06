import argparse
from logging import getLogger

from dotenv import load_dotenv

import ingestion.adapters as ing
from ingestion.models import SourceType
from ingestion.use_case import IngestionPipeline
from ingestion.utils import RetryTimer
from shared.logger import setup_logging

load_dotenv()
setup_logging(warning_level_loggers=["kafka"])


def build_arguments():
    parser = argparse.ArgumentParser(description="Data Ingestion Script")

    parser.add_argument(
        "--source",
        required=True,
        choices=["github", "gitlab"],
        help="Data source (Required)",
    )
    parser.add_argument(
        "--owner", required=False, default=None, help="Repository owner (Optional)"
    )
    parser.add_argument(
        "--repo", required=False, default=None, help="Repository name (Optional)"
    )
    parser.add_argument(
        "--org", required=False, default=None, help="Organization name (Optional)"
    )
    parser.add_argument(
        "--poll-interval",
        required=False,
        default=5,
        help="Interval between ingestion runs (Optional)",
    )

    return parser.parse_args()


def configure_ingestion_pipeline(
    source_type: SourceType,
    owner: str = None,
    repo: str = None,
    org: str = None,
) -> IngestionPipeline:
    return IngestionPipeline(
        client=ing.IngestionClient(source_type, owner, repo, org),
        engine=ing.IngestionEngine(source_type),
        producer=ing.IngestionPublisher(),
    )


def main():
    logger = getLogger("Application")
    args = build_arguments()
    logger.info(
        f"Starting application with parameters source={args.source}, owner={args.owner}, repo={args.repo}, org={args.org}, poll_interval={args.poll_interval}"
    )
    timer = RetryTimer(int(args.poll_interval))
    source_enum = SourceType(args.source)

    ingestion_pipeline = configure_ingestion_pipeline(
        source_type=source_enum,
        owner=args.owner,
        repo=args.repo,
        org=args.org,
    )

    while True:
        try:
            logger.info("Starting event extraction process")
            ingestion_pipeline.execute()
            logger.info("Event extraction process finished successfully")
            timer.reset().sleep()
        except Exception:
            logger.info("Event extraction process finished with errors")
            logger.info(f"Sleeping for {timer} seconds")
            timer.sleep().increase()


if __name__ == "__main__":
    main()
