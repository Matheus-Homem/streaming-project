import argparse
from logging import getLogger

from dotenv import load_dotenv

from ingestion.client import IngestionClient
from ingestion.engine import IngestionEngine
from ingestion.models import SourceType
from ingestion.publisher import IngestionPublisher
from ingestion.use_case import IngestionPipeline
from shared.logger import setup_logging
from shared.timer import RetryTimer

load_dotenv()
setup_logging(warning_level_loggers=["kafka"])


def build_arguments():
    parser = argparse.ArgumentParser(description="Script de Ingestão de Dados.")

    parser.add_argument(
        "--source",
        required=True,
        choices=["github", "gitlab"],
        help="Origem dos dados (Obrigatório)",
    )
    parser.add_argument(
        "--owner", required=False, default=None, help="Dono do repositório (Opcional)"
    )
    parser.add_argument(
        "--repo", required=False, default=None, help="Nome do repositório (Opcional)"
    )
    parser.add_argument(
        "--org", required=False, default=None, help="Nome da organização (Opcional)"
    )

    return parser.parse_args()


def configure_ingestion_pipeline(
    source_type: SourceType,
    owner: str = None,
    repo: str = None,
    org: str = None,
) -> IngestionPipeline:
    return IngestionPipeline(
        client=IngestionClient(source_type, owner, repo, org),
        engine=IngestionEngine(source_type),
        producer=IngestionPublisher(),
    )


def main():
    timer = RetryTimer()
    args = build_arguments()
    source_enum = SourceType(args.source)
    logger = getLogger("Application")

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
