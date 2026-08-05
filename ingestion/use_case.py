from logging import getLogger

from ingestion.client import IngestionClientBase
from ingestion.engine import IngestionEngineBase
from ingestion.publisher import IngestionPublisherBase


class IngestionPipeline:

    def __init__(
        self,
        client: IngestionClientBase,
        engine: IngestionEngineBase,
        producer: IngestionPublisherBase,
    ):
        self.logger = getLogger(self.__class__.__name__)
        self.client = client
        self.engine = engine
        self.producer = producer

    def execute(self):
        self.logger.info(f"Iniciando processo de ingestão para eventos")
        raw_events = self.client.get_events()

        self.logger.info(f"{len(raw_events)} eventos brutos obtidos")
        processed_events = self.engine.process(raw_events)
        self.producer.publish(processed_events)

        self.logger.info("Processo finalizado com sucesso")
