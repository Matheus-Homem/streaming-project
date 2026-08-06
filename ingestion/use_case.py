from logging import getLogger

from ingestion.adapters.client import IngestionClientBase
from ingestion.adapters.engine import IngestionEngineBase
from ingestion.adapters.publisher import IngestionPublisherBase


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
        raw_events = self.client.get_events()
        self.logger.info(f"{len(raw_events)} raw events fetched")
        processed_events = self.engine.process(raw_events)
        self.logger.info("Event standardization finished")
        self.producer.publish(processed_events)
        self.logger.info("Event publishing finished")
