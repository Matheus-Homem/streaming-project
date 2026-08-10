from logging import getLogger

from ingestion.ports import (
    IngestionClientBase,
    IngestionEngineBase,
    IngestionProducerBase,
    UniqueTrackerBase,
)


class IngestionPipeline:

    def __init__(
        self,
        client: IngestionClientBase,
        engine: IngestionEngineBase,
        producer: IngestionProducerBase,
        tracker: UniqueTrackerBase,
    ):
        self.logger = getLogger(self.__class__.__name__)
        self.client = client
        self.engine = engine
        self.producer = producer
        self.tracker = tracker

    def execute(self):
        events_publish = []
        raw_events = self.client.get_events()
        self.logger.info(f"{len(raw_events)} raw events fetched")

        processed_events = self.engine.process(raw_events)
        self.logger.info("Event standardization finished")

        for event in processed_events:
            if self.tracker.is_duplicated(value=event.source_event_id):
                self.logger.info("Skipping duplicated event")
            else:
                self.tracker.record(value=event.source_event_id)
                events_publish.append(event)

        self.producer.publish(events_publish)
        self.logger.info(f"{len(events_publish)} successful published")
