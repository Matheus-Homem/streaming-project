import json
from logging import getLogger

from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.serializer import Serializer

from ingestion.ports import IngestionProducerPort
from shared.models import RawEvent


class JsonSerializer(Serializer):
    def serialize(self, topic, value):
        return json.dumps(value).encode("utf-8")


class IngestionProducer(IngestionProducerPort):

    def __init__(
        self,
        bootstrap_servers: list[str],
        topic: str = "events-raw",
    ):
        self.logger = getLogger(self.__class__.__name__)
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._producer = None

    def _initialize_producer(self):
        self._producer = KafkaProducer(
            bootstrap_servers=(self._bootstrap_servers),
            value_serializer=JsonSerializer(),
        )

    def publish(self, events: list[RawEvent]) -> None:
        if not self._producer:
            self._initialize_producer()
        try:
            for event in events:
                self._producer.send(self._topic, value=event.model_dump(mode="json"))
            self.logger.info("Event enqueueing completed successfully")
            self._producer.flush()
        except KafkaError:
            self.logger.exception(f"Failed to publish events to {self._topic}")
            raise
