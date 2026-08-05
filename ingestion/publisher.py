import json
import os
from abc import ABC, abstractmethod
from logging import getLogger

from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.serializer import Serializer

from ingestion.models import RawEvent


class IngestionPublisherBase(ABC):

    @abstractmethod
    def publish(self, events: list[RawEvent]) -> None:
        """Publica todos os eventos no tópico kafka

        Args:
            events (list[RawEvent]): Lista de eventos padronizados da forma que é esperada pela camada Raw.
        """


class JsonSerializer(Serializer):
    def serialize(self, topic, value):
        return json.dumps(value).encode("utf-8")


class IngestionPublisher(IngestionPublisherBase):

    def __init__(
        self,
        bootstrap_servers: list[str] = None,
        topic: str = "events-raw",
    ):
        self.logger = getLogger(self.__class__.__name__)
        self._producer = KafkaProducer(
            bootstrap_servers=(
                bootstrap_servers or [os.environ["KAFKA_BOOTSTRAP_SERVERS"]]
            ),
            value_serializer=JsonSerializer(),
        )
        self._topic = topic

    def publish(self, events: list[RawEvent]) -> None:
        try:
            for event in events:
                self._producer.send(self._topic, value=event.model_dump(mode="json"))
            self.logger.info("Todos eventos enfileirados com sucesso")
            self._producer.flush()
        except KafkaError:
            self.logger.exception(f"Falha ao publicar eventos em {self._topic}")
            raise
