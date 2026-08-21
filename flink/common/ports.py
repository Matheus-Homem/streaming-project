from abc import ABC, abstractmethod
from logging import getLogger

from pyflink.datastream.connectors.kafka import KafkaSink, KafkaSource


class EventPort(ABC):
    """Base for Flink connector ports, providing a per-class logger."""

    def __init__(self, class_name: str):
        self.logger = getLogger(class_name)


class EventSourcePort(EventPort):
    """Builds a configured Kafka source for a Flink job to consume from."""

    @abstractmethod
    def build(self) -> KafkaSource:
        """Build and return an already configured KafkaSource.

        Returns:
            KafkaSource: The configured Kafka source.
        """


class EventSinkPort(EventPort):
    """Builds a configured Kafka sink for a Flink job to publish to."""

    @abstractmethod
    def build(self) -> KafkaSink:
        """Build and return an already configured KafkaSink.

        Returns:
            KafkaSink: The configured Kafka sink.
        """
