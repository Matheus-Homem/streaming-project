from abc import ABC, abstractmethod
from typing import Any

from shared.models import RawEvent


class EventClientPort(ABC):
    """Fetches raw events from a source-specific API."""

    @abstractmethod
    def get_events(self) -> list[dict[str, Any]]:
        """Make a request using the client and return the list of events.

        Returns:
            list[dict[str, Any]]: List of dictionaries, where each dictionary
                represents an event.
        """


class EventProducerPort(ABC):
    """Publishes standardized events to the ingestion topic."""

    @abstractmethod
    def publish(self, events: list[RawEvent]) -> None:
        """Publish all events to the Kafka topic.

        Args:
            events (list[RawEvent]): List of standardized events in the format expected by the Raw layer.
        """
