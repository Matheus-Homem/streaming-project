from abc import ABC, abstractmethod
from typing import Any

from shared.models import RawEvent


class IngestionClientPort(ABC):
    """Fetches raw events from a source-specific API."""

    @abstractmethod
    def get_events(self) -> list[dict[str, Any]]:
        """Make a request using the client and return the list of events.

        Returns:
            list[dict[str, Any]]: List of dictionaries, where each dictionary
                represents an event.
        """


class IngestionEnginePort(ABC):
    """Normalizes raw source-specific events into the Raw layer envelope."""

    @abstractmethod
    def process(self, events: list[dict[str, Any]]) -> list[RawEvent]:
        """Process raw dictionary events into the Raw layer event standard.

        Args:
            events (list[dict[str, Any]]): List of unstandardized events.

        Returns:
            list[RawEvent]: List of standardized events in the format expected by the Raw layer.
        """


class IngestionProducerPort(ABC):
    """Publishes standardized events to the ingestion topic."""

    @abstractmethod
    def publish(self, events: list[RawEvent]) -> None:
        """Publish all events to the Kafka topic.

        Args:
            events (list[RawEvent]): List of standardized events in the format expected by the Raw layer.
        """


class IngestionTrackerPort(ABC):
    """Tracks which events have already been processed to prevent duplicates."""

    @abstractmethod
    def is_duplicated(self, value: str) -> bool:
        """Check whether a value has already been recorded by the tracker.

        Args:
            value (str): Value to check.

        Returns:
            bool: True if the value has already been recorded, otherwise False.
        """

    @abstractmethod
    def record(self, value: str) -> None:
        """Record a value in the tracker's memory.

        Args:
            value (str): Value to record.
        """
