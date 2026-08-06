from abc import ABC, abstractmethod
from datetime import datetime
from logging import getLogger
from typing import Any

from pydantic import ValidationError

from ingestion.models import EventModel, RawEvent, SourceType, get_source_config


class IngestionEngineBase(ABC):

    @abstractmethod
    def process(self, events: list[dict[str, Any]]) -> list[RawEvent]:
        """Processa eventos brutos em forma de dicionário para o padrão de eventos esparos pela camada Raw.

        Args:
            events (list[dict[str, Any]]): Lista de eventos despadronizados.

        Returns:
            list[RawEvent]: Lista de eventos padronizados da forma que é esperada pela camada Raw.
        """


class IngestionEngine(IngestionEngineBase):

    def __init__(self, source: SourceType):
        self.logger = getLogger(self.__class__.__name__)
        self.source = source
        self._event_model = get_source_config(source).event_model

    def _format_events(self, events: list[dict[str, Any]]) -> list[EventModel]:
        formatted_events = []
        for event in events:
            try:
                formatted_events.append(self._event_model(**event))
            except ValidationError:
                self.logger.exception(
                    f"Invalid event skipped (id={event.get("id")})"
                )
        return formatted_events

    def process(self, events: list[dict[str, Any]]) -> list[RawEvent]:
        formatted_events = self._format_events(events)
        self.logger.info("Events formatted successfully")

        return [
            RawEvent(
                source=self.source,
                source_event_id=formatted_event.id,
                source_event_type=formatted_event.type,
                observed_at=datetime.now().isoformat(),
                schema_version=1,
                payload=formatted_event.model_dump(mode="json"),
            )
            for formatted_event in formatted_events
        ]
