from datetime import datetime, timezone
from logging import getLogger
from typing import Any

from pydantic import ValidationError

from ingestion.models import EventModel, SourceConfig
from ingestion.ports import IngestionEngineBase
from shared.models import RawEvent


class IngestionEngine(IngestionEngineBase):

    def __init__(self, source_config: SourceConfig):
        self.logger = getLogger(self.__class__.__name__)
        self.source_config = source_config

    def _format_events(self, events: list[dict[str, Any]]) -> list[EventModel]:
        formatted_events = []
        for event in events:
            try:
                formatted_event = EventModel(
                    **{
                        **event,
                        "id": str(self.source_config.get_event_id(event)),
                        "type": str(self.source_config.get_event_type(event)),
                    }
                )
                formatted_events.append(formatted_event)
            except (ValidationError, KeyError, ValueError):
                self.logger.exception(f"Invalid event skipped (id={event.get('id')})")
        return formatted_events

    def process(self, events: list[dict[str, Any]]) -> list[RawEvent]:
        formatted_events = self._format_events(events)
        self.logger.info("Events formatted successfully")

        return [
            RawEvent(
                source=self.source_config.source,
                source_event_id=formatted_event.id,
                source_event_endpoint=self.source_config.variant,
                source_event_type=formatted_event.type,
                observed_at=datetime.now(timezone.utc).isoformat(),
                schema_version=1,
                payload=formatted_event.model_dump(mode="json"),
            )
            for formatted_event in formatted_events
        ]
