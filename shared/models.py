from typing import Any

from pydantic import BaseModel


class RawEvent(BaseModel):
    source: str
    source_event_id: str
    source_event_endpoint: str
    source_event_type: str
    observed_at: str
    schema_version: int
    payload: dict[str, Any]
