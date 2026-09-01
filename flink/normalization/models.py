import re
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

FROM_PATTERN = re.compile(r"^[^.]+(\.[^.]+)*$")
TYPES = ["STRING", "BOOLEAN", "PRESENCE", "TIMESTAMP", "BIGINT", "INT", "DOUBLE"]


class FieldRule(BaseModel):
    model_config = ConfigDict(extra="forbid")
    from_: Optional[str] = Field(default=None, alias="from")
    take: Optional[str] = Field(default=None)
    type_: str = Field(alias="type")
    default: Optional[Any] = Field(default=None)
    expression: Optional[str] = Field(default=None)

    @field_validator("from_")
    @classmethod
    def check_from_is_dotted(cls, v: str) -> str:
        if v and not bool(FROM_PATTERN.match(v)):
            raise ValueError(
                f"'from_' must be dot-separated (e.g. 'a.b.c'), got: {v!r}"
            )
        return v

    @field_validator("type_")
    @classmethod
    def check_type_is_formatted(cls, v: str) -> str:
        ARRAY_TYPES = [f"ARRAY<{t}>" for t in TYPES]
        if (v not in TYPES) and (v not in ARRAY_TYPES):
            raise ValueError(f"'type_' must be {TYPES} or {ARRAY_TYPES}, got: {v!r}")
        return v

    @model_validator(mode="after")
    def require_only_from_or_expression(self) -> "FieldRule":
        if not any([self.from_, self.expression]) or all([self.from_, self.expression]):
            raise ValueError("FieldRule must define either 'from_' or 'expression'")
        return self


class NormalizationContract(BaseModel):
    source: str
    partition_key: FieldRule
    envelope: dict[str, FieldRule]
    common: dict[str, FieldRule]
    event_types: dict[str, dict[str, FieldRule]]


class NormalizedEvent(BaseModel):
    model_config = ConfigDict(extra="allow")
    source: str
    event_id: str
    event_type: str
    ingested_at: datetime
    schema_version: int
    partition_key: str
    entity_id: str
    entity_name: str
    event_time: int
