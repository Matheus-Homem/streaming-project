import re
from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

FROM_PATTERN = re.compile(r"^[^.]+(\.[^.]+)*$")


class FieldRule(BaseModel):
    model_config = ConfigDict(extra="forbid")
    from_: Optional[str] = Field(default=None, alias="from")
    take: Optional[str] = Field(default=None)
    as_: Optional[Literal["boolean", "timestamp"]] = Field(default=None, alias="as")
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
