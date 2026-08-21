import re
from abc import ABC, abstractmethod
from datetime import datetime
from logging import getLogger
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from shared.models import RawEvent

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


class EventEvaluator(ABC):
    """Applies a normalization contract's rules to a raw event's payload."""

    def __init__(self, class_name: str):
        self.logger = getLogger(class_name)

    @abstractmethod
    def apply(self, event: RawEvent, contract: NormalizationContract) -> dict[str, Any]:
        """Apply the contract's rules to the event's payload.

        Args:
            event (RawEvent): Raw event whose payload will be evaluated.
            contract (NormalizationContract): Contract defining the rules to apply.

        Returns:
            dict[str, Any]: The normalized fields produced by the contract's rules.
        """
