import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, Optional

import yaml
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
            raise ValueError(f"FieldRule must define either 'from_' or 'expression'")
        return self


class NormalizationContract(BaseModel):
    source: str
    partition_key: FieldRule
    envelope: dict[str, FieldRule]
    common: dict[str, FieldRule]
    event_types: dict[str, dict[str, FieldRule]]


@lru_cache(maxsize=1)
def get_contract(source: str) -> NormalizationContract:
    config_path = Path(__file__).parent / "config" / "sources" / f"{source}.yml"
    try:
        with config_path.open() as file:
            config = yaml.safe_load(file)
        return NormalizationContract.model_validate(config)
    except FileNotFoundError:
        raise NotImplementedError(
            f"Source '{source}' is not implemented in Normalization pipeline"
        )
