import os
from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict


class EventModel(BaseModel):
    model_config = ConfigDict(extra="allow")
    id: str
    type: str


class AuthConfig(BaseModel):
    env_var: str
    header: str
    value_template: str


class SourceYamlEntry(BaseModel):
    endpoints: dict[str, str]
    headers: dict[str, str]
    auth: AuthConfig | None = None
    id_field: str
    type_field: str


@dataclass(frozen=True)
class SourceConfig:
    source: str
    endpoints: dict[str, str]
    headers: dict[str, str]
    auth: AuthConfig | None
    id_field: str
    type_field: str
    variant: str
    url: str

    def resolve_auth_header(self) -> dict[str, str]:
        if self.auth is None:
            return {}

        token = os.environ.get(self.auth.env_var)
        if not token:
            return {}

        return {self.auth.header: self.auth.value_template.format(token=token)}

    def get_event_id(self, event: dict) -> str:
        return self._get_nested_value(event, self.id_field)

    def get_event_type(self, event: dict) -> str:
        return self._get_nested_value(event, self.type_field)

    @staticmethod
    def _get_nested_value(event: dict, path: str):
        value = event
        for key in path.split("."):
            if not isinstance(value, dict) or key not in value:
                raise ValueError(f"Could not resolve path '{path}' in event")
            value = value[key]
        return value

    @property
    def rate_limit_remaining(self) -> str:
        return self.headers["rate_limit_remaining"]

    @property
    def rate_limit_reset(self) -> str:
        return self.headers["rate_limit_reset"]
