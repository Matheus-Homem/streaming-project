import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from string import Formatter

import yaml
from pydantic import BaseModel, ConfigDict

DEFAULT_ENDPOINT_VARIANT = "default"


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


@lru_cache(maxsize=1)
def _load_yaml_config() -> dict[str, SourceYamlEntry]:
    config_path = Path(__file__).parent / "config" / "sources.yml"
    with config_path.open() as file:
        config = yaml.safe_load(file)

    return {
        source_name: SourceYamlEntry.model_validate(source_config)
        for source_name, source_config in config.items()
    }


def _resolve_url(
    source: str,
    endpoints: dict[str, str],
    variant: str,
    endpoint_params: dict[str, str],
) -> str:
    try:
        template = endpoints[variant]
    except KeyError:
        raise ValueError(
            f"Unsupported endpoint '{variant}' for source '{source}'. "
            f"Available endpoints: {sorted(endpoints)}"
        )

    required_params = {
        field_name for _, field_name, _, _ in Formatter().parse(template) if field_name
    }
    missing_params = required_params - endpoint_params.keys()
    if missing_params:
        raise ValueError(
            f"Missing endpoint parameters for '{source}.{variant}': "
            f"{sorted(missing_params)}"
        )

    return template.format(**endpoint_params)


def get_source_config(
    source: str,
    endpoint: str | None = None,
    endpoint_params: dict[str, str] | None = None,
) -> SourceConfig:
    try:
        entry = _load_yaml_config()[source]
    except KeyError:
        raise NotImplementedError(f"Unsupported source: {source}")

    variant = endpoint or DEFAULT_ENDPOINT_VARIANT

    return SourceConfig(
        source=source,
        endpoints=entry.endpoints,
        headers=entry.headers,
        auth=entry.auth,
        id_field=entry.id_field,
        type_field=entry.type_field,
        variant=variant,
        url=_resolve_url(source, entry.endpoints, variant, endpoint_params or {}),
    )
