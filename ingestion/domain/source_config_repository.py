from pathlib import Path
from string import Formatter

import yaml

from ingestion.domain import SourceConfigRepository
from ingestion.models import SourceConfig, SourceYamlEntry

DEFAULT_ENDPOINT_VARIANT = "default"


class YamlSourceConfigRepository(SourceConfigRepository):

    def __init__(self, sources_dir: Path):
        self._sources_dir = sources_dir
        self._cache: dict[str, SourceYamlEntry] = {}

    def get(
        self,
        source: str,
        endpoint: str | None = None,
        endpoint_params: dict[str, str] | None = None,
    ) -> SourceConfig:
        entry = self._load_entry(source)
        variant = endpoint or DEFAULT_ENDPOINT_VARIANT

        return SourceConfig(
            source=source,
            endpoints=entry.endpoints,
            headers=entry.headers,
            auth=entry.auth,
            id_field=entry.id_field,
            type_field=entry.type_field,
            variant=variant,
            url=self._resolve_url(
                source, entry.endpoints, variant, endpoint_params or {}
            ),
        )

    def _load_entry(self, source: str) -> SourceYamlEntry:
        if source not in self._cache:
            self._cache[source] = self._read(source)
        return self._cache[source]

    def _read(self, source: str) -> SourceYamlEntry:
        config_path = self._sources_dir / source / "ingestion.yml"
        try:
            with config_path.open() as file:
                config = yaml.safe_load(file)
        except FileNotFoundError:
            raise NotImplementedError(f"Unsupported source: {source}")

        return SourceYamlEntry.model_validate(config)

    @staticmethod
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
            field_name
            for _, field_name, _, _ in Formatter().parse(template)
            if field_name
        }
        missing_params = required_params - endpoint_params.keys()
        if missing_params:
            raise ValueError(
                f"Missing endpoint parameters for '{source}.{variant}': "
                f"{sorted(missing_params)}"
            )

        return template.format(**endpoint_params)
