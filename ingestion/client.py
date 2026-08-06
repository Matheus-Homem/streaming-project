from abc import ABC, abstractmethod
from logging import getLogger
from typing import Any

from requests import Session

from ingestion.models import SourceType, get_source_config


class IngestionClientBase(ABC):

    @abstractmethod
    def get_events(self) -> list[dict[str, Any]]:
        """Faz uma requisição no client e retorna a lista de eventos.

        Returns:
            list[dict[str, Any]]: Lista de dicionários, onde cada dicionário representa um evento.
        """


class IngestionClient(IngestionClientBase):

    def __init__(
        self,
        source: SourceType,
        owner: str = None,
        repo: str = None,
        org: str = None,
        session: Session = None,
    ):
        self.logger = getLogger(self.__class__.__name__)
        self.session = session or Session()
        self.source_config = get_source_config(source)
        self.url = self._get_url(owner, repo, org)

    def _get_url(
        self,
        owner: str = None,
        repo: str = None,
        org: str = None,
    ) -> str:
        if all(v is None for v in (owner, repo, org)):
            return self.source_config.events_url
        elif all(v is not None for v in (owner, repo)) and org is None:
            return self.source_config.network_events_url(owner, repo)
        elif org is not None and all(v is None for v in (owner, repo)):
            return self.source_config.organization_events_url(org)
        else:
            raise ValueError("Parâmetros inválidos para url")

    def get_events(self) -> list[dict[str, Any]]:
        try:
            self.logger.info(f"Performing GET on url '{self.url}'")
            events = self.session.get(self.url, timeout=10)
            events.raise_for_status()
            return events.json()
        except Exception:
            self.logger.exception(f"Failed to fetch events from '{self.url}'")
            raise
