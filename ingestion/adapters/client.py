from abc import ABC, abstractmethod
from datetime import datetime, timezone
from logging import getLogger
from typing import Any

from requests import Response, Session

from ingestion.models import SourceType, get_source_config
from ingestion.utils import RateLimitError


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

    def _is_rate_limited(self, response: Response) -> bool:
        return (
            response.status_code in (403, 429)
            and response.headers[self.source_config.rate_limit_remaining] == "0"
        )

    def _get_rate_limit_reset(self, response: Response) -> datetime:
        reset_epoch = int(response.headers[self.source_config.rate_limit_reset])
        return datetime.fromtimestamp(reset_epoch, tz=timezone.utc)

    def _get_response(self, timeout: int = 10) -> Response:
        try:
            return self.session.get(self.url, timeout=timeout)
        except Exception:
            self.logger.exception(f"Failed to connect to '{self.url}'")
            raise

    def _parse_response(self, response: Response) -> list[dict[str, Any]]:
        try:
            response.raise_for_status()
            return response.json()
        except Exception:
            self.logger.exception(f"Failed to fetch events from '{self.url}'")
            raise

    def get_events(self) -> list[dict[str, Any]]:
        self.logger.info(f"Performing GET on url '{self.url}'")
        response = self._get_response()

        if self._is_rate_limited(response):
            raise RateLimitError(expected_at=self._get_rate_limit_reset(response))

        return self._parse_response(response)
