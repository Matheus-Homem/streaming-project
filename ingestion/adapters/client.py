from datetime import datetime, timezone
from logging import getLogger
from typing import Any

from requests import Response, Session

from ingestion.models import SourceConfig
from ingestion.ports import IngestionClientPort
from ingestion.utils import RateLimitError


class IngestionClient(IngestionClientPort):

    def __init__(
        self,
        source_config: SourceConfig,
        session: Session = None,
    ):
        self.logger = getLogger(self.__class__.__name__)
        self.session = session or Session()
        self.session.headers.update(source_config.resolve_auth_header())
        self.source_config = source_config
        self.url = source_config.url

    def _is_rate_limited(self, response: Response) -> bool:
        return (
            response.status_code in (403, 429)
            and response.headers.get(self.source_config.rate_limit_remaining) == "0"
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
            raise RateLimitError(reset_at=self._get_rate_limit_reset(response))

        return self._parse_response(response)
