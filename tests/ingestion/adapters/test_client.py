import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from requests import Response, Session

import ingestion
from ingestion.adapters.client import IngestionClient
from ingestion.adapters.source_config_repository import YamlSourceConfigRepository
from ingestion.utils import RateLimitError

SOURCES_DIR = Path(ingestion.__file__).parent.parent / "interface" / "sources"


class TestIngestionClient(unittest.TestCase):

    def setUp(self):
        self.session_mock = Mock(spec=Session)
        self.session_mock.headers = MagicMock()
        self.source_config_repository = YamlSourceConfigRepository(
            sources_dir=SOURCES_DIR
        )

    def _build_client(
        self,
        source="github",
        endpoint: str = None,
        endpoint_params: dict[str, str] = None,
    ):
        return IngestionClient(
            source_config=self.source_config_repository.get(
                source, endpoint, endpoint_params
            ),
            session=self.session_mock,
        )

    def test_url_without_endpoint_returns_default_events_url(self):
        client = self._build_client()

        self.assertEqual(client.url, "https://api.github.com/events")

    def test_url_for_network_endpoint_uses_owner_and_repo(self):
        client = self._build_client(
            endpoint="network",
            endpoint_params={"owner": "kubernetes", "repo": "kubernetes"},
        )

        self.assertEqual(
            client.url,
            "https://api.github.com/networks/kubernetes/kubernetes/events",
        )

    def test_url_for_organization_endpoint_uses_org(self):
        client = self._build_client(
            endpoint="organization", endpoint_params={"org": "anthropics"}
        )

        self.assertEqual(client.url, "https://api.github.com/orgs/anthropics/events")

    @patch.dict("os.environ", {"GITHUB_TOKEN": "abc123"}, clear=True)
    def test_auth_header_is_applied_to_the_session_on_init(self):
        self._build_client()

        self.session_mock.headers.update.assert_called_once_with(
            {"Authorization": "Bearer abc123"}
        )

    @patch.dict("os.environ", {}, clear=True)
    def test_no_auth_header_applied_when_token_env_var_unset(self):
        self._build_client()

        self.session_mock.headers.update.assert_called_once_with({})

    def test_client_exposes_the_resolved_source_config(self):
        client = self._build_client(
            endpoint="organization", endpoint_params={"org": "anthropics"}
        )

        self.assertEqual(client.source_config.variant, "organization")
        self.assertEqual(client.source_config.url, client.url)

    def test_is_rate_not_limited(self):
        client = self._build_client()
        response_mock = MagicMock(spec=Response)
        response_mock.status_code = 200
        response_mock.headers = {}

        self.assertFalse(client._is_rate_limited(response_mock))

    def test_is_rate_limited(self):
        client = self._build_client()
        response_mock = MagicMock(spec=Response)
        response_mock.status_code = 403
        response_mock.headers = {client.source_config.rate_limit_remaining: "0"}

        self.assertTrue(client._is_rate_limited(response_mock))

    def test_is_rate_limited_on_429(self):
        client = self._build_client()
        response_mock = MagicMock(spec=Response)
        response_mock.status_code = 429
        response_mock.headers = {client.source_config.rate_limit_remaining: "0"}

        self.assertTrue(client._is_rate_limited(response_mock))

    def test_is_rate_not_limited_on_403_with_remaining_quota(self):
        client = self._build_client()
        response_mock = MagicMock(spec=Response)
        response_mock.status_code = 403
        response_mock.headers = {client.source_config.rate_limit_remaining: "10"}

        self.assertFalse(client._is_rate_limited(response_mock))

    def test_is_rate_not_limited_on_403_without_rate_limit_header(self):
        client = self._build_client()
        response_mock = MagicMock(spec=Response)
        response_mock.status_code = 403
        response_mock.headers = {}

        self.assertFalse(client._is_rate_limited(response_mock))

    def test_can_get_rate_limit_reset(self):
        client = self._build_client()
        response_mock = MagicMock(spec=Response)
        response_mock.headers = {client.source_config.rate_limit_reset: "1786103340"}

        rate_limit_datetime = client._get_rate_limit_reset(response_mock)

        self.assertEqual(
            rate_limit_datetime,
            datetime.fromtimestamp(1786103340, tz=timezone.utc),
        )

    def test_can_get_response(self):
        client = self._build_client()
        response_mock = Mock()
        self.session_mock.get.return_value = response_mock

        response = client._get_response()

        self.assertEqual(response, response_mock)
        self.session_mock.get.assert_called_once_with(client.url, timeout=10)

    def test_can_not_get_response(self):
        client = self._build_client()
        error = Exception("Connection failed")
        client.session.get = MagicMock(side_effect=error)

        with self.assertRaises(Exception) as context:
            client._get_response()

        self.assertEqual(context.exception, error)

    def test_can_parse_response(self):
        client = self._build_client()
        expected_response_json = MagicMock()
        response_mock = MagicMock(spec=Response)
        response_mock.raise_for_status = MagicMock(return_value=True)
        response_mock.json.return_value = expected_response_json

        response_json = client._parse_response(response_mock)

        self.assertEqual(response_json, expected_response_json)
        response_mock.raise_for_status.assert_called_once_with()

    def test_can_not_parse_response(self):
        client = self._build_client()
        response_mock = MagicMock(spec=Response)
        error = Exception("Connection failed")
        response_mock.raise_for_status = MagicMock(side_effect=error)

        with self.assertRaises(Exception) as context:
            client._parse_response(response_mock)

        self.assertEqual(context.exception, error)
        response_mock.raise_for_status.assert_called_once_with()

    def test_can_get_events(self):
        client = self._build_client()
        response = MagicMock()
        expected_events = [{"id": "123"}]
        client._get_response = MagicMock(return_value=response)
        client._is_rate_limited = MagicMock(return_value=False)
        client._parse_response = MagicMock(return_value=expected_events)

        result = client.get_events()

        self.assertEqual(result, expected_events)
        client._get_response.assert_called_once_with()
        client._is_rate_limited.assert_called_once_with(response)
        client._parse_response.assert_called_once_with(response)

    def test_can_not_get_events_rate_limit_error(self):
        client = self._build_client()
        response = MagicMock()
        expected_rate_limit_reset = datetime(2028, 8, 7, 13, 48, 0)
        client._get_rate_limit_reset = MagicMock(return_value=expected_rate_limit_reset)
        client._get_response = MagicMock(return_value=response)
        client._is_rate_limited = MagicMock(return_value=True)
        client._parse_response = MagicMock()

        with self.assertRaises(RateLimitError) as context:
            client.get_events()

        self.assertEqual(context.exception.reset_at, expected_rate_limit_reset)
        self.assertIn(
            "Connection from source reached rate limit", str(context.exception)
        )
        client._get_response.assert_called_once_with()
        client._is_rate_limited.assert_called_once_with(response)
        client._parse_response.assert_not_called()


if __name__ == "__main__":
    unittest.main()
