import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock

from requests import Response, Session

from ingestion.adapters.client import IngestionClient
from ingestion.models import SourceType
from ingestion.utils import RateLimitError


class TestIngestionClient(unittest.TestCase):

    def setUp(self):
        self.session_mock = Mock(spec=Session)

    def _build_client(self, owner=None, repo=None, org=None, source=SourceType.GITHUB):
        return IngestionClient(
            source=source,
            owner=owner,
            repo=repo,
            org=org,
            session=self.session_mock,
        )

    def test_get_url_without_params_returns_events_url(self):
        client = self._build_client()
        self.assertEqual(client.url, "https://api.github.com/events")

    def test_get_url_with_owner_and_repo_returns_network_events_url(self):
        client = self._build_client(owner="kubernetes", repo="kubernetes")
        self.assertEqual(
            client.url,
            "https://api.github.com/networks/kubernetes/kubernetes/events",
        )

    def test_get_url_with_org_returns_organization_events_url(self):
        client = self._build_client(org="anthropics")
        self.assertEqual(client.url, "https://api.github.com/orgs/anthropics/events")

    def test_get_url_with_only_owner_raises_value_error(self):
        with self.assertRaises(ValueError):
            self._build_client(owner="kubernetes")

    def test_get_url_with_only_repo_raises_value_error(self):
        with self.assertRaises(ValueError):
            self._build_client(repo="kubernetes")

    def test_get_url_with_owner_repo_and_org_together_raises_value_error(self):
        with self.assertRaises(ValueError):
            self._build_client(owner="kubernetes", repo="kubernetes", org="anthropics")

    def test_is_rate_not_limited(self):
        client = self._build_client()
        response_mock = MagicMock(spec=Response)
        response_mock.status_code = 200

        request_not_limited = client._is_rate_limited(response_mock)

        self.assertEqual(request_not_limited, False)

    def test_is_rate_limited(self):
        client = self._build_client()
        rate_limit_remaining = client.source_config.rate_limit_remaining
        response_mock = MagicMock(spec=Response)
        response_mock.status_code = 403
        response_mock.headers = {rate_limit_remaining: "0"}

        request_not_limited = client._is_rate_limited(response_mock)

        self.assertEqual(request_not_limited, True)

    def test_is_rate_not_limited_on_403_with_remaining_quota(self):
        client = self._build_client()
        rate_limit_remaining = client.source_config.rate_limit_remaining
        response_mock = MagicMock(spec=Response)
        response_mock.status_code = 403
        response_mock.headers = {rate_limit_remaining: "10"}

        request_not_limited = client._is_rate_limited(response_mock)

        self.assertEqual(request_not_limited, False)

    def test_can_get_rate_limit_reset(self):
        client = self._build_client()
        rate_limit_reset = client.source_config.rate_limit_reset
        response_mock = MagicMock(spec=Response)
        response_mock.headers = {rate_limit_reset: "1786103340"}

        rate_limit_datetime = client._get_rate_limit_reset(response_mock)

        expected_timestamp = 1786103340
        expected_datetime = rate_limit_datetime.fromtimestamp(
            expected_timestamp, tz=timezone.utc
        )
        self.assertEqual(rate_limit_datetime, expected_datetime)

    def test_can_get_response(self):
        client = self._build_client()
        response_mock = Mock()
        self.session_mock.get.return_value = response_mock

        response = client._get_response()
        self.assertEqual(response, response_mock)

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

    def test_can_get_events_on_403_without_rate_limit(self):
        client = self._build_client()
        response = MagicMock()
        expected_events = [{"id": "123"}]
        client._get_response = MagicMock(return_value=response)
        client._is_rate_limited = MagicMock(return_value=False)
        client._parse_response = MagicMock(return_value=expected_events)

        result = client.get_events()

        self.assertEqual(result, expected_events)
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
