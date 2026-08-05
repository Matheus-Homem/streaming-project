import unittest
from unittest.mock import Mock

from requests import ConnectionError, HTTPError, Session

from ingestion.client import IngestionClient
from ingestion.models import SourceType


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

    def test_get_events_success_returns_parsed_json(self):
        client = self._build_client()
        response_mock = Mock()
        response_mock.json.return_value = [{"id": "1"}]
        response_mock.raise_for_status.return_value = None
        self.session_mock.get.return_value = response_mock

        events = client.get_events()

        self.assertEqual(events, [{"id": "1"}])
        self.session_mock.get.assert_called_once_with(client.url, timeout=10)

    def test_get_events_http_error_propagates(self):
        client = self._build_client()
        response_mock = Mock()
        response_mock.raise_for_status.side_effect = HTTPError("500 Server Error")
        self.session_mock.get.return_value = response_mock

        with self.assertRaises(HTTPError):
            client.get_events()

    def test_get_events_connection_error_propagates(self):
        client = self._build_client()
        self.session_mock.get.side_effect = ConnectionError("host unreachable")

        with self.assertRaises(ConnectionError):
            client.get_events()


if __name__ == "__main__":
    unittest.main()
