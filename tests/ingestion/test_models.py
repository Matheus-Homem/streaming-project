import unittest

from pydantic import ValidationError

from ingestion.models import (
    GitHubEvent,
    GitHubEventType,
    GitLabEvent,
    RawEvent,
    SourceType,
    get_source_config,
)


def _valid_github_event_payload(**overrides):
    payload = {
        "id": "1",
        "type": "PushEvent",
        "actor": {"id": 1, "login": "octocat"},
        "repo": {"id": 1, "name": "octocat/repo"},
        "payload": {"foo": "bar"},
        "public": True,
        "created_at": "2026-08-01T12:00:00Z",
    }
    payload.update(overrides)
    return payload


class TestGetSourceConfig(unittest.TestCase):

    def test_returns_config_for_github(self):
        config = get_source_config(SourceType.GITHUB)
        self.assertEqual(config.events_url, "https://api.github.com/events")
        self.assertEqual(config.event_model, GitHubEvent)

    def test_returns_config_for_gitlab(self):
        config = get_source_config(SourceType.GITLAB)
        self.assertEqual(config.event_model, GitLabEvent)

    def test_raises_not_implemented_for_unknown_source(self):
        with self.assertRaises(NotImplementedError):
            get_source_config("bitbucket")


class TestGitHubEvent(unittest.TestCase):

    def test_valid_payload_creates_instance(self):
        event = GitHubEvent(**_valid_github_event_payload())
        self.assertEqual(event.id, "1")
        self.assertEqual(event.type, GitHubEventType.PUSH)

    def test_org_defaults_to_none_when_absent(self):
        event = GitHubEvent(**_valid_github_event_payload())
        self.assertIsNone(event.org)

    def test_missing_required_field_raises_validation_error(self):
        payload = _valid_github_event_payload()
        del payload["actor"]
        with self.assertRaises(ValidationError):
            GitHubEvent(**payload)

    def test_invalid_event_type_raises_validation_error(self):
        with self.assertRaises(ValidationError):
            GitHubEvent(**_valid_github_event_payload(type="NotARealEventType"))


class TestGitLabEvent(unittest.TestCase):

    def test_valid_minimal_payload_creates_instance(self):
        event = GitLabEvent(id="1")
        self.assertEqual(event.id, "1")


class TestRawEvent(unittest.TestCase):

    def _valid_payload(self, **overrides):
        payload = {
            "source": SourceType.GITHUB,
            "source_event_id": "1",
            "source_event_type": GitHubEventType.PUSH,
            "observed_at": "2026-08-01T12:00:00",
            "schema_version": 1,
            "payload": {"id": "1"},
        }
        payload.update(overrides)
        return payload

    def test_valid_payload_creates_instance(self):
        raw_event = RawEvent(**self._valid_payload())
        self.assertEqual(raw_event.source, SourceType.GITHUB)
        self.assertEqual(raw_event.source_event_type, GitHubEventType.PUSH)

    def test_invalid_source_raises_validation_error(self):
        with self.assertRaises(ValidationError):
            RawEvent(**self._valid_payload(source="bitbucket"))


if __name__ == "__main__":
    unittest.main()
