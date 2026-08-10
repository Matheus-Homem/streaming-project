import unittest

from ingestion.adapters.engine import IngestionEngine
from ingestion.models import GitHubEventType, SourceType


def _valid_github_event(event_id="1", event_type="PushEvent"):
    return {
        "id": event_id,
        "type": event_type,
        "actor": {"id": 1, "login": "octocat"},
        "repo": {"id": 1, "name": "octocat/repo"},
        "payload": {"foo": "bar"},
        "public": True,
        "created_at": "2026-08-01T12:00:00Z",
    }


class TestIngestionEngine(unittest.TestCase):

    def setUp(self):
        self.engine = IngestionEngine(SourceType.GITHUB)

    def test_format_events_with_valid_events_returns_event_models(self):
        events = [_valid_github_event("1"), _valid_github_event("2")]
        formatted = self.engine._format_events(events)

        self.assertEqual(len(formatted), 2)
        self.assertEqual({e.id for e in formatted}, {"1", "2"})

    def test_format_events_with_invalid_event_is_skipped_and_logged(self):
        valid_event = _valid_github_event("1")
        invalid_event = {"id": "2"}
        with self.assertLogs(self.engine.logger.name, level="ERROR"):
            formatted = self.engine._format_events([valid_event, invalid_event])

        self.assertEqual(len(formatted), 1)
        self.assertEqual(formatted[0].id, "1")

    def test_process_returns_raw_events_with_correct_fields(self):
        events = [_valid_github_event("1", "PushEvent")]
        raw_events = self.engine.process(events)

        self.assertEqual(len(raw_events), 1)
        raw_event = raw_events[0]
        self.assertEqual(raw_event.source, SourceType.GITHUB)
        self.assertEqual(raw_event.source_event_id, "1")
        self.assertEqual(raw_event.source_event_type, GitHubEventType.PUSH)
        self.assertEqual(raw_event.schema_version, 1)
        self.assertEqual(raw_event.payload["id"], "1")

    def test_process_filters_out_invalid_events(self):
        events = [_valid_github_event("1"), {"id": "invalid-event"}]
        with self.assertLogs(self.engine.logger.name, level="ERROR"):
            raw_events = self.engine.process(events)

        self.assertEqual(len(raw_events), 1)
        self.assertEqual(raw_events[0].source_event_id, "1")

    def test_process_with_empty_list_returns_empty_list(self):
        self.assertEqual(self.engine.process([]), [])


if __name__ == "__main__":
    unittest.main()
