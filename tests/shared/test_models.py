import unittest

from pydantic import ValidationError

from shared.models import RawEvent


class TestRawEvent(unittest.TestCase):

    def _valid_payload(self, **overrides):
        payload = {
            "source": "github",
            "source_event_id": "1",
            "source_event_endpoint": "default",
            "source_event_type": "PushEvent",
            "observed_at": "2026-08-01T12:00:00",
            "schema_version": 1,
            "payload": {"id": "1"},
        }
        payload.update(overrides)
        return payload

    def test_valid_payload_creates_instance(self):
        raw_event = RawEvent(**self._valid_payload())

        self.assertEqual(raw_event.source, "github")
        self.assertEqual(raw_event.source_event_id, "1")
        self.assertEqual(raw_event.source_event_endpoint, "default")
        self.assertEqual(raw_event.source_event_type, "PushEvent")
        self.assertEqual(raw_event.schema_version, 1)
        self.assertEqual(raw_event.payload, {"id": "1"})

    def test_missing_source_event_endpoint_raises_validation_error(self):
        payload = self._valid_payload()
        del payload["source_event_endpoint"]

        with self.assertRaises(ValidationError):
            RawEvent(**payload)

    def test_missing_payload_raises_validation_error(self):
        payload = self._valid_payload()
        del payload["payload"]

        with self.assertRaises(ValidationError):
            RawEvent(**payload)

    def test_non_dict_payload_raises_validation_error(self):
        with self.assertRaises(ValidationError):
            RawEvent(**self._valid_payload(payload="not-a-dict"))

    def test_model_dump_is_json_serializable(self):
        raw_event = RawEvent(**self._valid_payload())

        dumped = raw_event.model_dump(mode="json")

        self.assertEqual(dumped["source"], "github")
        self.assertEqual(dumped["source_event_endpoint"], "default")


if __name__ == "__main__":
    unittest.main()
