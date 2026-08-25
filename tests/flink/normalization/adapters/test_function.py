from unittest import TestCase
from unittest.mock import Mock

from pyflink.common import Row

from flink.normalization.adapters.function import NormalizationFlatMapFunction
from flink.normalization.domain.normalizer import EventNormalizer
from shared.models import RawEvent

VALID_RAW_EVENT_JSON = RawEvent(
    source="widget",
    source_event_id="1",
    source_event_endpoint="/widgets",
    source_event_type="CreatedEvent",
    observed_at="2026-07-17T12:21:32Z",
    schema_version=1,
    payload={"id": "w-1"},
).model_dump_json()


class TestNormalizationFlatMapFunctionFlatMap(TestCase):

    def setUp(self):
        self.normalizer = Mock(spec=EventNormalizer)
        self.function = NormalizationFlatMapFunction(normalizer=self.normalizer)

    def test_can_yield_the_normalized_event_as_a_key_value_row(self):
        normalized_event = Mock()
        normalized_event.partition_key = "my-org/my-repo"
        normalized_event.model_dump_json.return_value = '{"source": "widget"}'
        self.normalizer.normalize.return_value = normalized_event

        result = list(self.function.flat_map(VALID_RAW_EVENT_JSON))

        self.assertEqual(result, [Row(b"my-org/my-repo", b'{"source": "widget"}')])

    def test_can_discard_malformed_json_without_calling_the_normalizer(self):
        result = list(self.function.flat_map("not valid json"))

        self.assertEqual(result, [])
        self.normalizer.normalize.assert_not_called()

    def test_can_discard_a_raw_event_missing_a_required_field(self):
        result = list(self.function.flat_map('{"source": "widget"}'))

        self.assertEqual(result, [])
        self.normalizer.normalize.assert_not_called()

    def test_can_discard_an_event_from_an_unknown_source(self):
        self.normalizer.normalize.side_effect = NotImplementedError("unknown source")

        result = list(self.function.flat_map(VALID_RAW_EVENT_JSON))

        self.assertEqual(result, [])

    def test_can_discard_an_event_on_unexpected_normalization_failure(self):
        self.normalizer.normalize.side_effect = RuntimeError("boom")

        result = list(self.function.flat_map(VALID_RAW_EVENT_JSON))

        self.assertEqual(result, [])

    def test_open_is_a_no_op(self):
        self.function.open(Mock())
