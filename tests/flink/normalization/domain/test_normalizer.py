from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import Mock

from flink.normalization.domain import ContractRepository, EventEvaluator
from flink.normalization.domain.normalizer import EventNormalizer
from flink.normalization.models import FieldRule, NormalizationContract
from shared.models import RawEvent


class TestEventNormalizerNormalize(TestCase):

    def setUp(self):
        self.event_evaluator = Mock(spec=EventEvaluator)
        self.contract_repository = Mock(spec=ContractRepository)
        self.normalizer = EventNormalizer(
            event_evaluator=self.event_evaluator,
            contract_repository=self.contract_repository,
        )
        self.event = RawEvent(
            source="widget",
            source_event_id="1",
            source_event_endpoint="/widgets",
            source_event_type="CreatedEvent",
            observed_at="2026-07-17T12:21:32Z",
            schema_version=1,
            payload={"id": "w-1", "actor": {"id": 7}, "type": "gadget"},
        )
        self.contract = NormalizationContract(
            source="widget",
            partition_key=FieldRule(**{"from": "id", "type": "STRING"}),
            envelope={},
            common={},
            event_types={},
        )
        self.contract_repository.get.return_value = self.contract
        self.event_evaluator.apply.return_value = {
            "partition_key": "w-1",
            "entity_id": 7,
            "entity_name": "alice",
            "event_time": 1784290892000,
            "widget_type": "gadget",
        }

    def test_can_resolve_the_contract_for_the_event_source(self):
        self.normalizer.normalize(self.event)

        self.contract_repository.get.assert_called_once_with(self.event.source)

    def test_can_delegate_field_resolution_to_the_injected_evaluator(self):
        self.normalizer.normalize(self.event)

        self.event_evaluator.apply.assert_called_once()
        call_kwargs = self.event_evaluator.apply.call_args.kwargs
        self.assertIs(call_kwargs["event"], self.event)
        self.assertIs(call_kwargs["contract"], self.contract)

    def test_can_build_envelope_identity_from_the_raw_event(self):
        result = self.normalizer.normalize(self.event)

        self.assertEqual(result.source, "widget")
        self.assertEqual(result.event_id, "1")
        self.assertEqual(result.event_type, "CreatedEvent")
        self.assertEqual(result.schema_version, 1)

    def test_can_emit_its_own_schema_version_independent_of_the_raw_event(self):
        rebranded_event = self.event.model_copy(update={"schema_version": 99})

        result = self.normalizer.normalize(rebranded_event)

        self.assertEqual(result.schema_version, 1)

    def test_can_pull_partition_key_entity_and_event_time_from_the_evaluated_data(self):
        result = self.normalizer.normalize(self.event)

        self.assertEqual(result.partition_key, "w-1")
        self.assertEqual(result.entity_id, "7")
        self.assertEqual(result.entity_name, "alice")
        self.assertEqual(result.event_time, 1784290892000)

    def test_can_convert_ingested_at_from_observed_at(self):
        result = self.normalizer.normalize(self.event)

        self.assertEqual(
            result.ingested_at, datetime(2026, 7, 17, 12, 21, 32, tzinfo=timezone.utc)
        )

    def test_can_merge_extra_evaluator_fields_without_the_reserved_keys(self):
        result = self.normalizer.normalize(self.event)

        self.assertEqual(result.widget_type, "gadget")
        dumped = result.model_dump()
        self.assertEqual(
            {
                "source",
                "event_id",
                "event_type",
                "ingested_at",
                "schema_version",
                "partition_key",
                "entity_id",
                "entity_name",
                "event_time",
                "widget_type",
            },
            set(dumped),
        )
