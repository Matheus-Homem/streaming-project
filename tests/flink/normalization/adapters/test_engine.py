from unittest import TestCase
from unittest.mock import Mock, patch

from flink.normalization.adapters.engine import NormalizationEngine
from flink.normalization.models import FieldRule, NormalizationContract
from flink.normalization.ports import NormalizationEvaluatorBase
from shared.models import RawEvent


class TestNormalizationEngine(TestCase):

    def setUp(self):
        self.evaluator = Mock(spec=NormalizationEvaluatorBase)
        self.engine = NormalizationEngine(evaluator=self.evaluator)
        self.contract = NormalizationContract(
            source="widget",
            partition_key=FieldRule(**{"from": "id"}),
            envelope={"actor_id": FieldRule(**{"from": "actor.id"})},
            common={"widget_type": FieldRule(**{"from": "type"})},
            event_types={
                "CreatedEvent": {"created_by": FieldRule(**{"from": "payload.by"})},
            },
        )
        self.event = RawEvent(
            source="widget",
            source_event_id="1",
            source_event_endpoint="/widgets",
            source_event_type="CreatedEvent",
            observed_at="2026-01-01T00:00:00+00:00",
            schema_version=1,
            payload={"id": "w-1", "actor": {"id": 7}, "type": "gadget", "by": "alice"},
        )

    def test_can_populate_partition_key_from_contract_rule(self):
        self.evaluator.evaluate.side_effect = lambda rule, payload: "resolved"

        with patch(
            "flink.normalization.adapters.engine.get_contract",
            return_value=self.contract,
        ):
            result = self.engine.normalize(self.event)

        self.assertIn("partition_key", result)
        evaluated_rules = [c.args[0] for c in self.evaluator.evaluate.call_args_list]
        self.assertIn(self.contract.partition_key, evaluated_rules)

    def test_can_evaluate_every_rule_against_event_payload(self):
        self.evaluator.evaluate.side_effect = lambda rule, payload: "resolved"

        with patch(
            "flink.normalization.adapters.engine.get_contract",
            return_value=self.contract,
        ):
            self.engine.normalize(self.event)

        for call in self.evaluator.evaluate.call_args_list:
            self.assertEqual(call.args[1], self.event.payload)

    def test_can_return_null_field_when_evaluator_resolves_nothing(self):
        self.evaluator.evaluate.return_value = None

        with patch(
            "flink.normalization.adapters.engine.get_contract",
            return_value=self.contract,
        ):
            result = self.engine.normalize(self.event)

        self.assertIsNone(result["actor_id"])

    def test_can_normalize_event_of_undeclared_type_without_raising(self):
        undeclared_event = RawEvent(
            source="widget",
            source_event_id="2",
            source_event_endpoint="/widgets",
            source_event_type="UnmappedEvent",
            observed_at="2026-01-01T00:00:00+00:00",
            schema_version=1,
            payload={"id": "w-2", "actor": {"id": 8}, "type": "gizmo"},
        )
        self.evaluator.evaluate.side_effect = lambda rule, payload: "resolved"

        with patch(
            "flink.normalization.adapters.engine.get_contract",
            return_value=self.contract,
        ):
            result = self.engine.normalize(undeclared_event)

        self.assertEqual(result["partition_key"], "resolved")
        self.assertEqual(result["actor_id"], "resolved")
        self.assertEqual(result["widget_type"], "resolved")
        self.assertNotIn("created_by", result)

    def test_can_evaluate_only_envelope_and_common_rules_for_undeclared_type(self):
        undeclared_event = RawEvent(
            source="widget",
            source_event_id="2",
            source_event_endpoint="/widgets",
            source_event_type="UnmappedEvent",
            observed_at="2026-01-01T00:00:00+00:00",
            schema_version=1,
            payload={"id": "w-2", "actor": {"id": 8}, "type": "gizmo"},
        )
        self.evaluator.evaluate.side_effect = lambda rule, payload: "resolved"

        with patch(
            "flink.normalization.adapters.engine.get_contract",
            return_value=self.contract,
        ):
            self.engine.normalize(undeclared_event)

        evaluated_rules = [c.args[0] for c in self.evaluator.evaluate.call_args_list]
        self.assertIn(self.contract.partition_key, evaluated_rules)
        self.assertIn(self.contract.envelope["actor_id"], evaluated_rules)
        self.assertIn(self.contract.common["widget_type"], evaluated_rules)
        self.assertNotIn(
            self.contract.event_types["CreatedEvent"]["created_by"], evaluated_rules
        )

    def test_can_build_envelope_fields_named_exactly_as_the_spec_requires(self):
        self.evaluator.evaluate.side_effect = lambda rule, payload: "resolved"

        with patch(
            "flink.normalization.adapters.engine.get_contract",
            return_value=self.contract,
        ):
            result = self.engine.normalize(self.event)

        self.assertEqual(result["source"], "widget")
        self.assertEqual(result["event_id"], "1")
        self.assertEqual(result["event_type"], "CreatedEvent")
        for raw_only_field in (
            "source_event_id",
            "source_event_type",
            "source_event_endpoint",
            "observed_at",
            "payload",
        ):
            self.assertNotIn(raw_only_field, result)

    def test_can_delegate_ingested_at_conversion_to_the_evaluator(self):
        self.evaluator.evaluate.side_effect = lambda rule, payload: "resolved"
        self.evaluator.to_millis.return_value = 1767225600000

        with patch(
            "flink.normalization.adapters.engine.get_contract",
            return_value=self.contract,
        ):
            result = self.engine.normalize(self.event)

        self.evaluator.to_millis.assert_called_once_with("2026-01-01T00:00:00+00:00")
        self.assertEqual(result["ingested_at"], 1767225600000)

    def test_can_emit_its_own_schema_version_independent_of_the_raw_event(self):
        rebranded_event = self.event.model_copy(update={"schema_version": 99})
        self.evaluator.evaluate.side_effect = lambda rule, payload: "resolved"

        with patch(
            "flink.normalization.adapters.engine.get_contract",
            return_value=self.contract,
        ):
            result = self.engine.normalize(rebranded_event)

        self.assertEqual(result["schema_version"], 1)

    def test_module_contains_no_github_specific_vocabulary(self):
        import inspect

        from flink.normalization.adapters import engine as engine_module

        source = inspect.getsource(engine_module)

        for github_term in ("github", "repo_name", "org_login", "issue"):
            self.assertNotIn(github_term, source.lower())
