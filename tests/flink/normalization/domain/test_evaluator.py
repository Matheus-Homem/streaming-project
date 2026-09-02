from unittest import TestCase

import jmespath

from flink.normalization.domain.evaluator import (
    NormalizationFunctions,
    NormalizationRulesEventEvaluator,
)
from flink.normalization.models import FieldRule, NormalizationContract
from shared.models import RawEvent
from tests.fixtures.events import GITHUB_EVENT


class TestNormalizationFunctionsIsoToMillis(TestCase):

    def setUp(self):
        self.functions = NormalizationFunctions()

    def test_can_convert_z_suffixed_timestamp(self):
        result = self.functions._func_iso_to_millis("2026-07-17T12:21:32Z")

        self.assertEqual(result, 1784290892000)

    def test_can_convert_offset_naive_timestamp(self):
        result = self.functions._func_iso_to_millis("2026-07-17T12:21:32")

        self.assertEqual(result, 1784290892000)

    def test_can_convert_explicit_utc_offset_timestamp(self):
        result = self.functions._func_iso_to_millis("2026-07-17T12:21:32+00:00")

        self.assertEqual(result, 1784290892000)

    def test_can_convert_timestamp_with_microseconds(self):
        result = self.functions._func_iso_to_millis("2026-07-17T12:21:32.123456")

        self.assertEqual(result, 1784290892123)

    def test_returns_none_for_none_input(self):
        result = self.functions._func_iso_to_millis(None)

        self.assertIsNone(result)

    def test_returns_none_for_empty_string_input(self):
        result = self.functions._func_iso_to_millis("")

        self.assertIsNone(result)


OPTIONS = jmespath.Options(custom_functions=NormalizationFunctions())


class TestIsoToMillisThroughJmespath(TestCase):

    def test_can_search_expression_using_custom_function(self):
        expr = jmespath.compile("iso_to_millis(created_at)")

        result = expr.search({"created_at": "2026-07-17T12:21:32Z"}, options=OPTIONS)

        self.assertEqual(result, 1784290892000)

    def test_can_search_expression_with_missing_field(self):
        expr = jmespath.compile("iso_to_millis(created_at)")

        result = expr.search({}, options=OPTIONS)

        self.assertIsNone(result)

    def test_raises_without_options_wired_in(self):
        expr = jmespath.compile("iso_to_millis(created_at)")

        with self.assertRaises(jmespath.exceptions.UnknownFunctionError):
            expr.search({"created_at": "2026-07-17T12:21:32Z"})


class TestNormalizationRulesEventEvaluatorCompileRule(TestCase):

    def setUp(self):
        self.event = GITHUB_EVENT
        self.evaluator = NormalizationRulesEventEvaluator()

    def test_can_compile_plain_from(self):
        rule = FieldRule(**{"from": "actor.id", "type": "BIGINT"})

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(expr, "actor.id")
        self.assertEqual(
            self.evaluator._assess_payload_with_rule(rule, self.event),
            181008794,
        )

    def test_can_compile_take_as_list_pluck(self):
        rule = FieldRule(
            **{"from": "payload.issue.labels", "take": "name", "type": "ARRAY<STRING>"}
        )

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(expr, "payload.issue.labels[].name")
        result = self.evaluator._assess_payload_with_rule(rule, self.event)
        self.assertIn("area/test", result)

    def test_can_compile_presence(self):
        rule = FieldRule(**{"from": "payload.issue.pull_request", "type": "PRESENCE"})

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(expr, "payload.issue.pull_request != `null`")
        self.assertTrue(self.evaluator._assess_payload_with_rule(rule, self.event))

    def test_can_compile_boolean_passthrough(self):
        rule = FieldRule(**{"from": "public", "type": "BOOLEAN"})

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(expr, "public")
        self.assertTrue(self.evaluator._assess_payload_with_rule(rule, self.event))

    def test_can_compile_timestamp(self):
        rule = FieldRule(**{"from": "created_at", "type": "TIMESTAMP"})

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(expr, "iso_to_millis(created_at)")
        self.assertEqual(
            self.evaluator._assess_payload_with_rule(rule, self.event),
            1784290892000,
        )

    def test_can_compile_default_null_explicitly_declared(self):
        rule = FieldRule(
            **{"from": "does.not.exist", "type": "STRING", "default": None}
        )

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(expr, "does.not.exist || `null`")
        self.assertIsNone(self.evaluator._assess_payload_with_rule(rule, self.event))

    def test_can_compile_with_no_default_declared_at_all(self):
        rule = FieldRule(**{"from": "does.not.exist", "type": "STRING"})

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(expr, "does.not.exist")
        self.assertIsNone(self.evaluator._assess_payload_with_rule(rule, self.event))

    def test_can_compile_default_with_non_null_fallback(self):
        rule = FieldRule(
            **{"from": "org.login", "type": "STRING", "default": "unknown"}
        )

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(expr, 'org.login || `"unknown"`')
        self.assertEqual(
            self.evaluator._assess_payload_with_rule(rule, self.event),
            "kubernetes",
        )

    def test_can_compile_default_applied_when_field_is_missing(self):
        rule = FieldRule(
            **{"from": "does.not.exist", "type": "STRING", "default": "fallback"}
        )

        self.evaluator._compile_rule(rule)

        self.assertEqual(
            self.evaluator._assess_payload_with_rule(rule, self.event),
            "fallback",
        )


class TestNormalizationRulesEventEvaluatorApply(TestCase):

    def setUp(self):
        self.evaluator = NormalizationRulesEventEvaluator()
        self.contract = NormalizationContract(
            source="widget",
            partition_key=FieldRule(**{"from": "id", "type": "STRING"}),
            envelope={"entity_id": FieldRule(**{"from": "actor.id", "type": "BIGINT"})},
            common={"widget_type": FieldRule(**{"from": "type", "type": "STRING"})},
            event_types={
                "CreatedEvent": {
                    "created_by": FieldRule(**{"from": "by", "type": "STRING"})
                },
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
        result = self.evaluator.apply(event=self.event, contract=self.contract)

        self.assertEqual(result["partition_key"], "w-1")

    def test_can_evaluate_envelope_and_common_rules_against_the_payload(self):
        result = self.evaluator.apply(event=self.event, contract=self.contract)

        self.assertEqual(result["entity_id"], 7)
        self.assertEqual(result["widget_type"], "gadget")

    def test_can_evaluate_event_type_specific_rules(self):
        result = self.evaluator.apply(event=self.event, contract=self.contract)

        self.assertEqual(result["created_by"], "alice")

    def test_can_return_null_field_when_path_is_absent(self):
        contract = self.contract.model_copy(deep=True)
        contract.common["missing"] = FieldRule(
            **{"from": "does.not.exist", "type": "STRING"}
        )

        result = self.evaluator.apply(event=self.event, contract=contract)

        self.assertIsNone(result["missing"])

    def test_can_normalize_event_of_undeclared_type_without_raising(self):
        undeclared_event = self.event.model_copy(
            update={"source_event_type": "UnmappedEvent"}
        )

        result = self.evaluator.apply(event=undeclared_event, contract=self.contract)

        self.assertEqual(result["partition_key"], "w-1")
        self.assertEqual(result["entity_id"], 7)
        self.assertEqual(result["widget_type"], "gadget")
        self.assertNotIn("created_by", result)

    def test_module_contains_no_github_specific_vocabulary(self):
        import inspect

        from flink.normalization.domain import evaluator as evaluator_module

        source = inspect.getsource(evaluator_module)

        for github_term in ("github", "repo_name", "org_login", "issue"):
            self.assertNotIn(github_term, source.lower())
