from unittest import TestCase

import jmespath

from flink.normalization.adapters.evaluator import (
    NormalizationFunctions,
    NormalizationJmespathEvaluator,
)
from flink.normalization.models import FieldRule
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


class TestNormalizationJmespathEvaluatorToMillis(TestCase):
    def setUp(self):
        self.evaluator = NormalizationJmespathEvaluator()

    def test_can_convert_explicit_utc_offset_timestamp(self):
        result = self.evaluator.to_millis("2026-07-17T12:21:32+00:00")

        self.assertEqual(result, 1784290892000)

    def test_can_assume_utc_when_timestamp_carries_no_offset(self):
        result = self.evaluator.to_millis("2026-07-17T12:21:32")

        self.assertEqual(result, 1784290892000)

    def test_returns_none_for_absent_timestamp(self):
        self.assertIsNone(self.evaluator.to_millis(None))
        self.assertIsNone(self.evaluator.to_millis(""))

    def test_agrees_with_the_jmespath_function_it_exposes(self):
        for iso_string in (
            "2026-07-17T12:21:32Z",
            "2026-07-17T12:21:32",
            "2026-07-17T12:21:32.123456",
            None,
        ):
            with self.subTest(iso_string=iso_string):
                self.assertEqual(
                    self.evaluator.to_millis(iso_string),
                    NormalizationFunctions()._func_iso_to_millis(iso_string),
                )


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


class TestNormalizationJmespathEvaluator(TestCase):

    def setUp(self):
        self.event = GITHUB_EVENT
        self.evaluator = NormalizationJmespathEvaluator()

    def test_can_compile_plain_from(self):
        rule = FieldRule(**{"from": "actor.id"})

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(expr, "actor.id")
        self.assertEqual(
            self.evaluator.evaluate(rule, self.event),
            181008794,
        )

    def test_can_compile_take_as_list_pluck(self):
        rule = FieldRule(**{"from": "payload.issue.labels", "take": "name"})

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(expr, "payload.issue.labels[].name")
        result = self.evaluator.evaluate(rule, self.event)
        self.assertIn("area/test", result)

    def test_can_compile_as_boolean(self):
        rule = FieldRule(**{"from": "payload.issue.pull_request", "as": "boolean"})

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(expr, "payload.issue.pull_request != `null`")
        self.assertTrue(self.evaluator.evaluate(rule, self.event))

    def test_can_compile_as_timestamp(self):
        rule = FieldRule(**{"from": "created_at", "as": "timestamp"})

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(expr, "iso_to_millis(created_at)")
        self.assertEqual(
            self.evaluator.evaluate(rule, self.event),
            1784290892000,
        )

    def test_can_compile_default_null_as_native_missing(self):
        rule = FieldRule(**{"from": "does.not.exist", "default": None})

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(expr, "does.not.exist")
        self.assertIsNone(self.evaluator.evaluate(rule, self.event))

    def test_can_compile_default_with_non_null_fallback(self):
        rule = FieldRule(**{"from": "org.login", "default": "unknown"})

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(expr, 'org.login || `"unknown"`')
        self.assertEqual(
            self.evaluator.evaluate(rule, self.event),
            "kubernetes",
        )

    def test_can_compile_default_applied_when_field_is_missing(self):
        rule = FieldRule(**{"from": "does.not.exist", "default": "fallback"})

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(
            self.evaluator.evaluate(rule, self.event),
            "fallback",
        )

    def test_can_compile_expression_passed_through_verbatim(self):
        raw = "payload.issue.pull_request != `null`"
        rule = FieldRule(expression=raw)

        expr = self.evaluator._compile_rule(rule)

        self.assertEqual(expr, raw)
        self.assertTrue(self.evaluator.evaluate(rule, self.event))
