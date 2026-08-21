from unittest import TestCase

from flink.normalization.models import FieldRule, NormalizationContract


class TestFieldRule(TestCase):

    def setUp(self):
        self.base_value = {"from": "actor.id"}

    def test_can_initiate_with_success(self):
        field_rule = FieldRule(**self.base_value)

        self.assertEqual(field_rule.from_, "actor.id")
        self.assertEqual(field_rule.take, None)
        self.assertEqual(field_rule.as_, None)
        self.assertEqual(field_rule.default, None)
        self.assertEqual(field_rule.expression, None)

    def test_can_not_initiate_with_wrong_parameters(self):
        wrong_value = self.base_value
        wrong_value["as"] = "bool"
        with self.assertRaises(ValueError) as context:
            FieldRule(**wrong_value)

        self.assertIn(
            "Input should be 'boolean' or 'timestamp'", str(context.exception)
        )

    def test_can_check_from_is_dotted_with_success(self):
        complex_value = {"from": "metadata.actor.id"}
        field_rule = FieldRule(**complex_value)

        self.assertEqual(field_rule.from_, "metadata.actor.id")
        self.assertEqual(field_rule.take, None)
        self.assertEqual(field_rule.as_, None)
        self.assertEqual(field_rule.default, None)
        self.assertEqual(field_rule.expression, None)

    def test_can_raise_if_from_is_not_dotted(self):
        wrong_complex_value = {"from": "metadata.actor.id."}
        with self.assertRaises(ValueError) as context:
            FieldRule(**wrong_complex_value)

        self.assertIn(
            "Value error, 'from_' must be dot-separated", str(context.exception)
        )

    def test_can_raise_if_not_from_or_expression(self):
        with self.assertRaises(ValueError) as context:
            FieldRule()

        self.assertIn(
            "Value error, FieldRule must define either 'from_' or 'expression'",
            str(context.exception),
        )

    def test_can_raise_if_both_from_and_expression(self):
        wrong_value = self.base_value
        wrong_value["expression"] = "payload.issue.pull_request != null"

        with self.assertRaises(ValueError) as context:
            FieldRule(**wrong_value)

        self.assertIn(
            "Value error, FieldRule must define either 'from_' or 'expression'",
            str(context.exception),
        )


class TestNormalizationContract(TestCase):

    def setUp(self):
        self.valid_contract = {
            "source": "github",
            "partition_key": {"from": "repo.name"},
            "envelope": {
                "entity_id": {"from": "actor.id"},
                "event_time": {"from": "created_at", "as": "timestamp"},
            },
            "common": {
                "repo_id": {"from": "repo.id"},
                "org_login": {"from": "org.login", "default": None},
            },
            "event_types": {
                "WatchEvent": {"action": {"from": "payload.action"}},
                "IssueCommentEvent": {
                    "issue_labels": {"from": "payload.issue.labels", "take": "name"},
                    "issue_is_pull_request": {
                        "expression": "payload.issue.pull_request != null",
                        "as": "boolean",
                    },
                },
            },
        }

    def test_can_initiate_with_success(self):
        contract = NormalizationContract.model_validate(self.valid_contract)

        self.assertEqual(contract.source, "github")
        self.assertIsInstance(contract.partition_key, FieldRule)
        self.assertEqual(contract.partition_key.from_, "repo.name")
        self.assertIsInstance(contract.envelope["entity_id"], FieldRule)
        self.assertEqual(contract.envelope["event_time"].as_, "timestamp")
        self.assertEqual(contract.common["org_login"].default, None)
        self.assertIsInstance(
            contract.event_types["IssueCommentEvent"]["issue_labels"], FieldRule
        )
        self.assertEqual(
            contract.event_types["IssueCommentEvent"]["issue_labels"].take, "name"
        )

    def test_can_not_initiate_without_required_fields(self):
        with self.assertRaises(ValueError) as context:
            NormalizationContract()

        self.assertIn("source", str(context.exception))
        self.assertIn("Field required", str(context.exception))

    def test_can_raise_if_nested_field_rule_is_invalid(self):
        invalid_contract = self.valid_contract
        invalid_contract["envelope"]["entity_name"] = {}

        with self.assertRaises(ValueError) as context:
            NormalizationContract.model_validate(invalid_contract)

        self.assertIn(
            "Value error, FieldRule must define either 'from_' or 'expression'",
            str(context.exception),
        )

    def test_can_raise_if_nested_field_rule_has_unknown_key(self):
        invalid_contract = self.valid_contract
        invalid_contract["common"]["repo_id"]["frm"] = "repo.id"

        with self.assertRaises(ValueError) as context:
            NormalizationContract.model_validate(invalid_contract)

        self.assertIn("Extra inputs are not permitted", str(context.exception))
