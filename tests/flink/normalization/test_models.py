from unittest import TestCase

from flink.normalization.models import FieldRule, NormalizationContract


class TestFieldRule(TestCase):

    def setUp(self):
        self.base_value = {"from": "actor.id", "type": "STRING"}

    def test_can_initiate_with_success(self):
        field_rule = FieldRule(**self.base_value)

        self.assertEqual(field_rule.from_, "actor.id")
        self.assertEqual(field_rule.take, None)
        self.assertEqual(field_rule.type_, "STRING")
        self.assertEqual(field_rule.default, None)

    def test_can_initiate_with_array_type(self):
        array_value = {**self.base_value, "type": "ARRAY<STRING>"}
        field_rule = FieldRule(**array_value)

        self.assertEqual(field_rule.type_, "ARRAY<STRING>")

    def test_can_check_from_is_dotted_with_success(self):
        complex_value = {"from": "metadata.actor.id", "type": "STRING"}
        field_rule = FieldRule(**complex_value)

        self.assertEqual(field_rule.from_, "metadata.actor.id")
        self.assertEqual(field_rule.take, None)
        self.assertEqual(field_rule.type_, "STRING")
        self.assertEqual(field_rule.default, None)

    def test_can_raise_if_from_is_not_dotted(self):
        wrong_complex_value = {"from": "metadata.actor.id.", "type": "STRING"}
        with self.assertRaises(ValueError) as context:
            FieldRule(**wrong_complex_value)

        self.assertIn(
            "Value error, 'from_' must be dot-separated", str(context.exception)
        )

    def test_can_raise_if_from_is_missing(self):
        with self.assertRaises(ValueError) as context:
            FieldRule(type="STRING")

        self.assertIn("from", str(context.exception))
        self.assertIn("Field required", str(context.exception))

    def test_can_raise_if_type_is_missing(self):
        with self.assertRaises(ValueError) as context:
            FieldRule(**{"from": "actor.id"})

        self.assertIn("type", str(context.exception))
        self.assertIn("Field required", str(context.exception))

    def test_can_raise_if_type_is_invalid_token(self):
        wrong_value = {**self.base_value, "type": "INTEGER"}
        with self.assertRaises(ValueError) as context:
            FieldRule(**wrong_value)

        self.assertIn("Value error, 'type_' must be", str(context.exception))
        self.assertIn("INTEGER", str(context.exception))

    def test_can_raise_if_contract_declares_as_key(self):
        wrong_value = {**self.base_value, "as": "boolean"}
        with self.assertRaises(ValueError) as context:
            FieldRule(**wrong_value)

        self.assertIn("Extra inputs are not permitted", str(context.exception))

    def test_can_raise_if_contract_declares_expression_key(self):
        wrong_value = {
            **self.base_value,
            "expression": "payload.issue.pull_request != null",
        }
        with self.assertRaises(ValueError) as context:
            FieldRule(**wrong_value)

        self.assertIn("Extra inputs are not permitted", str(context.exception))

    def test_model_fields_set_excludes_absent_default(self):
        field_rule = FieldRule(**self.base_value)

        self.assertNotIn("default", field_rule.model_fields_set)

    def test_model_fields_set_includes_explicitly_declared_null_default(self):
        field_rule = FieldRule(**{**self.base_value, "default": None})

        self.assertIn("default", field_rule.model_fields_set)


class TestNormalizationContract(TestCase):

    def setUp(self):
        self.valid_contract = {
            "source": "github",
            "partition_key": {"from": "repo.name", "type": "STRING"},
            "envelope": {
                "entity_id": {"from": "actor.id", "type": "STRING"},
                "event_time": {"from": "created_at", "type": "TIMESTAMP"},
            },
            "common": {
                "repo_id": {"from": "repo.id", "type": "BIGINT"},
                "org_login": {"from": "org.login", "type": "STRING", "default": None},
            },
            "event_types": {
                "WatchEvent": {"action": {"from": "payload.action", "type": "STRING"}},
                "IssueCommentEvent": {
                    "issue_labels": {
                        "from": "payload.issue.labels",
                        "take": "name",
                        "type": "ARRAY<STRING>",
                    },
                    "issue_is_pull_request": {
                        "from": "payload.issue.pull_request",
                        "type": "PRESENCE",
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
        self.assertEqual(contract.envelope["event_time"].type_, "TIMESTAMP")
        self.assertEqual(contract.common["org_login"].default, None)
        self.assertIsInstance(
            contract.event_types["IssueCommentEvent"]["issue_labels"], FieldRule
        )
        self.assertEqual(
            contract.event_types["IssueCommentEvent"]["issue_labels"].take, "name"
        )
        self.assertEqual(
            contract.event_types["IssueCommentEvent"]["issue_is_pull_request"].type_,
            "PRESENCE",
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

        self.assertIn("envelope.entity_name.from", str(context.exception))
        self.assertIn("envelope.entity_name.type", str(context.exception))
        self.assertIn("Field required", str(context.exception))

    def test_can_raise_if_nested_field_rule_has_unknown_key(self):
        invalid_contract = self.valid_contract
        invalid_contract["common"]["repo_id"]["frm"] = "repo.id"

        with self.assertRaises(ValueError) as context:
            NormalizationContract.model_validate(invalid_contract)

        self.assertIn("Extra inputs are not permitted", str(context.exception))
