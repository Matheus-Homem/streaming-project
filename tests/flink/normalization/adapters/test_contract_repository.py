from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import flink.normalization
from flink.normalization.adapters.contract_repository import YamlContractRepository
from flink.normalization.models import FieldRule, NormalizationContract

CONTRACTS_DIR = (
    Path(flink.normalization.__file__).parent.parent.parent / "interface" / "sources"
)


class TestYamlContractRepository(TestCase):

    def setUp(self):
        self.repository = YamlContractRepository(contracts_dir=CONTRACTS_DIR)

    def test_can_load_and_validate_the_real_github_contract(self):
        contract = self.repository.get("github")

        self.assertIsInstance(contract, NormalizationContract)
        self.assertEqual(contract.source, "github")
        self.assertIsInstance(contract.partition_key, FieldRule)
        self.assertIn("IssueCommentEvent", contract.event_types)

    def test_can_raise_not_implemented_for_unknown_source(self):
        with self.assertRaises(NotImplementedError) as context:
            self.repository.get("some_source_without_a_contract_file")

        self.assertIn(
            "not implemented in Normalization pipeline", str(context.exception)
        )

    def test_can_raise_if_contract_file_is_invalid(self):
        broken_contract = {"source": "github"}

        with patch(
            "flink.normalization.adapters.contract_repository.yaml.safe_load",
            return_value=broken_contract,
        ):
            with self.assertRaises(ValueError) as context:
                self.repository.get("github")

        self.assertIn("Field required", str(context.exception))

    def test_can_cache_result_for_repeated_calls_with_the_same_source(self):
        first_call = self.repository.get("github")
        with patch(
            "flink.normalization.adapters.contract_repository.yaml.safe_load"
        ) as mock_safe_load:
            second_call = self.repository.get("github")

        mock_safe_load.assert_not_called()
        self.assertIs(first_call, second_call)
