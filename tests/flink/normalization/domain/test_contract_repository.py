import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import yaml

from flink.normalization.domain.contract_repository import YamlContractRepository
from flink.normalization.models import FieldRule, NormalizationContract

VALID_CONTRACT = {
    "source": "widget",
    "partition_key": {"from": "id", "type": "STRING"},
    "envelope": {
        "entity_id": {"from": "actor.id", "type": "BIGINT"},
    },
    "common": {},
    "event_types": {
        "CreatedEvent": {"created_by": {"from": "by", "type": "STRING"}},
    },
}


class TestYamlContractRepository(TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp_dir.cleanup)
        contracts_dir = Path(self.tmp_dir.name)

        widget_dir = contracts_dir / "widget"
        widget_dir.mkdir()
        with (widget_dir / "normalization.yml").open("w") as file:
            yaml.safe_dump(VALID_CONTRACT, file)

        self.repository = YamlContractRepository(contracts_dir=contracts_dir)

    def test_can_load_and_validate_a_contract_file(self):
        contract = self.repository.get("widget")

        self.assertIsInstance(contract, NormalizationContract)
        self.assertEqual(contract.source, "widget")
        self.assertIsInstance(contract.partition_key, FieldRule)
        self.assertIn("CreatedEvent", contract.event_types)

    def test_can_raise_not_implemented_for_unknown_source(self):
        with self.assertRaises(NotImplementedError) as context:
            self.repository.get("some_source_without_a_contract_file")

        self.assertIn(
            "not implemented in Normalization pipeline", str(context.exception)
        )

    def test_can_raise_if_contract_file_is_invalid(self):
        broken_contract = {"source": "widget"}

        with patch(
            "flink.normalization.domain.contract_repository.yaml.safe_load",
            return_value=broken_contract,
        ):
            with self.assertRaises(ValueError) as context:
                self.repository.get("widget")

        self.assertIn("Field required", str(context.exception))

    def test_can_cache_result_for_repeated_calls_with_the_same_source(self):
        first_call = self.repository.get("widget")
        with patch(
            "flink.normalization.domain.contract_repository.yaml.safe_load"
        ) as mock_safe_load:
            second_call = self.repository.get("widget")

        mock_safe_load.assert_not_called()
        self.assertIs(first_call, second_call)
