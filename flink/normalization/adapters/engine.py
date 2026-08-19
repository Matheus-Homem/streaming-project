from itertools import chain
from logging import getLogger
from typing import Any, Union

from flink.normalization.models import FieldRule, NormalizationContract, get_contract
from flink.normalization.ports import (
    NormalizationEngineBase,
    NormalizationEvaluatorBase,
)
from shared.models import RawEvent

NORMALIZED_SCHEMA_VERSION = 1


class NormalizationEngine(NormalizationEngineBase):
    def __init__(self, evaluator: NormalizationEvaluatorBase):
        self.logger = getLogger(self.__class__.__name__)
        self.evaluator = evaluator

    def _collect_field_rules(
        self, contract: NormalizationContract, event_type: str
    ) -> chain[tuple[str, FieldRule]]:
        try:
            return chain(
                [("partition_key", contract.partition_key)],
                contract.envelope.items(),
                contract.common.items(),
                contract.event_types[event_type].items(),
            )
        except KeyError as e:
            self.logger.warning(
                f"Error while trying to collect field rules from event type: {str(e)}"
            )
            return chain(
                [("partition_key", contract.partition_key)],
                contract.envelope.items(),
                contract.common.items(),
            )
        except Exception:
            self.logger.exception(
                f"Failed to assemble field-rule pairs for contract source={contract.source}"
            )
            raise

    def _resolve_fields(
        self,
        payload: dict[str, Any],
        contract_rules: chain[tuple[str, FieldRule]],
    ) -> dict[str, Any]:
        return {
            name: self.evaluator.evaluate(rule, payload)
            for name, rule in contract_rules
        }

    def _clean_event_dictionary(
        self, event_dict: dict[str, Any]
    ) -> dict[str, Union[str, int]]:
        return {
            "source": event_dict["source"],
            "event_id": event_dict["source_event_id"],
            "event_type": event_dict["source_event_type"],
            "ingested_at": self.evaluator.to_millis(event_dict["observed_at"]),
            "schema_version": NORMALIZED_SCHEMA_VERSION,
        }

    def normalize(self, event: RawEvent) -> dict[str, Any]:
        self.logger.info(
            f"Normalizing event id={event.source_event_id} "
            f"source={event.source} type={event.source_event_type}"
        )
        contract_rules = self._collect_field_rules(
            contract=get_contract(source=event.source),
            event_type=event.source_event_type,
        )
        cleaned_event_data = self._clean_event_dictionary(event.model_dump())
        normalized_fields = self._resolve_fields(event.payload, contract_rules)

        return {
            **cleaned_event_data,
            **normalized_fields,
        }
