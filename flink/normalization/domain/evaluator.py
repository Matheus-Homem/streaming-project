import json
from datetime import datetime, timezone
from itertools import chain
from typing import Any

import jmespath

from flink.normalization.domain import EventEvaluator
from flink.normalization.models import FieldRule, NormalizationContract
from shared.models import RawEvent


class NormalizationFunctions(jmespath.functions.Functions):

    @jmespath.functions.signature({"types": ["string", "null"]})
    def _func_iso_to_millis(self, iso_string):
        if not iso_string:
            return None
        dt = datetime.fromisoformat(iso_string)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)


class NormalizationRulesEventEvaluator(EventEvaluator):

    def __init__(self):
        super().__init__(self.__class__.__name__)
        functions = NormalizationFunctions()
        self.options = jmespath.Options(custom_functions=functions)

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
            name: self._assess_payload_with_rule(rule, payload)
            for name, rule in contract_rules
        }

    def _compile_rule(self, rule: FieldRule) -> str:
        def _wrap_jmespath_value(value: Any) -> str:
            return f"`{json.dumps(value)}`"

        if rule.expression:
            return rule.expression

        match rule:
            case FieldRule(take=take) if take:
                base = f"{rule.from_}[].{take}"
            case FieldRule(as_="boolean"):
                base = f"{rule.from_} != `null`"
            case FieldRule(as_="timestamp"):
                base = f"iso_to_millis({rule.from_})"
            case _:
                base = rule.from_

        if rule.default is None:
            return base
        return f"{base} || {_wrap_jmespath_value(rule.default)}"

    def _assess_payload_with_rule(
        self, rule: FieldRule, payload: dict[str, Any]
    ) -> Any:
        return jmespath.search(
            expression=self._compile_rule(rule),
            data=payload,
            options=self.options,
        )

    def apply(self, event: RawEvent, contract: NormalizationContract) -> dict[str, Any]:
        contract_rules = self._collect_field_rules(
            contract=contract,
            event_type=event.source_event_type,
        )
        return self._resolve_fields(event.payload, contract_rules)
