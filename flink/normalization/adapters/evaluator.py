import json
from datetime import datetime, timezone
from logging import getLogger
from typing import Any, Optional

import jmespath

from flink.normalization.models import FieldRule
from flink.normalization.ports import NormalizationEvaluatorBase


class NormalizationFunctions(jmespath.functions.Functions):

    @jmespath.functions.signature({"types": ["string", "null"]})
    def _func_iso_to_millis(self, iso_string):
        if not iso_string:
            return None

        dt = datetime.fromisoformat(iso_string)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)


class NormalizationJmespathEvaluator(NormalizationEvaluatorBase):

    def __init__(self):
        self.logger = getLogger(self.__class__.__name__)
        self.functions = NormalizationFunctions()
        self.options = jmespath.Options(custom_functions=self.functions)

    def to_millis(self, iso_string: Optional[str]) -> Optional[int]:
        return self.functions._func_iso_to_millis(iso_string)

    @staticmethod
    def _jmespath_literal(value: Any) -> str:
        return f"`{json.dumps(value)}`"

    def _compile_rule(self, rule: FieldRule) -> str:
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
        return f"{base} || {NormalizationJmespathEvaluator._jmespath_literal(rule.default)}"

    def evaluate(self, rule: FieldRule, payload: dict[str, Any]) -> Any:
        return jmespath.search(
            expression=self._compile_rule(rule),
            data=payload,
            options=self.options,
        )
