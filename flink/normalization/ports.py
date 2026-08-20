from abc import ABC, abstractmethod
from typing import Any, Optional

from flink.normalization.models import FieldRule
from shared.models import RawEvent


class NormalizationEngineBase(ABC):

    @abstractmethod
    def normalize(self, event: RawEvent) -> dict[str, Any]:
        """Apply the source's normalization contract to a RawEvent's payload.

        Looks up the contract for `event.source`, resolves the field rules
        that apply to `event.source_event_type` (partition_key + envelope +
        common + event-type-specific), and evaluates each rule against
        `event.payload`.

        Args:
            event: Raw event as produced by the ingestion layer, still
                carrying the source-specific payload untouched.

        Returns:
            Flat dict merging the event's non-payload fields with the
            resolved field values, ready to be emitted downstream.
        """


class NormalizationEvaluatorBase(ABC):

    @abstractmethod
    def evaluate(self, rule: FieldRule, payload: dict[str, Any]) -> Any:
        """Compile a single FieldRule into a query expression and evaluate it.

        Translates the declarative rule (`from`/`take`/`as`/`default`/
        `expression`) into the compiler's underlying query language, then
        runs it against `payload` to produce one resolved value.

        Args:
            rule: Declarative rule describing how to extract/transform a
                single field.
            payload: Source-specific payload to query.

        Returns:
            The resolved value, or the rule's `default` (or None) when the
            path is absent.
        """

    @abstractmethod
    def to_millis(self, iso_string: Optional[str]) -> Optional[int]:
        """Convert an ISO 8601 timestamp to epoch milliseconds.

        Exposes the same conversion `as: timestamp` applies to payload fields,
        for values that never pass through a contract rule. The engine needs it
        for `ingested_at`, which comes from the RawEvent envelope rather than
        from the payload the rules are evaluated against.

        Args:
            iso_string: ISO 8601 timestamp, or None/empty.

        Returns:
            Epoch milliseconds, or None when the input is absent or empty.
        """
