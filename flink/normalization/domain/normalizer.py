from flink.normalization.domain.utils import to_millis
from flink.normalization.models import EventEvaluator, NormalizedEvent
from flink.normalization.ports import ContractRepositoryPort
from shared.models import RawEvent

NORMALIZED_SCHEMA_VERSION = 1

_EXTRACTED_FIELDS = {"partition_key", "entity_id", "entity_name", "event_time"}


class EventNormalizer:

    def __init__(
        self,
        event_evaluator: EventEvaluator,
        contract_repository: ContractRepositoryPort,
    ):
        self._event_evaluator = event_evaluator
        self._contract_repository = contract_repository

    def normalize(self, event: RawEvent) -> NormalizedEvent:
        contract = self._contract_repository.get(event.source)
        evaluated = self._event_evaluator.apply(event=event, contract=contract)

        raw = event.model_dump()
        extra = {k: v for k, v in evaluated.items() if k not in _EXTRACTED_FIELDS}

        return NormalizedEvent(
            source=raw["source"],
            event_id=raw["source_event_id"],
            event_type=raw["source_event_type"],
            ingested_at=to_millis(raw["observed_at"]),
            schema_version=NORMALIZED_SCHEMA_VERSION,
            partition_key=evaluated["partition_key"],
            entity_id=str(evaluated["entity_id"]),
            entity_name=evaluated["entity_name"],
            event_time=evaluated["event_time"],
            **extra,
        )
