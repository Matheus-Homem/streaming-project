from pydantic import ValidationError
from pyflink.datastream.functions import FlatMapFunction, RuntimeContext

from flink.normalization.domain.normalizer import EventNormalizer
from flink.normalization.ports import NormalizationFlatMapFunctionPort
from shared.models import RawEvent


class NormalizationFlatMapFunction(NormalizationFlatMapFunctionPort, FlatMapFunction):

    def __init__(self, normalizer: EventNormalizer):
        super().__init__(self.__class__.__name__)
        self._normalizer = normalizer

    def open(self, runtime_context: RuntimeContext):
        pass

    def flat_map(self, value: str):
        try:
            raw_event = RawEvent.model_validate_json(value)
        except ValidationError:
            self.logger.warning("Discarding malformed raw event")
            return

        try:
            normalized_event = self._normalizer.normalize(raw_event)
        except NotImplementedError:
            self.logger.warning(
                f"Unknown source, discarding event: source={raw_event.source}"
            )
            return
        except Exception:
            self.logger.exception(
                f"Failed to normalize event id={raw_event.source_event_id} "
                f"source={raw_event.source}"
            )
            return

        yield normalized_event.model_dump_json()
