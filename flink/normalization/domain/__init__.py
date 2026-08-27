from abc import ABC, abstractmethod
from logging import getLogger
from typing import Any

from flink.normalization.models import NormalizationContract
from shared.models import RawEvent


class ContractRepository(ABC):
    """Resolves and provides normalization contracts by source."""

    @abstractmethod
    def get(self, source: str) -> NormalizationContract:
        """Return the NormalizationContract associated with a source.

        Args:
            source (str): Source identifier to look up the contract for.

        Returns:
            NormalizationContract: The contract defined for the source.

        Raises:
            NotImplementedError: If the source has no contract defined.
        """


class EventEvaluator(ABC):
    """Applies a normalization contract's rules to a raw event's payload."""

    def __init__(self, class_name: str):
        self.logger = getLogger(class_name)

    @abstractmethod
    def apply(self, event: RawEvent, contract: NormalizationContract) -> dict[str, Any]:
        """Apply the contract's rules to the event's payload.

        Args:
            event (RawEvent): Raw event whose payload will be evaluated.
            contract (NormalizationContract): Contract defining the rules to apply.

        Returns:
            dict[str, Any]: The normalized fields produced by the contract's rules.
        """
