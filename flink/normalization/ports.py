from abc import ABC, abstractmethod
from logging import getLogger
from typing import Any, Iterable

from flink.normalization.models import NormalizationContract


class NormalizationPort(ABC):
    """Base for normalization pipeline ports, providing a per-class logger."""

    def __init__(self, class_name: str):
        self.logger = getLogger(class_name)


class NormalizationFlatMapFunctionPort(NormalizationPort):
    """Transforms a single input event into zero, one, or more output events."""

    @abstractmethod
    def flat_map(self, value: Any) -> Iterable[Any]:
        """Receive an input event and yield zero, one, or more transformed events.

        Args:
            value (Any): Input event to transform.

        Returns:
            Iterable[Any]: Generator of zero, one, or more resulting events.
        """


class ContractRepositoryPort(ABC):
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
