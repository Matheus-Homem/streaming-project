from abc import ABC, abstractmethod
from logging import getLogger
from typing import Any, Iterable


class TransformerPort(ABC):
    """Transforms a single input event into zero, one, or more output events."""

    def __init__(self, class_name: str):
        self.logger = getLogger(class_name)

    @abstractmethod
    def flat_map(self, value: Any) -> Iterable[Any]:
        """Receive an input event and yield zero, one, or more transformed events.

        Args:
            value (Any): Input event to transform.

        Returns:
            Iterable[Any]: Generator of zero, one, or more resulting events.
        """
