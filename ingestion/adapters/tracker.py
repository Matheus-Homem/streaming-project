from collections import deque

from ingestion.ports import UniqueTrackerBase


class IngestionTracker(UniqueTrackerBase):

    def __init__(self, max_size: int):
        self.deque = deque(maxlen=max_size)
        self.memory = set()

    def is_duplicated(self, value: str) -> bool:
        if value in self.memory:
            return True
        return False

    def record(self, value: str) -> None:
        if len(self.deque) >= self.deque.maxlen:
            oldest = self.deque.popleft()
            self.memory.remove(oldest)

        self.deque.append(value)
        self.memory.add(value)
