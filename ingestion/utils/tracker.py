from collections import deque

class BoundedUniqueTracker:

    def __init__(self, max_size: int):
        self.deque = deque(maxlen=max_size)
        self.memory = set()
        self.max_size = max_size

    def is_duplicated(self, value: str) -> bool:
        if value in self.memory:
            return True
        return False

    def record(self, value: str) -> bool:
        if len(self.deque) >= self.max_size:
            oldest = self.deque.popleft()
            self.memory.remove(oldest)

        self.deque.append(value)
        self.memory.add(value)
