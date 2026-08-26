import unittest
from collections import deque

from ingestion.domain.tracker import InMemoryDuplicateTracker


class TestInMemoryDuplicateTracker(unittest.TestCase):

    def _get_tracker(self, max_size: int = 200):
        return InMemoryDuplicateTracker(max_size)

    def test_initialize(self):
        max_size = 100
        tracker = self._get_tracker(max_size)

        self.assertEqual(tracker.deque.maxlen, max_size)
        self.assertIsInstance(tracker.deque, deque)
        self.assertIsInstance(tracker.memory, set)

    def test_is_not_duplicate(self):
        tracker = self._get_tracker()
        result = tracker.is_duplicated(10)
        self.assertFalse(result)

    def test_is_duplicate(self):
        tracker = self._get_tracker()
        tracker.record(10)
        result = tracker.is_duplicated(10)
        self.assertTrue(result)

    def test_can_record(self):
        tracker = self._get_tracker(1)
        tracker.record(10)
        self.assertEqual(tracker.memory, {10})
        self.assertEqual(tracker.deque.index(10), 0)

    def test_can_record_full_memory(self):
        tracker = self._get_tracker(1)
        tracker.record(10)
        tracker.record(20)
        self.assertEqual(tracker.memory, {20})
        self.assertEqual(tracker.deque.index(20), 0)


if __name__ == "__main__":
    unittest.main()
