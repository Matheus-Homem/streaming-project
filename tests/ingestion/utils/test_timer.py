import unittest
from unittest.mock import patch

from ingestion.utils.timer import RetryTimer

TEST_TIME = 5
MAX_TIME = 300


class TestRetryTimer(unittest.TestCase):

    def test_timer_initializer(self):
        timer = RetryTimer(100)
        self.assertEqual(timer._time, 100)
        self.assertEqual(timer._max_time, MAX_TIME)
        self.assertEqual(timer._default_time, 100)

    def test_timer_cant_initialize_whithout_default_time(self):
        with self.assertRaises(TypeError):
            timer = RetryTimer()

    def test_timer_str(self):
        timer = RetryTimer(TEST_TIME)
        self.assertEqual(f"{timer}", str(TEST_TIME))

    @patch("ingestion.utils.timer.time_sleep")
    def test_timer_sleep(self, sleep_mock):
        timer = RetryTimer(TEST_TIME)
        returned_timer = timer.sleep()

        sleep_mock.assert_called_once_with(TEST_TIME)
        self.assertIsInstance(returned_timer, RetryTimer)

    def test_timer_reset(self):
        timer = RetryTimer(TEST_TIME)
        timer._time = 100
        returned_timer = timer.reset()

        self.assertEqual(timer._time, TEST_TIME)
        self.assertIsInstance(returned_timer, RetryTimer)

    def test_timer_increase_less_than_max_time(self):
        timer = RetryTimer(TEST_TIME)
        returned_timer = timer.increase()

        self.assertEqual(timer._time, TEST_TIME * 2)
        self.assertIsInstance(returned_timer, RetryTimer)

    def test_timer_increase_over_max_time(self):
        timer = RetryTimer(200)
        returned_timer = timer.increase()

        self.assertEqual(timer._time, MAX_TIME)
        self.assertIsInstance(returned_timer, RetryTimer)


if __name__ == "__main__":
    unittest.main()
