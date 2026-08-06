from time import sleep as time_sleep


class RetryTimer:

    def __init__(self, default_time: int):
        self._time = default_time
        self._max_time = 60 * 5

    def __str__(self):
        return str(self._time)

    def sleep(self) -> "RetryTimer":
        time_sleep(self._time)
        return self

    def reset(self) -> "RetryTimer":
        self._time = 5
        return self

    def increase(self) -> "RetryTimer":
        if self._time < self._max_time:
            self._time = self._time * 2
        else:
            self._time = self._max_time
        return self
