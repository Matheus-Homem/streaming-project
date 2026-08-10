from datetime import datetime, timezone
from time import sleep as time_sleep


class RetryTimer:

    def __init__(self, default_time: int):
        self._default_time = default_time
        self._time = default_time
        self._max_time = 60 * 5

    def __str__(self):
        return str(self._time)

    def sleep(self) -> "RetryTimer":
        time_sleep(self._time)
        return self

    def schedule_sleep(self, sleep_datetime: datetime) -> "RetryTimer":
        diff = sleep_datetime - datetime.now(tz=timezone.utc)
        if diff.total_seconds() < 0:
            return self
        time_sleep(int(diff.total_seconds()))
        return self

    def reset(self) -> "RetryTimer":
        self._time = self._default_time
        return self

    def increase(self) -> "RetryTimer":
        if (self._time * 2) < self._max_time:
            self._time = self._time * 2
        else:
            self._time = self._max_time
        return self
