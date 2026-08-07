from datetime import datetime


class RateLimitError(Exception):

    def __init__(self, expected_at: datetime):
        message = (
            f"Connection from source reached rate limit, expected at {expected_at}"
        )
        super().__init__(message)
        self.expected_at = expected_at
