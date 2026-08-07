from datetime import datetime


class RateLimitError(Exception):

    def __init__(self, reset_at: datetime):
        message = f"Connection from source reached rate limit, expected at {reset_at}"
        super().__init__(message)
        self.reset_at = reset_at
