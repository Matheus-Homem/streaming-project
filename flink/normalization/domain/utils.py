from datetime import datetime, timezone
from typing import Union


def to_millis(value: Union[str, int, float, datetime, None]) -> int:
    if value is None:
        return 0

    if isinstance(value, (int, float)):
        if abs(value) > 1e11:
            return int(value)
        return int(value * 1000)

    if isinstance(value, str):
        normalized_str = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return int(value.timestamp() * 1000)

    raise TypeError(
        f"Unsupported type for millisecond conversion: '{type(value).__name__}'. "
        "Expected str, int, float, or datetime."
    )
