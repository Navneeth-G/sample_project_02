# utils/query_window_utils.py

import pendulum

def get_query_window_timestamps(timezone_str: str = "UTC") -> dict:
    """
    Returns the start and end timestamps (ISO 8601) for the current day
    in the specified timezone.

    Args:
        timezone_str (str): Timezone in which to compute the day window (default is UTC)

    Returns:
        dict: {
            "query_windows_start_ts": <start-of-day>,
            "query_windows_end_ts": <end-of-day>
        }
    """
    try:
        tz = pendulum.timezone(timezone_str)
    except Exception:
        raise ValueError(f"Invalid timezone string provided: {timezone_str}")

    now = pendulum.now(tz)
    start_of_day = now.start_of("day").to_iso8601_string()
    end_of_day = now.end_of("day").to_iso8601_string()

    return {
        "query_windows_start_ts": start_of_day,
        "query_windows_end_ts": end_of_day
    }
