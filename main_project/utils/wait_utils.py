import math

def compute_scaled_pause(record_count: int, base_wait_secs: int, scaling_threshold: int, slope: float) -> int:
    """
    Computes a scaled pause duration based on record count and configuration.

    Parameters:
        record_count (int): Actual number of records.
        base_wait_secs (int): Base wait time in seconds for up to the threshold.
        scaling_threshold (int): Record count threshold after which pause is scaled.
        slope (float): Slope factor to control pause growth beyond threshold.

    Returns:
        int: Final computed wait time in seconds (rounded up).
    """
    if record_count <= scaling_threshold:
        return base_wait_secs

    excess_ratio = (record_count - scaling_threshold) / scaling_threshold
    scaled_wait = base_wait_secs + slope * base_wait_secs * excess_ratio
    return math.ceil(scaled_wait)
