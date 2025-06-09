import math

def compute_scaled_pause(actual_count: int, base_count: int, base_time_sec: int, slope: float) -> int:
    """
    Computes a dynamically scaled wait/pause time based on actual record count vs. base reference.

    Args:
        actual_count (int): Actual number of records encountered.
        base_count (int): Reference average record count the base_time_sec is designed for.
        base_time_sec (int): Base pause time in seconds for base_count records.
        slope (float): Skewness multiplier to control pause scaling beyond base_count.

    Returns:
        int: Final scaled wait time in seconds (rounded up).
    """
    if base_count <= 0:
        raise ValueError("base_count must be greater than 0")

    if actual_count <= base_count:
        return base_time_sec

    excess_ratio = (actual_count - base_count) / base_count
    scaled_pause = base_time_sec + (slope * base_time_sec * excess_ratio)

    return math.ceil(scaled_pause)
