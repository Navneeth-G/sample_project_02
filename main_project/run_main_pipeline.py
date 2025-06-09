

def should_proceed_with_pipeline(status: str) -> bool:
    """
    Decide whether to proceed with pipeline execution based on audit table status.

    Parameters:
        status (str): The latest status from the audit table.

    Returns:
        bool: True if pipeline should proceed, False otherwise.
    """
    if status in ("in_progress", "completed"):
        print(f"⚠️  Skipping execution. Status is already '{status}'.")
        return False
    return True




