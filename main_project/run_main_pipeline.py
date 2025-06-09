import pendulum
from utils.query_window import get_query_window_timestamps
from utils.snowflake_audit import check_audit_status, update_audit_status, fetch_snowflake_row_count, delete_from_snowflake
from utils.aws_cleanup import delete_from_s3
from k8_launcher.send_and_run_count_in_k8 import send_and_run_count_in_k8
from k8_launcher.send_and_run_parser_in_k8 import send_and_run_parser_in_k8

def run_main_pipeline(config_dict):
    timezone = config_dict["timezone"]
    index_name = config_dict["index_name"]
    pipeline_name = config_dict["pipeline_name"]

    # Step 1: Get query window timestamps
    query_start, query_end = get_query_window_timestamps(timezone)
    config_dict["query_windows_start_ts"] = query_start
    config_dict["query_windows_end_ts"] = query_end

    # Step 2: Check Snowflake audit table status
    audit_status_input = {
        "sf_user": config_dict["sf_username"],
        "sf_password": config_dict["sf_password"],
        "sf_account": config_dict["sf_account"],
        "sf_warehouse": config_dict["sf_warehouse"],
        "sf_database": config_dict["sf_database"],
        "sf_schema": config_dict["sf_schema"],
        "audit_table": config_dict["sf_audit_table"],
        "pipeline_name": pipeline_name,
        "index_name": index_name,
        "query_windows_start_ts": query_start,
        "query_windows_end_ts": query_end
    }

    status = check_audit_status(audit_status_input)
    now = pendulum.now(timezone)
    start_ts = pendulum.parse(query_start)
    end_ts = pendulum.parse(query_end)

    if status in ("in_progress", "completed"):
        print(f"[INFO] Skipping run. Status is already {status}.")
        return

    if status == "failed" and (start_ts <= now <= end_ts):
        print("[INFO] Retrying failed ELT + audit.")
    elif status == "failed":
        print("[INFO] Status is failed but window has expired. Skipping.")
        return

    # Step 3: Mark audit as in-progress with null ES_count
    update_audit_status(
        index_name=index_name,
        date=query_start.split("T")[0],
        status="in_progress",
        source_count=None,
        config=config_dict
    )

    # Step 4: Run record counting job in K8 pod
    try:
        send_and_run_count_in_k8(config_dict)
    except Exception as e:
        update_audit_status(
            index_name=index_name,
            date=query_start.split("T")[0],
            status="failed",
            source_count=None,
            config=config_dict
        )
        raise RuntimeError(f"[ERROR] Count job failed: {e}")

    # Step 5: Run parser job in K8 pod
    try:
        send_and_run_parser_in_k8(config_dict)
    except Exception as e:
        update_audit_status(
            index_name=index_name,
            date=query_start.split("T")[0],
            status="failed",
            source_count=None,
            config=config_dict
        )
        raise RuntimeError(f"[ERROR] Parser job failed: {e}")

    # Step 6: Fetch row count from Snowflake for validation
    sf_pattern = config_dict["sf_filename_pattern"]
    sf_count = fetch_snowflake_row_count(config_dict, sf_pattern)

    # Step 7: Update Snowflake audit with final count
    update_audit_status(
        index_name=index_name,
        date=query_start.split("T")[0],
        status="completed" if sf_count == config_dict["expected_record_count"] else "failed",
        source_count=sf_count,
        config=config_dict
    )

    # Step 8: If mismatch, delete bad data from Snowflake and S3
    if sf_count != config_dict["expected_record_count"]:
        print("[WARN] Count mismatch. Triggering cleanup.")
        delete_from_snowflake(config_dict, sf_pattern)
        delete_from_s3(config_dict["s3_bucket"], config_dict["s3_delete_prefix"], config_dict)
        print("[INFO] Cleanup complete. Status marked as failed.")
