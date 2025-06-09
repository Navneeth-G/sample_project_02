# Updated main pipeline logic with retry mechanism

import time
from main_project.snowflake_ops.compute_scaled_pause import compute_scaled_pause
from main_project.snowflake_ops.update_audit_retry_attempt import update_retry_attempt_in_audit_table
from main_project.utils.check_snowflake_audit import check_audit_status
from main_project.k8_launcher.send_and_run_count_in_k8 import send_and_run_count_in_k8
from main_project.k8_launcher.send_and_run_parser_in_k8 import send_and_run_parser_in_k8
from main_project.snowflake_ops.get_es_count_from_audit import get_es_count_from_audit_table
from main_project.snowflake_ops.trigger_task_and_wait import trigger_snowflake_task_and_wait
from main_project.snowflake_ops.get_sf_count import get_sf_count_from_snowflake
from main_project.snowflake_ops.compare_and_cleanup import handle_data_comparison_and_cleanup
from main_project.utils.get_query_timestamps import get_query_window_timestamps
from main_project.snowflake_ops.insert_audit_entry import insert_initial_audit_entry

def run_main_pipeline(config_dict):
    # Step 1: Get query timestamps
    start_ts, end_ts = get_query_window_timestamps(config_dict["timezone"])
    config_dict["query_windows_start_ts"] = start_ts
    config_dict["query_windows_end_ts"] = end_ts

    status = check_audit_status({
        "pipeline_name": config_dict["pipeline_name"],
        "index_name": config_dict["index_name"],
        "query_windows_start_ts": start_ts,
        "query_windows_end_ts": end_ts,
        **config_dict
    })

    now = time.time()

    # Step 2: Skip if status is in_progress or completed
    if status in ["in_progress", "completed"]:
        print("[INFO] Skipping ELT - already processed or running.")
        return

    # Step 3: Re-run if failed and within valid window
    if status == "failed":
        ts_now = time.time()
        ts_start = time.mktime(time.strptime(start_ts, "%Y-%m-%dT%H:%M:%S%z"))
        ts_end = time.mktime(time.strptime(end_ts, "%Y-%m-%dT%H:%M:%S%z"))
        if not (ts_start <= ts_now <= ts_end):
            print("[INFO] Skipping retry - outside retry window.")
            return

    # Step 4: Proceed with ELT + Audit Retry Loop
    max_retry_attempts = config_dict.get("max_retry_attempts", 3)
    retry_pause_base_secs = config_dict.get("retry_pause_base_secs", 30)
    current_attempt = 1

    while current_attempt <= max_retry_attempts:
        print(f"[INFO] ELT Attempt #{current_attempt}")
        try:
            # (Re)Insert audit status as in_progress
            insert_initial_audit_entry(config_dict, status="in_progress", retry_attempt=current_attempt)

            # Run ES Count in K8
            send_and_run_count_in_k8(config_dict)

            # Fetch latest ES count from Snowflake audit table
            es_count = get_es_count_from_audit_table(config_dict)
            if es_count is None:
                raise Exception("ES Count is None after count step")

            # Compute scaled wait before ingestion
            scaled_ingest_wait = compute_scaled_pause(
                record_count=es_count,
                base_wait_secs=config_dict["base_sf_ingest_wait_secs"],
                scaling_threshold=config_dict["avg_base_record_count"],
                slope=config_dict["ingest_pause_slope"]
            )
            print(f"[INFO] Waiting {scaled_ingest_wait}s for Snowflake ingestion readiness")
            time.sleep(scaled_ingest_wait)

            # Trigger ingestion
            trigger_snowflake_task_and_wait(config_dict)

            # Parse, send to S3, update status if fail
            send_and_run_parser_in_k8(config_dict)

            # Fetch Snowflake count from raw table
            sf_count = get_sf_count_from_snowflake(config_dict, config_dict["sf_col_pattern"])
            if sf_count is None:
                raise Exception("SF Count is None after ingestion")

            # Final comparison and cleanup
            handle_data_comparison_and_cleanup(config_dict, es_count, sf_count)

            print("[INFO] ELT pipeline completed successfully.")
            return  # Success

        except Exception as e:
            print(f"[ERROR] Attempt #{current_attempt} failed: {e}")
            update_retry_attempt_in_audit_table(config_dict, attempt_number=current_attempt)

            if current_attempt == max_retry_attempts:
                print("[FATAL] Max attempts reached. Marking audit as failed.")
                insert_initial_audit_entry(config_dict, status="failed", retry_attempt=current_attempt)
                return

            # Pause before next retry
            pause_time = compute_scaled_pause(
                record_count=es_count if 'es_count' in locals() else config_dict.get("avg_base_record_count", 1000),
                base_wait_secs=retry_pause_base_secs,
                scaling_threshold=config_dict.get("avg_base_record_count", 1000),
                slope=config_dict.get("retry_pause_slope", 1.0)
            )
            print(f"[INFO] Waiting {pause_time}s before retrying...")
            time.sleep(pause_time)
            current_attempt += 1
