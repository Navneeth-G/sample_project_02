import pendulum
import traceback
from main_project.utils.get_query_window import get_query_window_timestamps
from main_project.utils.check_audit_status import check_audit_status
from main_project.utils.sf_connection import get_snowflake_connection
from main_project.k8_launcher.send_and_run_parser_in_k8 import send_and_run_parser_in_k8
from main_project.k8_launcher.send_and_run_count_in_k8 import send_and_run_count_in_k8
from main_project.snowflake_ops.update_audit_status import update_audit_status
from main_project.snowflake_ops.get_es_count import get_es_count_from_audit
from main_project.snowflake_ops.get_sf_count import get_sf_row_count
from main_project.snowflake_ops.delete_from_sf import delete_snowflake_records
from main_project.aws_ops.delete_from_s3 import delete_s3_objects_by_prefix
from main_project.snowflake_ops.trigger_task_and_wait import trigger_snowflake_task_and_wait
from main_project.utils.compute_scaled_pause import compute_scaled_pause
import time


def run_main_pipeline(config_dict):
    try:
        # Step 1: Compute query window timestamps
        ts_dict = get_query_window_timestamps(config_dict["timezone"])
        config_dict.update(ts_dict)

        # Step 2: Check current audit status
        audit_input = {
            "pipeline_name": config_dict["pipeline_name"],
            "index_name": config_dict["index_name"],
            "query_windows_start_ts": config_dict["query_windows_start_ts"],
            "query_windows_end_ts": config_dict["query_windows_end_ts"],
            "sf_user": config_dict["sf_user"],
            "sf_password": config_dict["sf_password"],
            "sf_account": config_dict["sf_account"],
            "sf_warehouse": config_dict["sf_warehouse"],
            "sf_database": config_dict["sf_audit_database"],
            "sf_schema": config_dict["sf_audit_schema"],
            "audit_table": config_dict["sf_audit_table"]
        }

        status = check_audit_status(audit_input)

        now = pendulum.now(config_dict["timezone"])
        in_window = ts_dict["query_windows_start_ts"] <= now.to_iso8601_string() <= ts_dict["query_windows_end_ts"]

        if status in ("in_progress", "completed"):
            print("[INFO] Data for this window already processed. Exiting.")
            return

        if status == "failed" and not in_window:
            print("[INFO] Previous failed run, but outside query window. Exiting.")
            return

        # Step 3: Update audit status to in_progress
        update_audit_status(config_dict, status="in_progress")

        # Step 4: Run ES count job on K8 pod
        send_and_run_count_in_k8(config_dict)

        # Step 5: Get the updated ES count from audit table
        source_count = get_es_count_from_audit(config_dict)
        print(f"[INFO] Source ES count: {source_count}")

        # Step 6: Compute pause time for Snowflake ingestion
        pause_ingest = compute_scaled_pause(
            actual_count=source_count,
            base_count=config_dict["avg_record_count"],
            base_time_sec=config_dict["base_pause_time_sf_ingestion"],
            slope=config_dict["pause_scaling_slope"]
        )
        print(f"[INFO] Pausing {pause_ingest} sec before triggering Snowflake task...")
        time.sleep(pause_ingest)

        # Step 7: Trigger Snowflake task to ingest S3 data
        trigger_snowflake_task_and_wait(config_dict)

        # Step 8: Compute pause time for ingestion to complete
        time.sleep(pause_ingest)

        # Step 9: Get SF count
        sf_count = get_sf_row_count(config_dict)
        print(f"[INFO] Snowflake row count: {sf_count}")

        # Step 10: Update audit status with SF count
        update_audit_status(config_dict, status="completed", sf_count=sf_count)

        # Step 11: Compare counts
        if sf_count != source_count:
            print("[WARN] Count mismatch. Cleaning up and marking as failed.")

            # Compute scaled delays
            pause_delete_s3 = compute_scaled_pause(
                actual_count=source_count,
                base_count=config_dict["avg_record_count"],
                base_time_sec=config_dict["base_pause_time_s3_deletion"],
                slope=config_dict["pause_scaling_slope"]
            )

            pause_delete_sf = compute_scaled_pause(
                actual_count=source_count,
                base_count=config_dict["avg_record_count"],
                base_time_sec=config_dict["base_pause_time_sf_deletion"],
                slope=config_dict["pause_scaling_slope"]
            )

            time.sleep(pause_delete_s3)
            delete_s3_objects_by_prefix(config_dict["s3_bucket"], config_dict["s3_prefix"])

            time.sleep(pause_delete_sf)
            delete_snowflake_records(config_dict)

            update_audit_status(config_dict, status="failed")

        else:
            print("[INFO] Record counts match. ELT pipeline successful.")

    except Exception as e:
        print(f"[ERROR] Pipeline failed: {e}")
        traceback.print_exc()
        update_audit_status(config_dict, status="failed")
