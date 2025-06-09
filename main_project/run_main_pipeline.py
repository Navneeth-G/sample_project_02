import pendulum
import traceback
from utils.get_query_window import get_query_window_timestamps
from utils.snowflake_connector import get_snowflake_connection
from utils.check_audit_status import check_audit_status
from utils.update_audit_status import update_audit_status
from utils.pause_scaling import compute_scaled_pause
from k8_launcher.send_and_run_count_in_k8 import send_and_run_count_in_k8
from k8_launcher.send_and_run_parser_in_k8 import send_and_run_parser_in_k8
from utils.trigger_snowflake_task import trigger_task
from utils.get_sf_row_count import get_snowflake_record_count
from utils.delete_invalid_data import delete_s3_and_snowflake_data

def run_main_pipeline(config_dict: dict):
    try:
        # ✅ Step 1: Determine query window
        query_window = get_query_window_timestamps(timezone_str=config_dict["timezone"])
        config_dict["query_windows_start_ts"] = query_window["query_windows_start_ts"]
        config_dict["query_windows_end_ts"] = query_window["query_windows_end_ts"]
        audit_date = pendulum.parse(config_dict["query_windows_start_ts"]).to_date_string()

        # ✅ Step 2: Check current audit status in Snowflake
        audit_check_input = {
            "pipeline_name": config_dict["pipeline_name"],
            "index_name": config_dict["index_name"],
            "query_windows_start_ts": config_dict["query_windows_start_ts"],
            "query_windows_end_ts": config_dict["query_windows_end_ts"],
            **config_dict
        }
        status = check_audit_status(audit_check_input)

        now = pendulum.now(config_dict["timezone"])
        start = pendulum.parse(config_dict["query_windows_start_ts"])
        end = pendulum.parse(config_dict["query_windows_end_ts"])

        if status in ("in_progress", "completed"):
            print(f"[INFO] ELT already handled for this window: {status}")
            return

        if status == "failed":
            if not (start <= now <= end):
                print("[INFO] Previous ELT failed, but data is stale. Skipping.")
                return
            print("[INFO] Retrying failed ELT for current window.")

        # ✅ Step 3: Mark audit as in_progress with no source count yet
        update_audit_status(
            index_name=config_dict["index_name"],
            date=audit_date,
            status="in_progress",
            source_count=None,
            **config_dict
        )

        # ✅ Step 4: Run parser job in K8 pod
        send_and_run_parser_in_k8(config_dict)

        # ✅ Step 5: Run ES count job in K8 pod
        send_and_run_count_in_k8(config_dict)

        # ✅ Step 6: Wait for S3-to-Snowflake ingestion to finish
        base_pause = config_dict["avg_pause_seconds"]
        base_count = config_dict["avg_expected_record_count"]
        slope = config_dict["pause_scaling_slope"]
        wait_time = compute_scaled_pause(base_pause, base_count, slope)

        print(f"[INFO] Waiting {wait_time:.2f} seconds for Snowpipe ingestion...")
        pendulum.sleep(wait_time)

        # ✅ Step 7: Trigger Snowflake task to start Snowpipe
        trigger_task(
            sf_username=config_dict["sf_username"],
            sf_password=config_dict["sf_password"],
            sf_account=config_dict["sf_account"],
            task_name=config_dict["sf_task_name"]
        )

        print("[INFO] Task triggered. Waiting for final ingestion...")
        pendulum.sleep(wait_time)

        # ✅ Step 8: Get record count from Snowflake raw table
        sf_count = get_snowflake_record_count(
            config_dict=config_dict,
            pattern=config_dict["sf_filename_prefix"]
        )

        # ✅ Step 9: Update SF count in audit table
        update_audit_status(
            index_name=config_dict["index_name"],
            date=audit_date,
            status="completed" if sf_count == config_dict["ES_count"] else "failed",
            sf_count=sf_count,
            **config_dict
        )

        # ✅ Step 10: Delete invalid data if mismatch
        if sf_count != config_dict["ES_count"]:
            print("[WARN] Row count mismatch. Deleting invalid data...")
            delete_s3_and_snowflake_data(config_dict)
        else:
            print("[SUCCESS] ELT pipeline completed with matching counts.")

    except Exception as e:
        print(f"[ERROR] Pipeline failed: {e}")
        traceback.print_exc()
        try:
            update_audit_status(
                index_name=config_dict.get("index_name", "unknown"),
                date=audit_date,
                status="failed",
                **config_dict
            )
        except:
            print("[ERROR] Failed to update audit status to failed.")
