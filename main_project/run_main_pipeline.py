import pendulum
from main_project.k8_launcher.send_and_run_count_in_k8 import send_and_run_count_in_k8
from main_project.k8_launcher.send_and_run_parser_in_k8 import send_and_run_parser_in_k8
from main_project.snowflake_utils import (
    update_audit_status,
    get_snowflake_row_count,
    delete_snowflake_records,
)
from main_project.aws_utils import delete_s3_folder

def run_main_pipeline(config_dict: dict):
    index_name = config_dict["index_name"]
    audit_date = config_dict["query_start"].split("T")[0]
    retry_limit = config_dict.get("retry_limit", 2)

    # ✅ Step 1: Initial audit row with null count
    update_audit_status(
        index_name=index_name,
        date=audit_date,
        status="in_progress",
        source_count=None
    )

    try:
        # ✅ Step 2: Count K8 pod records
        source_count = send_and_run_count_in_k8(config_dict)

        # ✅ Step 3: Update audit table with actual count
        update_audit_status(
            index_name=index_name,
            date=audit_date,
            status="in_progress",
            source_count=source_count
        )

        # ✅ Step 4: Run parsing and upload
        send_and_run_parser_in_k8(config_dict)

        # ✅ Step 5: External task loads S3 to Snowflake — wait and check
        snowflake_count = get_snowflake_row_count(index_name, audit_date)

        if snowflake_count == source_count:
            update_audit_status(
                index_name=index_name,
                date=audit_date,
                status="completed",
                target_count=snowflake_count,
                retry_number=0
            )
        else:
            for retry_num in range(1, retry_limit + 1):
                update_audit_status(
                    index_name=index_name,
                    date=audit_date,
                    status="failed",
                    retry_number=retry_num,
                    error_message="Row count mismatch — retrying."
                )
                delete_snowflake_records(index_name, audit_date)
                delete_s3_folder(config_dict["final_ndjson_output_path"])

                send_and_run_parser_in_k8(config_dict)
                snowflake_count = get_snowflake_row_count(index_name, audit_date)

                if snowflake_count == source_count:
                    update_audit_status(
                        index_name=index_name,
                        date=audit_date,
                        status="completed",
                        target_count=snowflake_count,
                        retry_number=retry_num
                    )
                    break
            else:
                update_audit_status(
                    index_name=index_name,
                    date=audit_date,
                    status="failed",
                    retry_number=retry_limit,
                    error_message="Final retry failed: counts still mismatch."
                )

    except Exception as e:
        update_audit_status(
            index_name=index_name,
            date=audit_date,
            status="failed",
            error_message=str(e)
        )
        raise
