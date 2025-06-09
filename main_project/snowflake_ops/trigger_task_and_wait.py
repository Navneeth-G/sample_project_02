import snowflake.connector
import time

def trigger_snowflake_task_and_wait(config_dict):
    """
    Triggers a Snowflake task that loads data from S3 using Snowpipe and waits for ingestion to complete.

    Args:
        config_dict (dict): Dictionary containing Snowflake credentials and wait settings.
            Required keys:
                - sf_user
                - sf_password
                - sf_account
                - sf_warehouse
                - sf_database
                - sf_schema
                - task_name
                - wait_time_sec (default fallback)
    """
    task_fqn = f"{config_dict['sf_database']}.{config_dict['sf_schema']}.{config_dict['task_name']}"
    wait_time_sec = config_dict.get("wait_time_sec", 60)

    try:
        task_name = config_dict["task"]
        conn = snowflake.connector.connect(
            user=config_dict["sf_user"],
            password=config_dict["sf_password"],
            account=config_dict["sf_account"],
            warehouse=config_dict["sf_warehouse"],
            database=config_dict["sf_audit_database"],
            schema=config_dict["sf_audit_schema"]
        )
        cursor = conn.cursor()

        # Trigger the Snowflake task
        print(f"[INFO] Triggering Snowflake task: {task_fqn}")
        query = f"EXECUTE TASK {database}.{schema}.{task_name}"
        cursor.execute(query)

        print(f"[INFO] Waiting for {wait_time_sec} seconds to allow data ingestion via Snowpipe...")
        time.sleep(wait_time_sec)

    except Exception as e:
        print(f"[ERROR] Failed to trigger Snowflake task or wait: {e}")
        raise

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass
