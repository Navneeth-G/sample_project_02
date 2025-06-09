import snowflake.connector

def update_retry_attempt_in_audit_table(config_dict: dict, attempt_number: int):
    """
    Updates the retry attempt count for the current audit row in Snowflake.

    Args:
        config_dict (dict): Dictionary with Snowflake connection info and audit identifiers.
        attempt_number (int): Retry attempt number to update.
    """
    table_fqn = f'{config_dict["sf_database"]}.{config_dict["sf_schema"]}.{config_dict["sf_audit_table"]}'

    try:
        conn = snowflake.connector.connect(
            user=config_dict["sf_user"],
            password=config_dict["sf_password"],
            account=config_dict["sf_account"],
            warehouse=config_dict["sf_warehouse"],
            database=config_dict["sf_database"],
            schema=config_dict["sf_schema"]
        )
        cursor = conn.cursor()

        update_sql = f"""
            UPDATE {table_fqn}
            SET retry_attempt = %(attempt)s
            WHERE pipeline_name = %(pipeline_name)s
              AND index_name = %(index_name)s
              AND query_windows_start_ts = %(start_ts)s
              AND query_windows_end_ts = %(end_ts)s
        """
        cursor.execute(update_sql, {
            "attempt": attempt_number,
            "pipeline_name": config_dict["pipeline_name"],
            "index_name": config_dict["index_name"],
            "start_ts": config_dict["query_windows_start_ts"],
            "end_ts": config_dict["query_windows_end_ts"]
        })
        conn.commit()
        print(f"[INFO] Updated retry_attempt to {attempt_number} in audit table.")
    except Exception as e:
        print(f"[ERROR] Failed to update retry attempt in audit table: {e}")
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass




def get_sf_count_from_snowflake(config_dict: dict, sf_col_pattern: str) -> int:
    """
    Fetches the row count from Snowflake raw table using a prefix match on 'filename'.

    Args:
        config_dict (dict): Contains Snowflake credentials and table details.
        sf_col_pattern (str): Prefix string of the 'filename' column (e.g., 'users_20250603').

    Returns:
        int: Row count in the raw table matching the prefix.
    """
    try:
        conn = snowflake.connector.connect(
            user=config_dict["sf_user"],
            password=config_dict["sf_password"],
            account=config_dict["sf_account"],
            warehouse=config_dict["sf_warehouse"],
            database=config_dict["sf_database"],
            schema=config_dict["sf_schema"]
        )
        cursor = conn.cursor()

        raw_table_fqn = f"{config_dict['sf_database']}.{config_dict['sf_schema']}.{config_dict['sf_raw_table']}"
        query = f"""
            SELECT COUNT(*) FROM {raw_table_fqn}
            WHERE filename LIKE %(pattern)s
        """
        cursor.execute(query, {"pattern": f"{sf_col_pattern}%"})
        row = cursor.fetchone()
        return row[0] if row else 0
    except Exception as e:
        print(f"[ERROR] Failed to fetch SF count: {e}")
        return 0
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass



