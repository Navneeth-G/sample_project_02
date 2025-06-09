from main_project.utils.snowflake_connect import create_snowflake_connection

def check_audit_status(input_dict: dict) -> str:
    """
    Check the most recent status in the audit table for the given query window.

    Parameters:
        input_dict (dict): Must include keys:
            - sf_user, sf_password, sf_account
            - sf_warehouse, sf_database, sf_schema
            - pipeline_name, index_name
            - query_windows_start_ts, query_windows_end_ts
            - audit_table (table name)

    Returns:
        str: Most recent status from audit table or None if not found.
    """
    try:
        conn = create_snowflake_connection(input_dict)
        cursor = conn.cursor()

        fully_qualified_table_name = (
            f"{input_dict['sf_database']}."
            f"{input_dict['sf_schema']}."
            f"{input_dict['audit_table']}"
        )

        query = f"""
            SELECT status
            FROM {fully_qualified_table_name}
            WHERE pipeline_name = %(pipeline_name)s
              AND index_name = %(index_name)s
              AND query_windows_start_ts = %(query_windows_start_ts)s
              AND query_windows_end_ts = %(query_windows_end_ts)s
            ORDER BY rec_last_updated_ts DESC
            LIMIT 1
        """

        cursor.execute(query, {
            "pipeline_name": input_dict["pipeline_name"],
            "index_name": input_dict["index_name"],
            "query_windows_start_ts": input_dict["query_windows_start_ts"],
            "query_windows_end_ts": input_dict["query_windows_end_ts"],
        })

        row = cursor.fetchone()
        return row[0] if row else None

    except Exception as e:
        print(f" Error checking audit status: {e}")
        return None

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass
