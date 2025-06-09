import snowflake.connector

def get_snowflake_raw_count(info_dict: dict, sf_col_pattern: str) -> int:
    """
    Queries the Snowflake raw table to get the count of records that match a specific filename pattern.

    Args:
        info_dict (dict): Snowflake connection and table info. Must include:
            - sf_username
            - sf_password
            - sf_account
            - sf_warehouse
            - sf_database
            - sf_schema
            - sf_raw_table
        sf_col_pattern (str): Prefix pattern used in the 'filename' column (e.g., 'bucket/prefix/.../usergroup_simple_20250603')

    Returns:
        int: Count of records matching the filename pattern. Returns None if no result or query fails.
    """
    try:
        table_fqn = f"{info_dict['sf_database']}.{info_dict['sf_schema']}.{info_dict['sf_raw_table']}"

        conn = snowflake.connector.connect(
            user=info_dict["sf_username"],
            password=info_dict["sf_password"],
            account=info_dict["sf_account"],
            warehouse=info_dict["sf_warehouse"],
            database=info_dict["sf_database"],
            schema=info_dict["sf_schema"]
        )
        cursor = conn.cursor()

        query = f"""
            SELECT COUNT(*) 
            FROM {table_fqn}
            WHERE filename LIKE '{sf_col_pattern}%'
        """
        cursor.execute(query)
        row = cursor.fetchone()

        return row[0] if row else None

    except Exception as e:
        print(f"[ERROR] Failed to fetch Snowflake raw count: {e}")
        return None

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass
