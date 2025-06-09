import os
import re
import json
import pendulum
import snowflake.connector

def count_usergroup_records_legacy(file_path, farm, valid_fields=None, disallowed_fields=None):
    """
    Counts valid usergroup records in a legacy-format LSF file without storing them in memory.

    Args:
        file_path (str): Full path to the file (e.g., /data/env1/lsb.users)
        farm (str): Name of the farm/environment
        valid_fields (List[str]): Required column headers
        disallowed_fields (List[str]): Forbidden column headers

    Returns:
        int: Number of valid records
    """
    try:
        with open(file_path, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"[WARN] Could not read file for farm '{farm}': {e}")
        return 0

    in_group = False
    process_next_line = False
    total_count = 0

    for line in lines:
        if line.startswith("#"):
            continue
        if "Begin UserGroup" in line:
            in_group = True
            process_next_line = False
            continue
        if "End UserGroup" in line:
            in_group = False
            continue
        if in_group and valid_fields and all(field in line for field in valid_fields):
            if disallowed_fields and any(bad in line for bad in disallowed_fields):
                process_next_line = False
                continue
            process_next_line = True
            continue
        if process_next_line:
            if line.startswith("#"):
                continue
            values = re.split(r"\s+", line.split("#")[0].strip())
            if len(values) < 3:
                continue
            member_str = values[1]
            members = re.findall(r'\w+', member_str)
            total_count += len(members)

    return total_count


def update_snowflake_count(info_dict, total_count):
    """
    Updates the ES_count field in the audit table in Snowflake.
    """
    table_fqn = f'{info_dict["sf_database"]}.{info_dict["sf_schema"]}.{info_dict["sf_audit_table"]}'

    conn = snowflake.connector.connect(
        user=info_dict["sf_username"],
        password=info_dict["sf_password"],
        account=info_dict["sf_account"],
        warehouse=info_dict["sf_warehouse"],
        database=info_dict["sf_database"],
        schema=info_dict["sf_schema"]
    )
    cursor = conn.cursor()

    update_sql = f"""
        UPDATE {table_fqn}
        SET ES_count = %(total_count)s
        WHERE index_name = %(index_name)s
          AND elt_date = %(elt_date)s
    """

    try:
        cursor.execute(update_sql, {
            "total_count": total_count,
            "index_name": info_dict["index_name"],
            "elt_date": info_dict["elt_date"]
        })
        conn.commit()
        print(f"[INFO] Snowflake audit table updated with ES_count = {total_count}")
    except Exception as e:
        print(f"[ERROR] Failed to update Snowflake: {e}")
    finally:
        cursor.close()
        conn.close()


def count_and_update_snowflake(info_dict):
    """
    Main function that counts usergroup records and updates Snowflake.
    """
    total_records = 0
    valid_fields = info_dict.get("valid_block_fields", ["GROUP_NAME", "GROUP_MEMBER", "USER_SHARES"])
    disallowed_fields = info_dict.get("disallowed_block_fields", ["PRIORITY", "GROUP_ADMIN"])

    for farm in info_dict["farm_list"]:
        file_path = info_dict["template_path"].replace("{fm}", farm)
        if not os.path.exists(file_path):
            print(f"[WARN] File not found for farm '{farm}': {file_path}")
            continue
        count = count_usergroup_records_legacy(file_path, farm, valid_fields, disallowed_fields)
        print(f"[INFO] Counted {count} valid records for farm '{farm}'")
        total_records += count

    update_snowflake_count(info_dict, total_records)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        info_dict = json.load(f)

    count_and_update_snowflake(info_dict)
