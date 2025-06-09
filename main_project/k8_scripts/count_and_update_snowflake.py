import os
import re
import json
import pendulum
import snowflake.connector


def parse_lsb_users_file_legacy(file_path, farm, timezone="UTC"):
    """
    Parses a legacy-formatted LSF usergroup block and extracts fairshare data.

    Args:
        file_path (str): Full path to the lsb.users file
        farm (str): Name of the farm/environment
        timezone (str): Timezone for timestamp field

    Returns:
        List[Dict]: Parsed records from the UserGroup block
    """
    try:
        with open(file_path, 'r') as f:
            lines = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
    except Exception as e:
        print(f"[WARN] Could not open or read file for farm '{farm}': {e}")
        return []

    in_group = False
    process_next_line = False
    records = []

    for line in lines:
        if "Begin UserGroup" in line:
            in_group = True
            process_next_line = False
            continue
        if "End UserGroup" in line:
            in_group = False
            continue
        if in_group and "GROUP_NAME" in line and "GROUP_MEMBER" in line and "USER_SHARES" in line and "#USER_SHARES" not in line:
            process_next_line = True
            continue
        if process_next_line and not line.startswith("#"):
            line_clean = line.split("#")[0].strip()
            values = re.split(r"\s+", line_clean)
            if len(values) < 3:
                continue

            group_name = values[0]
            member_str = values[1]
            share_str = values[2]

            members = re.findall(r'\w+', member_str)
            shares = re.findall(r'\[([^\]]+)\]', share_str)
            share_dict = {}
            for s in shares:
                try:
                    user, val = s.split(",")
                    share_dict[user.strip()] = int(val.strip())
                except:
                    continue

            for user in members:
                fairshare = share_dict.get(user, None)
                if fairshare is not None:
                    records.append({
                        "farm": farm,
                        "group": group_name,
                        "user_name": user,
                        "fairshare": fairshare,
                        "timestamp": pendulum.now(timezone).to_iso8601_string()
                    })
    return records


def update_snowflake_count(info_dict, total_count):
    """
    Updates the ES_count field in the audit table in Snowflake.

    Args:
        info_dict (dict): Config dictionary with Snowflake credentials
        total_count (int): Total number of parsed usergroup records
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
    Main function that parses all farm files, counts records, and updates Snowflake.
    """
    total_records = 0
    for farm in info_dict["farm_list"]:
        file_path = info_dict["template_path"].replace("{fm}", farm)
        if not os.path.exists(file_path):
            print(f"[WARN] File not found for farm '{farm}': {file_path}")
            continue
        records = parse_lsb_users_file_legacy(file_path, farm, timezone=info_dict.get("timezone", "UTC"))
        print(f"[INFO] Parsed {len(records)} records for farm '{farm}'")
        total_records += len(records)

    update_snowflake_count(info_dict, total_records)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        info_dict = json.load(f)

    count_and_update_snowflake(info_dict)
