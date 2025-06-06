import os
import re
import json
import pendulum
import snowflake.connector

def parse_lsb_users_file_legacy(file_path, farm, timezone="UTC"):
    with open(file_path, 'r') as f:
        lines = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

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
            header = re.split(r"\s+", line.strip().split("#")[0])
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
        UPDATE {info_dict["sf_audit_table"]}
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
    finally:
        cursor.close()
        conn.close()


def count_and_update_snowflake(info_dict):
    total_records = 0
    for farm in info_dict["farm_list"]:
        file_path = info_dict["template_path"].replace("{fm}", farm)
        if not os.path.exists(file_path):
            continue
        records = parse_lsb_users_file_legacy(file_path, farm, timezone=info_dict.get("timezone", "UTC"))
        total_records += len(records)

    update_snowflake_count(info_dict, total_records)
    print(f"[INFO] Updated Snowflake with total record count: {total_records}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        info_dict = json.load(f)

    count_and_update_snowflake(info_dict)
