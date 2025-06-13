import os
import re
import json
import pendulum
import boto3
from typing import List, Tuple, Optional

def parse_multiple_farms_and_upload_to_s3(
    farm_list: List[str],
    file_path_template: str,
    s3_bucket: str,
    s3_key_prefix: str,
    index_name: str,
    aws_access_key: str,
    aws_secret_key: str,
    timezone: str = "UTC",
    temp_output_dir: str = "/tmp"
) -> Optional[Tuple[str, str]]:
    """
    Parses LSF user group files for each farm, extracts fairshare records, and uploads a single NDJSON to AWS S3.

    Returns:
        Tuple (local_file_path, s3_uri) on success, or None if nothing to upload.
    """
    ts = pendulum.now(timezone)
    ts_formatted = ts.format("YYYY-MM-DDTHH-mm-ss")
    output_file_name = f"{index_name}_{ts_formatted}.ndjson"
    output_path = os.path.join(temp_output_dir, output_file_name)

    all_records = []

    for farm in farm_list:
        file_path = file_path_template.replace("{farm}", farm)

        if not os.path.isfile(file_path):
            print(f"[INFO][{farm}] File not found: {file_path}. Skipping.")
            continue

        print(f"[INFO][{farm}] Using file: {file_path}")

        try:
            with open(file_path, "r") as f:
                lines = [line.strip() for line in f if line.strip()]
        except Exception as e:
            print(f"[WARN][{farm}] Could not read file: {e}")
            continue

        if not any("Begin UserGroup" in line for line in lines):
            print(f"[INFO][{farm}] No 'Begin UserGroup' found. Skipping.")
            continue

        in_group = False
        process_next_line = False
        record_count_for_farm = 0

        for i, line in enumerate(lines):
            print(f"[DEBUG][{farm}] Line {i}: {line}")
            if line.startswith("#"):
                continue
            if "Begin UserGroup" in line:
                in_group = True
                continue
            if "End UserGroup" in line:
                in_group = False
                process_next_line = False
                continue
            if in_group and "GROUP_NAME" in line and "GROUP_MEMBER" in line and "USER_SHARES" in line:
                process_next_line = True
                continue
            if process_next_line:
                line = line.split("#")[0].strip()
                members_match = re.search(r"\(([^)]*)\)", line)
                group_name = line[:members_match.start()].strip() if members_match else "<unknown>"
                members = members_match.group(1).split() if members_match else []

                shares = re.findall(r"\[([^\]]+)\]", line)
                share_dict = {}
                for s in shares:
                    try:
                        user, val = s.split(",")
                        share_dict[user.strip()] = int(val.strip())
                    except Exception as e:
                        print(f"[WARN][{farm}] Malformed share '{s}': {e}")

                for user in members:
                    fairshare = share_dict.get(user)
                    if fairshare is not None:
                        record = {
                            "farm": farm,
                            "group": group_name,
                            "user_name": user,
                            "fairshare": fairshare,
                            "timestamp": ts.to_iso8601_string()
                        }
                        all_records.append(record)
                        record_count_for_farm += 1
                    else:
                        print(f"[WARN][{farm}] No fairshare found for user '{user}'")

        print(f"[INFO][{farm}] Parsed {record_count_for_farm} records.")

    if not all_records:
        print("[INFO] No records found across all farms. Nothing to upload.")
        return None

    try:
        with open(output_path, "w") as f:
            for record in all_records:
                f.write(json.dumps(record) + "\n")
        print(f"[INFO] Wrote {len(all_records)} records to {output_path}")
    except Exception as e:
        print(f"[ERROR] Failed to write NDJSON file: {e}")
        return None

    s3_date = ts.format("YYYY-MM-DD")
    s3_time = ts.format("HH-mm")
    s3_filename = f"{index_name}_{ts_formatted}.ndjson"
    s3_key = f"{s3_key_prefix}/{s3_date}/{s3_time}/{s3_filename}"

    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        s3.upload_file(output_path, s3_bucket, s3_key)
        print(f"[INFO] Uploaded to s3://{s3_bucket}/{s3_key}")
    except Exception as e:
        print(f"[ERROR] Failed to upload to S3: {e}")
        return None

    return output_path, f"s3://{s3_bucket}/{s3_key}"
