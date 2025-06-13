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
    parse_multiple_farms_and_upload_to_s3

    Parses LSF-style user group configuration files from multiple farms, extracts fairshare user records, 
    and writes the data to a single NDJSON file which is uploaded to AWS S3.

    ----------------------------------------------------------------------------------------

    Inputs
    ------
    - farm_list : List[str]
        A list of farm/environment names to process. Each farm is expected to have an `lsb.users` file 
        located at a standard path.

    - file_path_template : str
        File path pattern with a `{farm}` placeholder for farm name.
        Example: "/global/lsf/cells/{farm}/conf/lsbatch/{farm}/configdir/lsb.users"

    - s3_bucket : str
        AWS S3 bucket name for storing the NDJSON file.

    - s3_key_prefix : str
        S3 prefix path (folder structure). Example: "fairshare/all_farms"

    - index_name : str
        A logical tag for naming the output file. Will be embedded in the filename.

    - aws_access_key : str
        AWS access key ID (ensure IAM permissions for s3:PutObject).

    - aws_secret_key : str
        AWS secret access key.

    - timezone : str (default = "UTC")
        Timezone for timestamping records and S3 key naming.

    - temp_output_dir : str (default = "/tmp")
        Local directory path to write the temporary NDJSON file before upload.

    ----------------------------------------------------------------------------------------

    ----------------------------------------------------------------------------------------

    Expected Source File Format (lsb.users)
    ---------------------------------------
    Begin UserGroup
    GROUP_NAME      GROUP_MEMBER                  USER_SHARES
    prod_users      (alice bob carol)             [alice, 10][bob, 20][carol, 30]
    dev_users       (dave eve)                    [default, 5]
    End UserGroup

    Each group line contains:
    - A group name
    - A list of users in parentheses
    - A list of share assignments in square brackets

    ----------------------------------------------------------------------------------------

    Parsing Logic
    -------------
    - For each non-comment line within a valid UserGroup section:
        1. Only the first three fields are parsed: group name, member list, and share list.
        2. Any content beyond the third field (e.g., inline comments or extra malformed fields) is ignored.
        3. Member list is extracted from parentheses: (alice bob)
        4. Share mappings are extracted from square brackets: [alice, 10]

    This ensures robustness even if comments or garbage data follow valid lines.

    ----------------------------------------------------------------------------------------


    ----------------------------------------------------------------------------------------

    Output (NDJSON File)
    --------------------
    - Written to: /tmp/lsf_user_share_2025-06-12T18-45-00.ndjson (local)
    - Uploaded to: s3://<bucket>/<s3_key_prefix>/<YYYY-MM-DD>/<HH-mm>/<index_name>_<timestamp>.ndjson

    Each line:
        {"farm": "us01_swe", "group": "prod_users", "user_name": "alice", "fairshare": 10, "timestamp": "..."}

    ----------------------------------------------------------------------------------------

    Logging Behavior
    ----------------
    - Logs every farm processed, files read, user lines parsed, and record-level details.
    - Warns on malformed shares, missing users, or missing fairshare.
    - Skips farms with no parseable content or missing files.

    ----------------------------------------------------------------------------------------

    Returns
    -------
    - Tuple: (local_output_path, s3_uri) on success
    - None if no records are parsed or upload fails

    ----------------------------------------------------------------------------------------

    Example Call
    ------------
    parse_multiple_farms_and_upload_to_s3(
        farm_list=["us01_swe", "tcad", "pythia"],
        file_path_template="/global/lsf/cells/{farm}/conf/lsbatch/{farm}/configdir/lsb.users",
        s3_bucket="my-audit-bucket",
        s3_key_prefix="fairshare/all_farms",
        index_name="lsf_user_share",
        aws_access_key="AKIA...",
        aws_secret_key="SECRET..."
    )
    """
    ts = pendulum.now(timezone)
    ts_formatted = ts.format("YYYY-MM-DDTHH-mm-ss")
    output_file_name = f"{index_name}_{ts_formatted}.json"
    output_path = os.path.join(temp_output_dir, output_file_name)

    all_records = []

    for farm in farm_list:
        file_path = file_path_template.replace("{farm}", farm)

        if not os.path.isfile(file_path):
            print(f"[INFO][{farm}] File not found: {file_path}. Skipping.")
            continue

        print(f"[INFO][{farm}] Processing farm.")
        print(f"[INFO][{farm}] Using source file: {file_path}")

        try:
            with open(file_path, "r") as f:
                lines = [line.strip() for line in f if line.strip()]
        except Exception as e:
            print(f"[WARN][{farm}] Could not read file: {e}")
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
                print(f"[DEBUG][{farm}] Found 'Begin UserGroup'")
                continue

            if "End UserGroup" in line:
                in_group = False
                process_next_line = False
                print(f"[DEBUG][{farm}] Found 'End UserGroup'")
                break  # << STOP AFTER FIRST BLOCK

            if in_group and "GROUP_NAME" in line and "GROUP_MEMBER" in line and "USER_SHARES" in line:
                process_next_line = True
                print(f"[DEBUG][{farm}] Header line detected.")
                continue

            if process_next_line:
                print(f"[DEBUG][{farm}] Raw line: {line}")
                parts = re.split(r"\s+", line.split("#")[0].strip())
                if len(parts) < 3:
                    print(f"[WARN][{farm}] Skipping line (too few columns): {line}")
                    continue

                group_name = parts[0]
                members_raw = parts[1]
                shares_raw = parts[2]

                print(f"[DEBUG][{farm}] Group: {group_name} | Members raw: {members_raw} | Shares raw: {shares_raw}")

                members = re.findall(r'\w+', members_raw)
                shares = re.findall(r'\[([^\]]+)\]', shares_raw)
                share_dict = {}

                for s in shares:
                    try:
                        user, val = s.split(",")
                        share_dict[user.strip()] = int(val.strip())
                    except Exception as e:
                        print(f"[WARN][{farm}] Malformed share '{s}': {e}")
                        continue

                print(f"[DEBUG][{farm}] Parsed share map: {share_dict}")

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
                        print(f"[DEBUG][{farm}] Parsed record: {record}")
                        all_records.append(record)
                        record_count_for_farm += 1
                    else:
                        print(f"[WARN][{farm}] No fairshare for user: {user}")

        print(f"[INFO][{farm}] Parsed {record_count_for_farm} records.")

    if not all_records:
        print("[INFO] No valid records found. Nothing to upload.")
        return

    try:
        with open(output_path, "w") as f:
            for record in all_records:
                f.write(json.dumps(record) + "\n")
        print(f"[INFO] NDJSON file created: {output_path}")
    except Exception as e:
        print(f"[ERROR] Failed to write file: {e}")
        return

    s3_date = ts.format("YYYY-MM-DD")
    s3_time = ts.format("HH-mm")
    s3_key = f"{s3_key_prefix}/{s3_date}/{s3_time}/{output_file_name}"

    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        s3.upload_file(output_path, s3_bucket, s3_key)
        print(f"[INFO] Uploaded to s3://{s3_bucket}/{s3_key}")
    except Exception as e:
        print(f"[ERROR] Upload to S3 failed: {e}")
        return

    print("[DONE] All farm data processed and uploaded.")
    return output_path, f"s3://{s3_bucket}/{s3_key}"

