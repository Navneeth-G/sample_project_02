import os
from typing import List
import re
import json
import boto3
import pendulum

def parse_multiple_farms_and_upload_to_s3(
    farm_list,
    file_path_template,
    s3_bucket,
    s3_key_prefix,
    index_name,
    aws_access_key,
    aws_secret_key,
    timezone="America/Los_Angeles",
    temp_output_dir="/tmp"
):
    """
    parse_multiple_farms_and_upload_to_s3

    Parses LSF-style user group configuration files from multiple farms, extracts fairshare user records, and writes the data
    to a single NDJSON file which is uploaded to AWS S3.

    ----------------------------------------------------------------------------------------

    Inputs
    ------
    - farm_list (List[str]):
        A list of farm/environment names to process. Each farm is expected to have an `lsb.users` file located at a standard path.

    - file_path_template (str):
        A file path pattern that includes `{farm}` as a placeholder for each farm name.
        Example: "/global/lsf/cells/{farm}/conf/lsbatch/{farm}/configdir/lsb.users"

    - s3_bucket (str):
        The name of the AWS S3 bucket where the NDJSON file should be uploaded.

    - s3_key_prefix (str):
        The path prefix in the S3 bucket under which the NDJSON file will be stored.
        Example: "fairshare/all_farms"
        The full S3 key will be generated dynamically using the current date and time.

    - index_name (str):
        A logical name that is included in the output filename, e.g., "lsf_user_share".

    - aws_access_key (str):
        AWS access key ID for uploading to S3.

    - aws_secret_key (str):
        AWS secret access key for uploading to S3.

    - timezone (str, optional, default "UTC"):
        The timezone to use for timestamps in the records and S3 key structure.

    - temp_output_dir (str, optional, default "/tmp"):
        Local directory path to write the temporary NDJSON file before uploading to S3.

    ----------------------------------------------------------------------------------------

    Expected Source File Format (lsb.users)
    ---------------------------------------
    Begin UserGroup
    GROUP_NAME      GROUP_MEMBER                  USER_SHARES
    prod_users      (alice bob carol)             [alice, 10][bob, 20][carol, 30]
    dev_users       (dave eve)                    [default, 5]
    End UserGroup

    Each line after the header defines a group, a list of users, and either default or explicit fairshare values.

    ----------------------------------------------------------------------------------------

    Output (S3 File)
    ---------------
    - A single NDJSON file will be created and uploaded.
    - File name example:
        lsf_user_share_2025-06-12T18-45-00.ndjson

    - S3 key path structure:
        s3://<bucket>/<s3_key_prefix>/<YYYY-MM-DD>/<HH-mm>/<index_name>_<timestamp>.ndjson

    - Each line in the NDJSON file looks like:
        {"farm": "us01_swe", "group": "prod_users", "user_name": "alice", "fairshare": 10, "timestamp": "2025-06-12T18:45:00+00:00"}

    ----------------------------------------------------------------------------------------

    Logging & Debugging
    -------------------
    - The function prints:
        - The source file being processed for each farm
        - Every raw line from the file
        - Group name, members, share mappings
        - Final parsed records per user
    - Skips farms that don't have the `lsb.users` file (no error, just a message)

    ----------------------------------------------------------------------------------------

    Returns
    -------
    - Tuple: (local_file_path, s3_uri) on success
    - Or None if no records were found or file writing/upload failed

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
    output_file_name = f"{index_name}_{ts_formatted}.ndjson"
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
                continue

            if in_group and "GROUP_NAME" in line and "GROUP_MEMBER" in line and "USER_SHARES" in line and "#USER_SHARES" not in line:
                process_next_line = True
                print(f"[DEBUG][{farm}] Header line detected. Expecting data lines next.")
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

                print(f"[DEBUG][{farm}] Group Name: {group_name}")
                print(f"[DEBUG][{farm}] Members raw: {members_raw}")
                print(f"[DEBUG][{farm}] Shares raw: {shares_raw}")

                members = re.findall(r'\w+', members_raw)
                print(f"[DEBUG][{farm}] Extracted members: {members}")

                shares = re.findall(r'\[([^\]]+)\]', shares_raw)
                share_dict = {}
                for s in shares:
                    try:
                        user, val = s.split(",")
                        share_dict[user.strip()] = int(val.strip())
                    except Exception as e:
                        print(f"[WARN][{farm}] Skipping malformed share: '{s}' | Error: {e}")
                        continue

                print(f"[DEBUG][{farm}] Parsed shares: {share_dict}")

                for user in members:
                    fairshare = share_dict.get(user, None)
                    if fairshare is not None:
                        record = {
                            "farm": farm,
                            "group": group_name,
                            "user_name": user,
                            "fairshare": fairshare,
                            "timestamp": ts.to_iso8601_string()
                        }
                        print(f"[DEBUG][{farm}] Final record: {json.dumps(record)}")
                        all_records.append(record)
                        record_count_for_farm += 1
                    else:
                        print(f"[WARN][{farm}] No fairshare found for user '{user}' in group '{group_name}'")

        print(f"[INFO][{farm}] Parsed {record_count_for_farm} records.")

    if not all_records:
        print("[INFO] No records found across all farms. Nothing to upload.")
        return

    try:
        with open(output_path, "w") as f:
            for record in all_records:
                f.write(json.dumps(record) + "\n")
        print(f"[INFO] Wrote {len(all_records)} total records to: {output_path}")
    except Exception as e:
        print(f"[ERROR] Failed to write NDJSON file: {e}")
        return

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
        print(f"[ERROR] Upload to S3 failed: {e}")
        return

    print("[DONE] All farm data processed and uploaded successfully.")
    return output_path, f"s3://{s3_bucket}/{s3_key}"



def cleanup_temp_ndjson_files(file_paths: List[str]):
    """
    Deletes the specified NDJSON files if they exist.

    Parameters
    ----------
    file_paths : List[str]
        List of full NDJSON file paths to delete.

    Returns
    -------
    List[str]
        Files successfully deleted.
    """
    deleted = []

    for path in file_paths:
        if os.path.isfile(path):
            try:
                os.remove(path)
                print(f"[INFO] Deleted: {path}")
                deleted.append(path)
            except Exception as e:
                print(f"[ERROR] Failed to delete {path}: {e}")
        else:
            print(f"[WARN] File not found: {path}")

    return deleted
