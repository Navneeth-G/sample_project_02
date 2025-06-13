import os
import re
import json
import pendulum
import boto3
from typing import List, Tuple, Optional
from k8_scipts.block_identify import extract_first_usergroup_block_lines
from k8_scripts.


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

    ts_obj = pendulum.now(timezone)
    timestamp_str = ts_obj.to_iso8601_string()
    all_records = []

    for farm in farm_list:
        file_path = file_path_template.replace("{farm}", farm)
        if not os.path.isfile(file_path):
            print(f"[WARN] File not found for {farm}: {file_path}")
            continue

        with open(file_path, "r") as f:
            raw_lines = [line.strip() for line in f if line.strip()]

        block_lines = extract_first_usergroup_block_lines(raw_lines)
        parsed = parse_usergroup_block_lines(block_lines, farm, timestamp_str)
        print(f"[INFO][{farm}] Parsed {len(parsed)} records")
        all_records.extend(parsed)

    if not all_records:
        print("[INFO] No records found. Skipping JSON and upload.")
        return

    output_path = write_records_to_json(all_records, index_name, output_dir, ts_obj)
    if not output_path:
        return

    s3_uri = upload_json_to_s3(output_path, s3_bucket, s3_key_prefix,
                               index_name, ts_obj, aws_access_key, aws_secret_key)
    if s3_uri:
        clean_temp_json_file(output_path)

    return s3_uri
