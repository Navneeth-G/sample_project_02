# File: k8_scripts/run_parser_job.py

import os
import re
import json
import argparse
from pathlib import Path
import boto3

def parse_lsf_file(file_path, farm_name, timezone_str):
    from pendulum import now
    records = []

    with open(file_path, "r") as f:
        lines = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

    in_group = False
    process_next_line = False

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

            members = re.findall(r"\w+", member_str)
            shares = re.findall(r"\[([^\]]+)\]", share_str)

            share_dict = {}
            for s in shares:
                try:
                    user, val = s.split(",")
                    share_dict[user.strip()] = int(val.strip())
                except Exception:
                    continue

            for user in members:
                fairshare = share_dict.get(user)
                if fairshare is not None:
                    records.append({
                        "farm": farm_name,
                        "group": group_name,
                        "user_name": user,
                        "fairshare": fairshare,
                        "timestamp": now(timezone_str).to_iso8601_string()
                    })

    return records

def write_ndjson(records, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

def upload_to_s3(local_path, s3_path, aws_key, aws_secret):
    bucket = s3_path.split("/")[2]
    key = "/".join(s3_path.split("/")[3:])
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret
    )
    s3.upload_file(local_path, bucket, key)

def cleanup(file_path):
    try:
        os.remove(file_path)
    except Exception:
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config, "r") as f:
        config = json.load(f)

    timezone = config.get("timezone", "UTC")
    all_records = []

    for farm in config["farm_list"]:
        src_path = config["template_path"].replace("{fm}", farm)
        if os.path.exists(src_path):
            all_records += parse_lsf_file(src_path, farm, timezone)

    output_path = config["final_ndjson_output_path"]
    write_ndjson(all_records, output_path)

    upload_to_s3(
        output_path,
        config["aws_s3_final_output_path"],
        config["aws_access_key_id"],
        config["aws_secret_access_key"]
    )

    cleanup(output_path)
