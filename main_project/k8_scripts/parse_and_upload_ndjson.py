import os
import re
import json
import pendulum
import boto3


def parse_lsf_users_file_legacy(file_path, farm, timezone="UTC", fixed_timestamp=None,
                                 valid_fields=None, disallowed_fields=None):
    """
    Parses a legacy LSF file and returns valid records based on required and disallowed field headers.

    Args:
        file_path (str): Path to the file (e.g., /data/farm1/lsb.users)
        farm (str): Identifier for the farm
        timezone (str): Timezone for consistent timestamping
        fixed_timestamp (str): Common timestamp for all records
        valid_fields (List[str]): Required column names
        disallowed_fields (List[str]): Forbidden column names

    Returns:
        List[Dict]: Parsed and filtered records
    """
    try:
        with open(file_path, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"[WARN] Cannot read file for farm '{farm}': {e}")
        return []

    in_group = False
    process_next_line = False
    records = []

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
                        "timestamp": fixed_timestamp
                    })
    return records


def write_ndjson(records, output_path):
    with open(output_path, 'w') as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")


def upload_to_s3(local_path, bucket, s3_key, aws_access_key, aws_secret_key):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    s3.upload_file(local_path, bucket, s3_key)
    print(f"[INFO] Uploaded to s3://{bucket}/{s3_key}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    all_records = []
    timestamp = pendulum.now(config.get("timezone", "UTC")).to_iso8601_string()

    for farm in config["farm_list"]:
        file_path = config["template_path"].replace("{fm}", farm)
        if not os.path.exists(file_path):
            print(f"[WARN] File not found: {file_path}")
            continue
        records = parse_lsf_users_file_legacy(
            file_path=file_path,
            farm=farm,
            timezone=config.get("timezone", "UTC"),
            fixed_timestamp=timestamp,
            valid_fields=config.get("valid_block_fields", []),
            disallowed_fields=config.get("disallowed_fields", [])
        )
        all_records.extend(records)

    output_path = config["final_ndjson_output_path"]
    write_ndjson(all_records, output_path)

    upload_to_s3(
        local_path=output_path,
        bucket=config["s3_bucket"],
        s3_key=config["s3_key"],
        aws_access_key=config["aws_access_key"],
        aws_secret_key=config["aws_secret_key"]
    )

    os.remove(output_path)
    print(f"[INFO] Temporary file deleted: {output_path}")


if __name__ == "__main__":
    main()
