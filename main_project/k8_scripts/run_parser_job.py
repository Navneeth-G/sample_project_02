import os
import re
import json
import pendulum
import boto3
import logging

# Enable full debug logging
logging.basicConfig(level=logging.DEBUG)


def parse_lsf_users_file_legacy(file_path, farm, timezone="UTC", fixed_timestamp=None,
                                 valid_fields=None, disallowed_fields=None):
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
        print(f"[DEBUG] Raw line: {line}")  # Line preview

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
            print(f"[DEBUG] Valid block line detected: {line}")
            print(f"[DEBUG] Valid fields required: {valid_fields}")
            if disallowed_fields and any(bad in line for bad in disallowed_fields):
                print(f"[DEBUG] Line rejected due to disallowed fields: {line}")
                process_next_line = False
                continue
            process_next_line = True
            continue
        if process_next_line:
            if line.startswith("#"):
                continue
            values = re.split(r"\s+", line.split("#")[0].strip())
            if len(values) < 3:
                print(f"[DEBUG] Skipping short line: {line}")
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
    print(f"[DEBUG] Parsed {len(records)} records for farm: {farm}")
    return records


def write_ndjson(records, output_path):
    with open(output_path, 'w') as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    print(f"[DEBUG] Wrote NDJSON to: {output_path}")
    print(f"[DEBUG] File size: {os.path.getsize(output_path)} bytes")

    # Optional preview of output
    with open(output_path, 'r') as f:
        print("[DEBUG] First 2 lines of NDJSON:")
        for i in range(2):
            print(f.readline().strip())


def upload_to_s3(local_path, bucket, s3_key, aws_access_key, aws_secret_key):
    print(f"[DEBUG] Preparing to upload: {local_path}")
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        s3.upload_file(local_path, bucket, s3_key)
        print(f"[INFO] Uploaded to s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"[ERROR] Failed to upload to S3: {e}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    print(f"[DEBUG] Template path: {config['template_path']}")
    print(f"[DEBUG] Output path: {config['final_ndjson_output_path']}")
    print(f"[DEBUG] S3 bucket/key: {config['s3_bucket']} / {config['s3_key']}")

    all_records = []
    timestamp = pendulum.now(config.get("timezone", "UTC")).to_iso8601_string()

    for farm in config["farm_list"]:
        file_path = config["template_path"].replace("{fm}", farm)
        print(f"[DEBUG] Looking for file: {file_path}")
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

    print(f"[DEBUG] Total records parsed: {len(all_records)}")
    if not all_records:
        print("[ERROR] No records were parsed. Nothing to upload.")
        return

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
