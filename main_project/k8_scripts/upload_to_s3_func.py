import os
import boto3
import pendulum

def upload_json_file_to_s3(
    local_file_path,
    s3_bucket,
    s3_key_prefix,
    index_name,
    ts_obj,
    aws_access_key,
    aws_secret_key
):
    """
    Uploads a single .json file to AWS S3 under a structured key path with fixed time component as '00-00'.

    Parameters
    ----------
    local_file_path : str
        Full path to the local file to upload.

    s3_bucket : str
        Name of the S3 bucket.

    s3_key_prefix : str
        Prefix path inside the bucket (e.g., "fairshare/all_farms").

    index_name : str
        Logical identifier used in the S3 filename.

    ts_obj : pendulum.DateTime
        Timestamp object used to structure the S3 key path and file name.

    aws_access_key : str
        AWS access key ID.

    aws_secret_key : str
        AWS secret access key.

    Returns
    -------
    str or None
        Full S3 URI if upload was successful, else None.
    """
    if not os.path.exists(local_file_path):
        print(f"[ERROR] File does not exist: {local_file_path}")
        return None

    ts_str = ts_obj.format("YYYY-MM-DDTHH-mm-ss")
    s3_date = ts_obj.format("YYYY-MM-DD")
    s3_time = "00-00"  
    filename = f"{index_name}_{ts_str}.json"
    s3_key = f"{s3_key_prefix}/{s3_date}/{s3_time}/{filename}"

    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        s3.upload_file(local_file_path, s3_bucket, s3_key)
        print(f"[INFO] Uploaded to s3://{s3_bucket}/{s3_key}")
        return f"s3://{s3_bucket}/{s3_key}"
    except Exception as e:
        print(f"[ERROR] Upload failed: {e}")
        return None
