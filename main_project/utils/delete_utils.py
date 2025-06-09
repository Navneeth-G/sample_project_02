import boto3
import snowflake.connector

def delete_snowflake_records(sf_config: dict, filename_prefix: str):
    """
    Deletes records from Snowflake where filename starts with the given prefix.

    Args:
        sf_config (dict): Includes Snowflake credentials and raw table name
        filename_prefix (str): Prefix pattern to delete (e.g., 'index_name/yyyy-mm-dd/hh-mm/')
    """
    table_fqn = f"{sf_config['sf_database']}.{sf_config['sf_schema']}.{sf_config['sf_raw_table']}"
    conn = snowflake.connector.connect(
        user=sf_config["sf_user"],
        password=sf_config["sf_password"],
        account=sf_config["sf_account"],
        warehouse=sf_config["sf_warehouse"],
        database=sf_config["sf_database"],
        schema=sf_config["sf_schema"]
    )
    try:
        cursor = conn.cursor()
        delete_sql = f"""
            DELETE FROM {table_fqn}
            WHERE filename LIKE %(pattern)s
        """
        cursor.execute(delete_sql, {"pattern": f"{filename_prefix}%"})
        conn.commit()
        print(f"[INFO] Deleted Snowflake records for prefix: {filename_prefix}")
    finally:
        cursor.close()
        conn.close()

def delete_s3_file(aws_config: dict, s3_prefix: str):
    """
    Deletes an NDJSON file from S3.

    Args:
        aws_config (dict): Includes aws_access_key_id, aws_secret_access_key, and s3_bucket
        s3_prefix (str): Full path to the S3 object (key)
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_config["aws_access_key_id"],
        aws_secret_access_key=aws_config["aws_secret_access_key"]
    )
    try:
        s3.delete_object(Bucket=aws_config["s3_bucket"], Key=s3_prefix)
        print(f"[INFO] Deleted S3 file: s3://{aws_config['s3_bucket']}/{s3_prefix}")
    except Exception as e:
        print(f"[ERROR] Failed to delete S3 object: {e}")
