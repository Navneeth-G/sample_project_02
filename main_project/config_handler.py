from pathlib import Path
import json
from airflow.models import Variable  #  Required for loading Airflow secrets

def load_airflow_secrets() -> dict:
    aws_secret = Variable.get("aws_credentials", deserialize_json=True)
    sf_secret = Variable.get("snowflake_credentials", deserialize_json=True)

    return {
        "aws_access_key": aws_secret["aws_access_key"],
        "aws_secret_key": aws_secret["aws_secret_key"],
        "sf_username": sf_secret["sf_username"],
        "sf_password": sf_secret["sf_password"],
        "sf_account": sf_secret["sf_account"]
    }

def load_pipeline_settings(config_json: dict) -> dict:
    return config_json.get("pipeline_settings", {})

def load_k8_config(config_json: dict) -> dict:
    k8 = config_json["k8_details"]
    return {
        "ssh_conn_id": k8["ssh_conn_id"],
        "remote_working_dir": k8["remote_working_dir"],

        "count_local_script_path": k8["count_script"]["local_path"],
        "count_remote_script_path": k8["count_script"]["remote_path"],
        "count_tmp_local_config_path": k8["count_script"]["tmp_local_config_path"],
        "count_remote_config_path": k8["count_script"]["remote_config_path"],

        "parser_local_script_path": k8["parser_script"]["local_path"],
        "parser_remote_script_path": k8["parser_script"]["remote_path"],
        "parser_tmp_local_config_path": k8["parser_script"]["tmp_local_config_path"],
        "parser_remote_config_path": k8["parser_script"]["remote_config_path"]
    }

def load_aws_config(config_json: dict) -> dict:
    aws = config_json["aws_details"]
    return {
        "s3_bucket": aws["s3_bucket"],
        "s3_prefix_base": aws["s3_prefix_base"]
    }

def load_sf_config(config_json: dict) -> dict:
    sf = config_json["snowflake_details"]
    return {
        "sf_warehouse": sf["warehouse"],
        "sf_database": sf["database"],
        "sf_schema": sf["schema"],
        "sf_audit_table": sf["audit_table"],
        "sf_raw_table": sf["raw_table"],
        "sf_col_pattern": sf["sf_col_pattern"],
        "task_name": sf["task_name"]
    }


def load_config_from_json(json_path: str) -> dict:
    with open(Path(json_path)) as f:
        raw_config = json.load(f)

    final_config = {}
    final_config.update(load_pipeline_settings(raw_config))            # e.g. pipeline_name, index_name, timezone
    final_config.update(load_k8_config(raw_config))
    final_config.update(load_aws_config(raw_config))
    final_config.update(load_sf_config(raw_config))
    final_config.update(load_airflow_secrets())

    return final_config
