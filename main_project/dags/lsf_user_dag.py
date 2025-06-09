import os
import json

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# ——————————————————————————————————————————————
# 1. Paths & raw config load
# ——————————————————————————————————————————————
DAG_FOLDER   = os.path.dirname(__file__)
CONFIG_PATH  = os.path.join(DAG_FOLDER, "main_project", "config.json")

with open(CONFIG_PATH) as f:
    raw_config = json.load(f)

# ——————————————————————————————————————————————
# 2. Extract pipeline_settings via your handler
# ——————————————————————————————————————————————
from main_project.config_handler import load_pipeline_settings
pipeline_cfg    = load_pipeline_settings(raw_config)
timezone_str    = pipeline_cfg.get("timezone", "UTC")
max_retries     = pipeline_cfg.get("max_retry_attempts", 3)
retry_delay_secs= pipeline_cfg.get("retry_pause_base_secs", 30)

# Pendulum timezone & durations
local_tz      = pendulum.timezone(timezone_str)
retry_delay   = pendulum.duration(seconds=retry_delay_secs)
schedule_every= pendulum.duration(hours=8)
start_date    = pendulum.datetime(2025, 6, 9, 0, 0, 0, tz=local_tz)

# ——————————————————————————————————————————————
# 3. Default args
# ——————————————————————————————————————————————
default_args = {
    "owner":           "navneeth",
    "depends_on_past": False,
    "retries":         max_retries,
    "retry_delay":     retry_delay,
}

# ——————————————————————————————————————————————
# 4. Python callable to invoke your pipeline
# ——————————————————————————————————————————————
def run_lsf_pipeline():
    # re-load full config (including secrets) at runtime
    from main_project.config_handler import load_config_from_json
    config = load_config_from_json(CONFIG_PATH)

    # call your existing entrypoint
    from main_project.run_main_pipeline import run_main_pipeline
    run_main_pipeline(config)

# ——————————————————————————————————————————————
# 5. DAG definition
# ——————————————————————————————————————————————
with DAG(
    dag_id="lsf_pipeline",
    description="LSF ETL pipeline (every 8h; timezone from config)",
    default_args=default_args,
    schedule_interval=schedule_every,
    start_date=start_date,
    catchup=False,
    max_active_runs=1,
    tags=["lsf", "etl"],
) as dag:

    run_pipeline = PythonOperator(
        task_id="run_main_pipeline",
        python_callable=run_lsf_pipeline,
    )
