from airflow.providers.ssh.hooks.ssh import SSHHook
from pathlib import Path
import json

def send_and_run_parser_in_k8(config_dict: dict):
    ssh_conn_id = config_dict["ssh_conn_id"]
    local_script_path = config_dict["local_script_path"]
    remote_script_path = config_dict["remote_script_path"]
    tmp_local_config_path = config_dict["tmp_local_config_path"]
    remote_config_path = config_dict["remote_config_path"]
    remote_work_dir = config_dict["remote_working_dir"]

    # Step 1: Write config dict to a local JSON file
    local_tmp = Path(tmp_local_config_path)
    with open(local_tmp, "w") as f:
        json.dump(config_dict, f, indent=2)

    # Step 2: Establish SSH connection
    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
    with ssh_hook.get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()

        try:
            # Step 3: Prepare the remote working directory
            ssh_client.exec_command(f"mkdir -p {remote_work_dir}")

            # Step 4: Send script and config
            sftp.put(local_script_path, remote_script_path)
            sftp.put(str(local_tmp), remote_config_path)

            # Step 5: Run the parser script remotely
            ssh_client.exec_command(f"python3 {remote_script_path} --config {remote_config_path}")

        finally:
            sftp.close()

        # Step 6: Cleanup after execution
        ssh_client.exec_command(f"rm -rf {remote_work_dir}")
