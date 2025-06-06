from pathlib import Path
import json
from airflow.providers.ssh.hooks.ssh import SSHHook

def send_and_run_count_in_k8(config_dict):
    ssh_conn_id = config_dict["ssh_conn_id"]
    local_script_path = config_dict["local_script_path"]
    remote_script_path = config_dict["remote_script_path"]
    tmp_local_config_path = config_dict["tmp_local_config_path"]
    remote_config_path = config_dict["remote_config_path"]

    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)

    # Save the config locally as a temporary JSON file
    local_tmp = Path(tmp_local_config_path)
    with open(local_tmp, "w") as f:
        json.dump(config_dict, f, indent=2)

    with ssh_hook.get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()
        try:
            # Upload config to remote pod
            sftp.put(str(local_tmp), remote_config_path)
        finally:
            sftp.close()

        # Execute count script on remote pod
        ssh_client.exec_command(f"python3 {remote_script_path} --config {remote_config_path}")

        # Optional cleanup: delete the config file after execution
        ssh_client.exec_command(f"rm -f {remote_config_path}")

    return "✅ Count job submitted and executed in K8 pod."
