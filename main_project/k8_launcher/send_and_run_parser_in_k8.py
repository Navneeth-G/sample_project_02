from pathlib import Path
import json
from airflow.providers.ssh.hooks.ssh import SSHHook

def send_and_run_parser_in_k8(config_dict):
    """
    Sends the parser script and config to the remote K8 pod via SSH,
    executes it, and performs cleanup afterward.
    """
    ssh_conn_id = config_dict["ssh_conn_id"]
    local_script_path = config_dict["local_script_path"]
    remote_script_path = config_dict["remote_script_path"]
    tmp_local_config_path = config_dict["tmp_local_config_path"]
    remote_config_path = config_dict["remote_config_path"]
    remote_work_dir = config_dict["remote_working_dir"]

    local_tmp = Path(tmp_local_config_path)
    with open(local_tmp, "w") as f:
        json.dump(config_dict, f, indent=2)

    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)

    with ssh_hook.get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()
        try:
            ssh_client.exec_command(f"mkdir -p {remote_work_dir}")
            sftp.put(local_script_path, remote_script_path)
            sftp.put(str(local_tmp), remote_config_path)
        finally:
            sftp.close()

        stdin, stdout, stderr = ssh_client.exec_command(f"python3 {remote_script_path} --config {remote_config_path}")
        print(stdout.read().decode().strip())
        print(stderr.read().decode().strip())

        ssh_client.exec_command(f"rm -f {remote_script_path}")
        ssh_client.exec_command(f"rm -f {remote_config_path}")
        ssh_client.exec_command(f"rm -rf {remote_work_dir}")

    return "✅ Parser job submitted and executed in K8 pod."
