from pathlib import Path
import json
from airflow.providers.ssh.hooks.ssh import SSHHook


def send_and_run_count_in_k8(config_dict):
    """
    Sends the count script and config to the remote K8 pod via SSH,
    executes it, and then optionally deletes the remote config.

    Args:
        config_dict (dict): Dictionary with keys:
            - ssh_conn_id
            - local_script_path
            - remote_script_path
            - tmp_local_config_path
            - remote_config_path

    Returns:
        str: Status message
    """
    try:
        ssh_conn_id = config_dict["ssh_conn_id"]
        local_script_path = config_dict["local_script_path"]
        remote_script_path = config_dict["remote_script_path"]
        tmp_local_config_path = config_dict["tmp_local_config_path"]
        remote_config_path = config_dict["remote_config_path"]

        ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)

        # Step 1: Save the config locally as a temporary JSON file
        local_tmp = Path(tmp_local_config_path)
        with open(local_tmp, "w") as f:
            json.dump(config_dict, f, indent=2)

        #  Step 2: SSH and SFTP to pod
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            try:
                # Upload config file
                sftp.put(str(local_tmp), remote_config_path)
                print(f"[INFO] Config file uploaded to: {remote_config_path}")
            finally:
                sftp.close()

            #  Step 3: Execute the remote script
            stdin, stdout, stderr = ssh_client.exec_command(f"python3 {remote_script_path} --config {remote_config_path}")
            stdout_output = stdout.read().decode().strip()
            stderr_output = stderr.read().decode().strip()
            print(f"[INFO] STDOUT:\n{stdout_output}")
            if stderr_output:
                print(f"[WARN] STDERR:\n{stderr_output}")

            #  Step 4: Clean up remote config
            ssh_client.exec_command(f"rm -f {remote_config_path}")
            print(f"[INFO] Removed remote config: {remote_config_path}")

        return " Count job submitted and executed in K8 pod."

    except Exception as e:
        return f" Failed to submit count job to K8 pod: {e}"
