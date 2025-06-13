import os
import re

def estimate_total_json_records(farm_list, file_path_template):
    """
    Estimates the total number of JSON records that would be generated
    from all farm LSF files (1 record per user inside user groups).

    Parameters
    ----------
    farm_list : List[str]
        List of farm names to check.

    file_path_template : str
        Template path to lsb.users file with '{farm}' placeholder.

    Returns
    -------
    int
        Estimated number of user-level JSON records.
    """
    total_users = 0

    for farm in farm_list:
        path = file_path_template.replace("{farm}", farm)

        if not os.path.isfile(path):
            print(f"[INFO][{farm}] File not found: {path}. Skipping.")
            continue

        try:
            with open(path, "r") as f:
                lines = [line.strip() for line in f if line.strip()]
        except Exception as e:
            print(f"[ERROR][{farm}] Failed to read file: {e}")
            continue

        in_group = False
        process_next = False

        for line in lines:
            if line.startswith("#"):
                continue
            if "Begin UserGroup" in line:
                in_group = True
                process_next = False
                continue
            if "End UserGroup" in line:
                in_group = False
                continue
            if in_group and "GROUP_NAME" in line and "GROUP_MEMBER" in line and "USER_SHARES" in line:
                process_next = True
                continue
            if process_next and not line.startswith("#"):
                match = re.search(r"\(([^)]+)\)", line)
                if match:
                    users = match.group(1).split()
                    total_users += len(users)

    print(f"[ESTIMATE] JSON records that will be generated: {total_users}")
    return total_users
