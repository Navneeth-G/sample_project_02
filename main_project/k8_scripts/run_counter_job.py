import os
import re
from typing import List

def estimate_total_json_records(farm_list: List[str], file_path_template: str) -> int:
    """
    Estimates the total number of user-level JSON records that would be generated
    by parsing all LSF user group configuration files from multiple farms.

    Parameters
    ----------
    farm_list : List[str]
        List of farm/environment names.

    file_path_template : str
        Path template where '{farm}' will be replaced with the actual farm name.
        Example: "/global/lsf/cells/{farm}/conf/lsbatch/{farm}/configdir/lsb.users"

    Returns
    -------
    int
        Total number of user-level records (each user = 1 record).
    """

    total_users = 0

    for farm in farm_list:
        path = file_path_template.replace("{farm}", farm)

        if not os.path.isfile(path):
            print(f"[INFO][{farm}] File not found: {path}. Skipping.")
            continue

        print(f"[INFO][{farm}] Reading file: {path}")

        try:
            with open(path, "r") as f:
                lines = [line.strip() for line in f if line.strip()]
        except Exception as e:
            print(f"[ERROR][{farm}] Failed to read file: {e}")
            continue

        in_group = False
        process_next = False
        user_count_this_farm = 0

        for i, line in enumerate(lines):
            print(f"[DEBUG][{farm}] Line {i}: {line}")

            if line.startswith("#"):
                continue
            if "Begin UserGroup" in line:
                in_group = True
                continue
            if "End UserGroup" in line:
                in_group = False
                process_next = False
                continue
            if in_group and "GROUP_NAME" in line and "GROUP_MEMBER" in line and "USER_SHARES" in line:
                process_next = True
                continue
            if process_next:
                match = re.search(r"\(([^)]+)\)", line)
                if match:
                    users = match.group(1).split()
                    user_count_this_farm += len(users)
                else:
                    print(f"[WARN][{farm}] Could not extract users from line: {line}")

        total_users += user_count_this_farm
        print(f"[INFO][{farm}] Estimated users: {user_count_this_farm}")

    print(f"[ESTIMATE] Total JSON records that would be generated: {total_users}")
    return total_users
