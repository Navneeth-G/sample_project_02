import os
import re
from typing import List

def estimate_total_json_records(farm_list: List[str], file_path_template: str) -> int:
    """
    estimate_total_json_records

    Estimates the total number of user-level JSON records that would be generated
    from parsing the LSF user group files for a given list of farms.

    ----------------------------------------------------------------------------------------

    Purpose
    -------
    This function is used for a **dry run** before parsing and uploading. It helps estimate
    how many user-level records would be created — one per user — across all farm files.

    ----------------------------------------------------------------------------------------

    Inputs
    ------
    - farm_list : List[str]
        A list of farm/environment names to evaluate.

    - file_path_template : str
        A string template where `{farm}` will be replaced by each farm name.
        Example: "/global/lsf/cells/{farm}/conf/lsbatch/{farm}/configdir/lsb.users"

    ----------------------------------------------------------------------------------------

    Expected Input File Format (for each farm)
    ------------------------------------------
    Begin UserGroup
    GROUP_NAME      GROUP_MEMBER                  USER_SHARES
    prod_users      (alice bob carol)             [alice, 10][bob, 20][carol, 30]
    dev_users       (dave eve)                    [default, 5]
    End UserGroup

    The users inside the parentheses (GROUP_MEMBER) are extracted and counted.

    ----------------------------------------------------------------------------------------

    Output
    ------
    - Console debug/INFO logs for each farm
    - Final total number of estimated JSON records printed
    - Returns an `int`: the sum total of all users that would become JSON records

    ----------------------------------------------------------------------------------------

    Example Call
    ------------
    farm_list = ["us01_swe", "tcad", "pythia"]
    file_path_template = "/global/lsf/cells/{farm}/conf/lsbatch/{farm}/configdir/lsb.users"

    estimate_total_json_records(farm_list, file_path_template)

    ----------------------------------------------------------------------------------------
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
