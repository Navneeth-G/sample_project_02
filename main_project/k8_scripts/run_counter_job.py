import os
import re
from typing import List
import pendulum
from k8_scripts.block_identify import extract_first_usergroup_block  # Update this import path if needed

def estimate_total_json_records(farm_list, file_path_template):
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




    total_records = 0
    ts = pendulum.now(timezone).to_iso8601_string()

    for farm in farm_list:
        file_path = file_path_template.replace("{farm}", farm)
        print(f"\n[INFO][{farm}] Checking file: {file_path}")

        lines = extract_first_usergroup_block(file_path)
        if not lines:
            print(f"[INFO][{farm}] Skipping — no valid UserGroup block found.")
            continue

        record_count = 0
        header_found = False

        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if "GROUP_NAME" in line and "GROUP_MEMBER" in line and "USER_SHARES" in line:
                header_found = True
                continue

            if not header_found:
                continue

            parts = re.split(r"\s+", line.split("#")[0].strip())
            if len(parts) < 3:
                continue

            member_raw = parts[1]
            members = re.findall(r"\w+", member_raw)
            record_count += len(members)

        print(f"[INFO][{farm}] Estimated JSON records: {record_count}")
        total_records += record_count

    print(f"\n[SUMMARY] Total estimated JSON records across all farms: {total_records}")
    return total_records
