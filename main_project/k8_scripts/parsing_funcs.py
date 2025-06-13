import re

def parse_usergroup_block_lines(collected_lines, farm_name):
    """
    Parses raw lines from a UserGroup block and extracts user fairshare records.

    Parameters
    ----------
    collected_lines : List[str]
        Raw lines from a single UserGroup block (including header and data).
    farm_name : str
        Name of the farm this data belongs to.

    Returns
    -------
    List[Dict]
        Parsed user-level records from the block. Each record is a dict:
        {
            "farm": <farm_name>,
            "group": <group_name>,
            "user_name": <user>,
            "fairshare": <int>
        }
    """
    parsed_records = []
    in_group_block = False
    process_data_lines = False

    for i, line in enumerate(collected_lines):
        line = line.strip()
        print(f"[DEBUG][{farm_name}] Line {i}: {line}")

        if "Begin UserGroup" in line:
            in_group_block = True
            print(f"[INFO][{farm_name}] >>> Start of UserGroup block")
            continue

        if "End UserGroup" in line:
            print(f"[INFO][{farm_name}] >>> End of UserGroup block")
            break

        if in_group_block and "GROUP_NAME" in line and "GROUP_MEMBER" in line and "USER_SHARES" in line:
            process_data_lines = True
            print(f"[INFO][{farm_name}] >>> Detected header line")
            continue

        if process_data_lines:
            # Remove any trailing comments
            cleaned = line.split("#")[0].strip()
            parts = re.split(r"\s+", cleaned)
            if len(parts) < 3:
                print(f"[WARN][{farm_name}] Skipping malformed line: {line}")
                continue

            group_name = parts[0]
            members_raw = parts[1]
            shares_raw = parts[2]

            members = re.findall(r'\w+', members_raw)
            shares = re.findall(r'\[([^\]]+)\]', shares_raw)
            share_dict = {}

            for s in shares:
                try:
                    user, val = s.split(",")
                    share_dict[user.strip()] = int(val.strip())
                except Exception as e:
                    print(f"[WARN][{farm_name}] Skipping malformed share '{s}': {e}")
                    continue

            for user in members:
                fairshare = share_dict.get(user)
                if fairshare is not None:
                    record = {
                        "farm": farm_name,
                        "group": group_name,
                        "user_name": user,
                        "fairshare": fairshare
                    }
                    parsed_records.append(record)
                    print(f"[DEBUG][{farm_name}] Parsed record: {record}")
                else:
                    print(f"[WARN][{farm_name}] Fairshare not found for user: {user}")

    print(f"[INFO][{farm_name}] Total parsed records: {len(parsed_records)}")
    return parsed_records
