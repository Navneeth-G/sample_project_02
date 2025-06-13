import os

def extract_first_usergroup_block(file_path):
    """
    Extracts the first UserGroup block from the file as raw lines.

    Parameters
    ----------
    file_path : str
        Path to the lsb.users file.

    Returns
    -------
    List[str]
        Raw lines from the first UserGroup block, including header and data.
    """
    if not os.path.isfile(file_path):
        print(f"[ERROR] File not found: {file_path}")
        return []

    print(f"[INFO] Reading file: {file_path}")

    inside_block = False
    collected_lines = []

    try:
        with open(file_path, "r") as f:
            for i, line in enumerate(f):
                stripped_line = line.rstrip("\n")
                print(f"[DEBUG] Line {i}: {stripped_line}")

                if "Begin UserGroup" in stripped_line:
                    inside_block = True
                    print(f"[INFO] >>> Found 'Begin UserGroup' at line {i}")
                    collected_lines.append(stripped_line)
                    continue

                if inside_block:
                    collected_lines.append(stripped_line)
                    print(f"[DEBUG] >>> Collected line: {stripped_line}")

                    if "End UserGroup" in stripped_line:
                        print(f"[INFO] >>> Found 'End UserGroup' at line {i}")
                        break

        if not collected_lines:
            print("[WARN] No UserGroup block found.")
        else:
            print("\n[INFO] Completed collecting UserGroup block:")
            for cl in collected_lines:
                print(f"  {cl}")

    except Exception as e:
        print(f"[ERROR] Failed to read or process file: {e}")
        return []

    return collected_lines
