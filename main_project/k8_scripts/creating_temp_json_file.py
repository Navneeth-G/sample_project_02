import os
import json

def write_records_to_ndjson_file(records, output_dir, index_name, ts_str):
    """
    Writes records as NDJSON content into a `.json` file using a consistent timestamp string.

    Parameters
    ----------
    records : List[Dict]
        Parsed records to write.

    output_dir : str
        Directory to save the .json file.

    index_name : str
        Logical name used in the file name.

    ts_str : str
        Timestamp string (already formatted) to embed in the filename.
        Example: "2025-06-12T18-30-00"

    Returns
    -------
    str or None
        Full path to the written file or None if writing failed.
    """
    if not records:
        print("[INFO] No records to write. Skipping file generation.")
        return None

    filename = f"{index_name}_{ts_str}.json"
    output_path = os.path.join(output_dir, filename)

    try:
        os.makedirs(output_dir, exist_ok=True)
        with open(output_path, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        print(f"[INFO] Wrote {len(records)} records to: {output_path}")
        return output_path
    except Exception as e:
        print(f"[ERROR] Failed to write file: {e}")
        return None
