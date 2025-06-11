import csv
import getpass
import os
import sys
import fcntl
from datetime import datetime

QUEUE_FILE = "/work/kimhuang/1_Python/8_stilManager/task_queue.csv"
VALID_XMODES = ["", "4"]  # 定義有效 xmode 值

def generate_batch_id(input_csv):
    base = os.path.splitext(os.path.basename(input_csv))[0]
    timestamp = datetime.now().strftime("%y%m%d_%H%M")
    return f"{base}_{timestamp}"

def validate_and_append(input_csv, xmode="", queue_file=QUEUE_FILE, debug=False):
    if not os.path.isfile(input_csv):
        print(f"[ERROR] CSV file not found: {input_csv}")
        sys.exit(1)

    # 驗證 xmode
    if xmode not in VALID_XMODES:
        print(f"[ERROR] Invalid xmode: {xmode}. Must be one of {VALID_XMODES}")
        sys.exit(1)

    user = getpass.getuser()
    email = f"{user}@rivosinc.com"
    batch_id = generate_batch_id(input_csv)

    if debug:
        print(f"[DEBUG] Opening input CSV: {input_csv}")
        print(f"[DEBUG] User: {user}, Email: {email}, BatchID: {batch_id}, XMode: {xmode}")

    with open(input_csv, newline='') as f:
        reader = csv.DictReader(f)
        tasks = list(reader)
        if debug:
            print(f"[DEBUG] CSV headers: {reader.fieldnames}")
            print(f"[DEBUG] Loaded {len(tasks)} task(s) from CSV")

    if "STIL_Path" not in reader.fieldnames:
        print("[ERROR] Input CSV must contain header: STIL_Path")
        sys.exit(1)

    for idx, row in enumerate(tasks):
        if debug:
            print(f"[DEBUG] Checking task {idx+1}: {row}")
        path = row.get("STIL_Path", "").strip()
        if not path:
            print("[ERROR] Missing STIL file path")
            print("[ABORT] Submit failed.")
            return
        if not os.path.isabs(path):
            print(f"[ERROR] STIL_Path must be an absolute path: {path}")
            print("[ABORT] Submit failed.")
            return
        if not os.path.isfile(path):
            print(f"[ERROR] STIL file not found at specified path: {path}")
            print("[ABORT] Submit failed.")
            return
        if os.path.realpath(path) != os.path.abspath(path):
            print(f"[ERROR] STIL file path does not match its actual location: {path}")
            print("[ABORT] Submit failed.")
            return

    try:
        file_exists = os.path.exists(queue_file)
        with open(queue_file, "a+", newline='') as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            writer = csv.writer(f)
            if not file_exists or os.stat(queue_file).st_size == 0:
                writer.writerow(["Timestamp", "SubmittedBy", "Email", "BatchID", "STIL_Path", "XMode", "Status"])
            for row in tasks:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                writer.writerow([now, user, email, batch_id, row["STIL_Path"], xmode, "PENDING"])
            fcntl.flock(f, fcntl.LOCK_UN)
        print(f"[INFO] Submit successful. BatchID: {batch_id}")
        print(f"[INFO] Added {len(tasks)} task(s).")
    except Exception as e:
        print(f"[ERROR] Failed to write to queue: {e}")
        sys.exit(1)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv", help="CSV file with STIL paths")
    parser.add_argument("--xmode", help="Specify xmode (e.g. 4)", default="")
    parser.add_argument("--debug", action="store_true", help="Enable debug logs")
    args = parser.parse_args()

    validate_and_append(args.input_csv, xmode=args.xmode, debug=args.debug)
