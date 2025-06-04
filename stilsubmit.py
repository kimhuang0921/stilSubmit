import csv
import getpass
import os
import sys
import fcntl
from datetime import datetime


QUEUE_FILE = "/work/kimhuang/1_Python/8_stilManager/task_queue.csv"


def generate_batch_id(input_csv):
    base = os.path.splitext(os.path.basename(input_csv))[0]
    timestamp = datetime.now().strftime("%y%m%d_%H%M")
    return f"{base}_{timestamp}"

def validate_and_append(input_csv, queue_file=QUEUE_FILE):
    if not os.path.isfile(input_csv):
        print(f"[ERROR] CSV file not found: {input_csv}")
        sys.exit(1)

    user = getpass.getuser()
    email = f"{user}@rivosinc.com"
    batch_id = generate_batch_id(input_csv)

    with open(input_csv, newline='') as f:
        reader = csv.DictReader(f)
        tasks = list(reader)

    if "STIL_Path" not in reader.fieldnames:
        print("[ERROR] Input CSV must contain header: STIL_Path")
        sys.exit(1)

    for row in tasks:
        path = row.get("STIL_Path", "").strip()
        if not path:
            print("[ERROR] Missing STIL file path")
            print("[ABORT] Submit failed.")
            return
        # 檢查是否為絕對路徑
        if not os.path.isabs(path):
            print(f"[ERROR] STIL_Path must be an absolute path: {path}")
            print("[ABORT] Submit failed.")
            return
        # 檢查檔案是否存在於指定絕對路徑
        if not os.path.isfile(path):
            print(f"[ERROR] STIL file not found at specified path: {path}")
            print("[ABORT] Submit failed.")
            return
        # 確保檔案的真實路徑與指定路徑一致
        if os.path.realpath(path) != os.path.abspath(path):
            print(f"[ERROR] STIL file path does not match its actual location: {path}")
            print("[ABORT] Submit failed.")
            return

    try:
        file_exists = os.path.exists(queue_file)
        with open(queue_file, "a+", newline='') as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            writer = csv.writer(f)
            # If the file is empty, write header first
            if not file_exists or os.stat(queue_file).st_size == 0:
                writer.writerow(["Timestamp", "SubmittedBy", "Email", "BatchID", "STIL_Path", "Status"])
            for row in tasks:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                writer.writerow([now, user, email, batch_id, row["STIL_Path"], "PENDING"])
            fcntl.flock(f, fcntl.LOCK_UN)
        print(f"[INFO] Submit successful. BatchID: {batch_id}")
        print(f"[INFO] Added {len(tasks)} task(s).")
    except Exception as e:
        print(f"[ERROR] Failed to write to queue: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 submit_tool.py <input_csv>")
        sys.exit(1)

    validate_and_append(sys.argv[1])
