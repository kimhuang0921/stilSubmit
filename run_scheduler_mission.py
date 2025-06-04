import csv
import os
import fcntl
import subprocess
import json
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from collections import defaultdict
import time
from pathlib import Path

# === CONFIGURATION ===
BASE_DIR = "/work/kimhuang/1_Python/8_stilManager"
CONFIG_FILE = os.path.join(BASE_DIR, "repack_config.json")
QUEUE_FILE = os.path.join(BASE_DIR, "task_queue.csv")
EXECUTION_LOG_FILE = os.path.join(BASE_DIR, "execution_log.csv")
LOCK_FILE = "/tmp/mission_scheduler.lock"
LOG_DIR = os.path.join(BASE_DIR, "logs")
OUTPUT_DIR = "/projects/ga0/patterns/release_pattern/"
SETUP_FILE = "/work/kimhuang/1_Python/8_stilManager/smt8p7_setup.py"

def load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)

def send_email(sender_email, sender_password, to_email, subject, body):
    msg = MIMEText(body)
    msg["From"] = sender_email
    msg["To"] = to_email
    msg["Subject"] = subject

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)

def extract_file_base(filepath: str) -> str:
    """Extract base filename without extensions."""
    name = Path(filepath.strip()).name
    stem = Path(name).stem
    while "." in stem:
        stem = Path(stem).stem
    return stem.rstrip("_")

def check_pattern_folder_exists(pattern_name: str, check_dir: str = "/projects/ga0/patterns/release_pattern/src") -> bool:
    """檢查指定目錄中是否存在與 pattern_name 同名的資料夾"""
    folder_path = os.path.join(check_dir, pattern_name)
    return os.path.isdir(folder_path)

def run_stil_command(stil_path, batch_id, log_path):
    project_name = extract_file_base(stil_path)

    cmd = (
        f"srun -p hw-h --mem=64G --cpus-per-task=2 bash -c '"
        f"source /etc/profile && "
        f"module load tdl && "
        f"ategen "
        f"-input_file_type:STIL "
        f"-workdir:{OUTPUT_DIR} "
        f"-project_name:{project_name} "
        f"-logfile:{log_path} "
        f"-setup:{SETUP_FILE} "
        f"-licwait "
        f"-timestamp "
        f"{stil_path}'"
    )

    try:
        start_time = datetime.now()
        start_sec = time.time()

        result = subprocess.run(
            ["bash", "-c", cmd],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )

        end_sec = time.time()
        end_time = datetime.now()
        duration = round(end_sec - start_sec, 2)
        output = result.stdout + "\n" + result.stderr

        return result.returncode == 0, output, start_time, end_time, duration

    except Exception as e:
        now = datetime.now()
        return False, str(e), now, now, 0.0

def group_tasks_by_batch(tasks):
    batches = defaultdict(list)
    for row in tasks:
        batches[row[3]].append(row)
    return batches

def log_execution(start_time, end_time, batch_id, stil_path, duration, status):
    file_exists = os.path.exists(EXECUTION_LOG_FILE)
    with open(EXECUTION_LOG_FILE, "a", newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["StartTime", "EndTime", "BatchID", "STIL_Path", "Duration_sec", "Status"])
        writer.writerow([
            start_time.strftime("%Y-%m-%d %H:%M:%S"),
            end_time.strftime("%Y-%m-%d %H:%M:%S"),
            batch_id,
            stil_path,
            duration,
            status
        ])

def process_first_pending_task():
    config = load_config()
    email_config = config["email"]
    sender_email = email_config["from"]
    sender_password = email_config["password"]

    os.makedirs(LOG_DIR, exist_ok=True)

    with open(QUEUE_FILE, "r+", newline='') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        reader = list(csv.reader(f))
        header = reader[0]
        tasks = reader[1:]

        pending_tasks = [row for row in tasks if row[5] == "PENDING"]
        if not pending_tasks:
            print("[INFO] No pending tasks.")
            return

        batches = group_tasks_by_batch(tasks)
        first_batch = None
        for batch_id, rows in batches.items():
            if any(row[5] == "PENDING" for row in rows):
                first_batch = batch_id
                break

        if not first_batch:
            print("[INFO] No pending batches found.")
            return

        batch_tasks = [row for row in tasks if row[3] == first_batch]
        pending_in_batch = [row for row in batch_tasks if row[5] == "PENDING"]
        current_task = pending_in_batch[0]
        timestamp, submitted_by, submitter_email, batch_id, stil_path, status = current_task

        # 檢查 pattern 資料夾是否存在
        pattern_name = extract_file_base(stil_path)
        if not check_pattern_folder_exists(pattern_name):
            print(f"[INFO] Pattern folder {pattern_name} not found in /projects/ga0/patterns/release_pattern/src. Skipping task.")
            for row in tasks:
                if row == current_task:
                    row[5] = "SKIPPED"
                    break
            f.seek(0)
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(tasks)
            f.truncate()
            fcntl.flock(f, fcntl.LOCK_UN)
            return

        log_filename = os.path.join(LOG_DIR, f"{batch_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        print(f"[EXECUTE] Running ategen on {stil_path}")

        success, output, start_time, end_time, duration = run_stil_command(stil_path, batch_id, log_filename)

        with open(log_filename, "a") as log:
            if success:
                log.write(f"[{datetime.now()}] SUCCESS:\n{output}\n")
                result = "COMPLETE"
            else:
                log.write(f"[{datetime.now()}] FAILED:\n{output}\n")
                result = "FAILED"

        for row in tasks:
            if row == current_task:
                row[5] = result
                break

        f.seek(0)
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(tasks)
        f.truncate()

        log_execution(start_time, end_time, batch_id, stil_path, duration, result)

        updated_batch_tasks = [row for row in tasks if row[3] == batch_id]
        if all(row[5] in ("COMPLETE", "FAILED", "SKIPPED") for row in updated_batch_tasks):
            summary = "\n".join([f"{row[4]}  -->  {row[5]}" for row in updated_batch_tasks])
            passed = sum(1 for row in updated_batch_tasks if row[5] == "COMPLETE")
            failed = sum(1 for row in updated_batch_tasks if row[5] == "FAILED")
            skipped = sum(1 for row in updated_batch_tasks if row[5] == "SKIPPED")
            status_tag = "PASS" if failed == 0 else "FAIL"
            subject = f"[{status_tag}] Pattern Release : {batch_id}"
            body = (
                f"Batch: {batch_id}\n\n"
                f"Summary:\n{summary}\n\n"
                f"Result: {passed} Passed, {failed} Failed, {skipped} Skipped"
            )
            try:
                send_email(sender_email, sender_password, submitter_email, subject, body)
            except Exception as e:
                print(f"[WARN] Email not sent: {e}")

        fcntl.flock(f, fcntl.LOCK_UN)

if __name__ == "__main__":
    if os.path.exists(LOCK_FILE):
        print("[INFO] Existing instance is running.")
    else:
        try:
            with open(LOCK_FILE, "x") as lockfile:
                try:
                    process_first_pending_task()
                except Exception as e:
                    print(f"[ERROR WARN] Task execution failed: {e}")
                finally:
                    os.remove(LOCK_FILE)
        except FileExistsError:
            print("[INFO] Existing instance is running.")
