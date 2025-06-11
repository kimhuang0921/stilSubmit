import subprocess
import getpass
import csv
import os
import fcntl
import json
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from collections import defaultdict
import time
from pathlib import Path

# === CONFIGURATION ===
MAX_LICENSE = 1
BASE_DIR = "/work/kimhuang/1_Python/8_stilManager"
CONFIG_FILE = os.path.join(BASE_DIR, "repack_config.json")
QUEUE_FILE = os.path.join(BASE_DIR, "task_queue.csv")
EXECUTION_LOG_FILE = os.path.join(BASE_DIR, "execution_log.csv")
LOCK_FILE = "/tmp/mission_scheduler.lock"
LOG_DIR = os.path.join(BASE_DIR, "logs")
OUTPUT_DIR = "/projects/ga0/patterns/release_pattern"
SETUP_FILE = "/work/kimhuang/1_Python/8_stilManager/smt8p7_setup.py"
SETUP_X4_FILE = "/work/kimhuang/1_Python/8_stilManager/smt8p7_setupx4.py"
SIZE_THRESHOLD = 20 * 1024 * 1024  # 20MB in bytes
VALID_XMODES = ["", "4"]  # 定義有效 xmode 值

def skip_if_too_many_jobs(debug=False):
    user = getpass.getuser()
    try:
        jobs = subprocess.check_output(f"squeue -u {user} -h -o '%j'", shell=True).decode().splitlines()
        current = sum(1 for job in jobs if "ategen" in job)
        if debug:
            print(f"[DEBUG] Current ategen jobs: {current}/{MAX_LICENSE}")
        if current >= MAX_LICENSE:
            print(f"[INFO] {current} ategen job(s) running. Skipping this cycle.")
            exit(0)
        else:
            print(f"[INFO] Current ategen jobs: {current}/{MAX_LICENSE}. Proceeding.")
    except Exception as e:
        print(f"[ERROR] Failed to check squeue: {e}")

def load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)

def send_email(sender_email, sender_password, to_email, subject, body, debug=False):
    msg = MIMEText(body)
    msg["From"] = sender_email
    msg["To"] = to_email
    msg["Subject"] = subject
    if debug:
        print(f"[DEBUG] Sending email to {to_email} with subject: {subject}")
        print(f"[DEBUG] Email body:\n{body}")
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        if debug:
            print(f"[DEBUG] Email sent successfully")
    except Exception as e:
        print(f"[WARN] Email not sent: {e}")

def extract_file_base(filepath: str) -> str:
    name = Path(filepath.strip()).name
    stem = Path(name).stem
    while "." in stem:
        stem = Path(stem).stem
    return stem.rstrip("_")

def run_stil_command(stil_path, batch_id, log_path, xmode="", debug=False):
    project_name = extract_file_base(stil_path)
    
    if xmode not in VALID_XMODES:
        error_msg = f"[ERROR] Invalid xmode: {xmode}. Must be one of {VALID_XMODES}"
        print(error_msg)
        return False, error_msg, datetime.now(), datetime.now(), 0.0

    setup_file = SETUP_X4_FILE if xmode == "4" else SETUP_FILE
    if debug:
        print(f"[DEBUG] Using setup file: {setup_file} for xmode: {xmode}")

    ategen_cmd = (
        f"ategen "
        f"-input_file_type:STIL "
        f"-workdir:{OUTPUT_DIR} "
        f"-project_name:{project_name} "
        f"-logfile:{log_path} "
        f"-setup:{setup_file} "
        f"-licwait "
        f"-timestamp "
        f"{stil_path}"
    )

    try:
        file_size = os.path.getsize(stil_path)
        if debug:
            print(f"[DEBUG] STIL file size: {file_size / 1024 / 1024:.2f}MB")
        if file_size < SIZE_THRESHOLD:
            print(f"[INFO] File {stil_path} ({file_size / 1024 / 1024:.2f}MB) is < 20MB, running locally.")
            cmd = (
                f"bash -c '"
                f"source /etc/profile && "
                f"module load tdl && "
                f"{ategen_cmd}'"
            )
        else:
            print(f"[INFO] File {stil_path} ({file_size / 1024 / 1024:.2f}MB) is >= 20MB, running on Slurm.")
            cmd = (
                f"srun -p hw-h --mem=32G --cpus-per-task=2 bash -c '"
                f"source /etc/profile && "
                f"module load tdl && "
                f"{ategen_cmd}'"
            )

        if debug:
            print(f"[DEBUG] Executing command: {cmd}")

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
        if debug:
            print(f"[DEBUG] Command output:\n{output}")
            print(f"[DEBUG] Execution duration: {duration}s")
        return result.returncode == 0, output, start_time, end_time, duration
    except Exception as e:
        now = datetime.now()
        print(f"[ERROR] Command execution failed: {e}")
        return False, str(e), now, now, 0.0

def group_tasks_by_batch(tasks, debug=False):
    batches = defaultdict(list)
    for row in tasks:
        batches[row[3]].append(row)
    if debug:
        print(f"[DEBUG] Grouped {len(tasks)} tasks into {len(batches)} batches")
        for batch_id, batch_tasks in batches.items():
            print(f"[DEBUG] Batch {batch_id}: {len(batch_tasks)} tasks")
    return batches

def log_execution(start_time, end_time, batch_id, stil_path, duration, status, debug=False):
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
    if debug:
        print(f"[DEBUG] Logged execution: BatchID={batch_id}, STIL_Path={stil_path}, Status={status}")

def process_first_pending_task(debug=False):
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
        pending_tasks = [row for row in tasks if len(row) >= 7 and row[6] == "PENDING"]
        if debug:
            print(f"[DEBUG] Loaded {len(tasks)} tasks, {len(pending_tasks)} pending")
        if not pending_tasks:
            print("[INFO] No pending tasks or invalid task format.")
            fcntl.flock(f, fcntl.LOCK_UN)
            return

        batches = group_tasks_by_batch(tasks, debug)
        first_batch = next((batch_id for batch_id, rows in batches.items() if any(len(row) >= 7 and row[6] == "PENDING" for row in rows)), None)
        if not first_batch:
            print("[INFO] No pending batches found.")
            fcntl.flock(f, fcntl.LOCK_UN)
            return

        batch_tasks = [row for row in tasks if row[3] == first_batch]
        current_task = next(row for row in batch_tasks if len(row) >= 7 and row[6] == "PENDING")
        
        if len(current_task) < 7:
            print(f"[ERROR] Invalid task format: {current_task}. Skipping.")
            fcntl.flock(f, fcntl.LOCK_UN)
            return

        timestamp, submitted_by, submitter_email, batch_id, stil_path, xmode, status = current_task
        if debug:
            print(f"[DEBUG] Processing task: BatchID={batch_id}, STIL_Path={stil_path}, XMode={xmode}")

        for row in tasks:
            if row == current_task:
                row[6] = "RUNNING"
                break

        f.seek(0)
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(tasks)
        f.truncate()
        fcntl.flock(f, fcntl.LOCK_UN)

    log_filename = os.path.join(LOG_DIR, f"{batch_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    print(f"[EXECUTE] Running ategen on {stil_path}")
    success, output, start_time, end_time, duration = run_stil_command(stil_path, batch_id, log_filename, xmode, debug)

    with open(QUEUE_FILE, "r+", newline='') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        reader = list(csv.reader(f))
        header = reader[0]
        tasks = reader[1:]
        for row in tasks:
            if row[4] == stil_path and row[3] == batch_id:
                row[6] = "COMPLETE" if success else "FAILED"
                break
        f.seek(0)
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(tasks)
        f.truncate()
        fcntl.flock(f, fcntl.LOCK_UN)

    with open(log_filename, "a") as log:
        log.write(f"[{datetime.now()}] {'SUCCESS' if success else 'FAILED'}:\n{output}\n")
    log_execution(start_time, end_time, batch_id, stil_path, duration, "COMPLETE" if success else "FAILED", debug)

    with open(QUEUE_FILE, "r", newline='') as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        reader = list(csv.reader(f))
        tasks = reader[1:]
        batch_tasks = [row for row in tasks if row[3] == batch_id]
        fcntl.flock(f, fcntl.LOCK_UN)

    if all(row[6] in ("COMPLETE", "FAILED") for row in batch_tasks):
        summary = "\n".join([f"{row[4]}  -->  {row[6]}" for row in batch_tasks])
        passed = sum(1 for row in batch_tasks if row[6] == "COMPLETE")
        failed = sum(1 for row in batch_tasks if row[6] == "FAILED")
        status_tag = "PASS" if failed == 0 else "FAIL"
        subject = f"[{status_tag}] Pattern Release : {batch_id}"
        body = f"Batch: {batch_id}\n\nSummary:\n{summary}\n\nResult: {passed} Passed, {failed} Failed"
        send_email(sender_email, sender_password, submitter_email, subject, body, debug)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Enable debug logs")
    args = parser.parse_args()

    try:
        skip_if_too_many_jobs(args.debug)
        with open(LOCK_FILE, "x") as lockfile:
            try:
                process_first_pending_task(args.debug)
            except Exception as e:
                print(f"[ERROR] Task execution failed: {e}")
            finally:
                os.remove(LOCK_FILE)
    except FileExistsError:
        print("[INFO] Another instance is running.")
