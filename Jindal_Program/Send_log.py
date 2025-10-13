import os
import time
import json
import threading
import requests
import logging
from logging.handlers import TimedRotatingFileHandler
from queue import Queue, Empty

# -*- coding: utf-8 -*-

# ------------------ Config ------------------
API_URL = "http://104.154.22.115:8001/app/v0.0.1/plc/raw-post"
BASE_DIR = r"D:\AI_PROGRAMS\JSON"      # JSON files folder
LOG_DIR = r"D:\AI_PROGRAMS\PLC_LOGS"       # Logs folder
DB_NAME = "BALAJI_EMS_NEW"                 # DB name for logs

CHECK_INTERVAL = 0.30
WORKERS = 3
QUEUE_MAXSIZE = 2000
MAX_RETRIES = 50
RETRY_DELAY = 2

send_queue = Queue(maxsize=QUEUE_MAXSIZE)
last_sent_mtime = 0.0
stop_event = threading.Event()

# ------------------ Logging Setup ------------------
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

LOG_FILE = os.path.join(LOG_DIR, "plc_sender.log")

logger = logging.getLogger("plc_sender")
logger.setLevel(logging.INFO)

# Rotate logs daily at midnight, keep last 30 logs
file_handler = TimedRotatingFileHandler(
    LOG_FILE, when="midnight", interval=1, backupCount=30, encoding="utf-8", utc=False
)

# Add suffix for rotated files
file_handler.suffix = "%Y-%m-%d"

# Rename rotated logs → plc_sender_YYYY-MM-DD.log
def custom_namer(default_name):
    # default_name looks like: plc_sender.log.YYYY-MM-DD
    base, ext = os.path.splitext(default_name)
    parts = base.split(".")
    if len(parts) > 1:
        date_part = parts[-1]  # last part after ".log"
        return f"{parts[0]}_{date_part}.log"
    return default_name

file_handler.namer = custom_namer

console_handler = logging.StreamHandler()

formatter = logging.Formatter("%(asctime)s [%(levelname)s] [DB:%(db)s] %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Adapter to inject DB name
class DBLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        if "extra" not in kwargs:
            kwargs["extra"] = {}
        kwargs["extra"]["db"] = DB_NAME
        return msg, kwargs

logger = DBLoggerAdapter(logger, {"db": DB_NAME})
# ---------------------------------------------------


def send_worker():
    session = requests.Session()
    while not stop_event.is_set():
        try:
            path = send_queue.get(timeout=1.0)
        except Empty:
            continue

        if path is None:
            send_queue.task_done()
            break

        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                resp = session.post(API_URL, json=data, timeout=5)
                resp.raise_for_status()

                msg = (
                    f"✅ Sent {os.path.basename(path)} (try {attempt}) "
                    f"Status: {resp.status_code} API Response: {resp.text[:200]}"
                )
                logger.info(msg)
                success = True
                break

            except Exception as e:
                logger.warning(
                    f"⚠️ Error sending {os.path.basename(path)} "
                    f"(try {attempt}/{MAX_RETRIES}): {e}"
                )
                time.sleep(RETRY_DELAY)

            finally:
                try:
                    del data
                except Exception:
                    pass

        if not success:
            logger.error(
                f"❌ Giving up on {os.path.basename(path)} after {MAX_RETRIES} retries"
            )

        send_queue.task_done()


def scan_latest_json(folder: str):
    latest_path = None
    latest_mtime = 0.0
    try:
        with os.scandir(folder) as it:
            for entry in it:
                if not entry.is_file():
                    continue
                if not entry.name.endswith(".json"):
                    continue
                try:
                    st = entry.stat()
                except FileNotFoundError:
                    continue
                mtime = st.st_mtime
                if mtime > latest_mtime:
                    latest_mtime = mtime
                    latest_path = entry.path
    except (FileNotFoundError, PermissionError):
        return None, 0.0
    return latest_path, latest_mtime


def monitor_folder():
    global last_sent_mtime
    while not stop_event.is_set():
        try:
            latest_path, latest_mtime = scan_latest_json(BASE_DIR)
            if latest_path and latest_mtime > last_sent_mtime:
                if os.path.exists(latest_path):
                    try:
                        send_queue.put(latest_path, timeout=1.0)
                        last_sent_mtime = latest_mtime
                        logger.info(
                            f"📂 Queued {os.path.basename(latest_path)} for sending | "
                            f"Queue size: {send_queue.qsize()}"
                        )
                    except Exception:
                        pass

        except Exception as e:
            logger.error(f"Error in monitor_folder: {e}")
        time.sleep(CHECK_INTERVAL)


def main():
    threads = []
    for _ in range(WORKERS):
        t = threading.Thread(target=send_worker, daemon=True)
        t.start()
        threads.append(t)

    try:
        monitor_folder()
    except KeyboardInterrupt:
        logger.info("Stopping... (KeyboardInterrupt)")
    finally:
        stop_event.set()
        for _ in range(WORKERS):
            try:
                send_queue.put_nowait(None)
            except Exception:
                pass

        try:
            send_queue.join()
        except Exception:
            pass

        for t in threads:
            t.join(timeout=2.0)

        logger.info("✅ Clean shutdown.")


if __name__ == "__main__":
    main()
