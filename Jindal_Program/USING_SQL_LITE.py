import os
import csv
import json
import time
import shutil
import snap7
import gc
import ctypes
import threading
import sqlite3
import requests
from datetime import datetime

# =========================
# Config
# =========================
BASE_DIR = "X:/PLC_JSON"
KEEP_MINUTES = 5
PLC_IP = "172.16.12.40"
PLC_RACK = 0
PLC_SLOT = 5
DB_NUMBER = 199
DB_SIZE = 358
CSV_FILE = "DB199.csv"
TARGET_MS = 20
API_URL = "http://104.154.22.115:8001/app/v0.0.1/plc/raw-post"

# =========================
# SQLite Setup
# =========================
DB_FILE = "plc_queue.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS plc_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            payload TEXT
        )
    """)
    conn.commit()
    conn.close()

def insert_data(payload: dict):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT INTO plc_queue (timestamp, payload) VALUES (?, ?)",
              (datetime.now().isoformat(), json.dumps(payload)))
    conn.commit()
    conn.close()

def fetch_batch(limit=50):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, payload FROM plc_queue ORDER BY id LIMIT ?", (limit,))
    rows = c.fetchall()
    conn.close()
    return rows

def delete_ids(ids):
    if not ids:
        return
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.executemany("DELETE FROM plc_queue WHERE id=?", [(i,) for i in ids])
    conn.commit()
    conn.close()

# =========================
# API Sender Thread
# =========================
def sender_thread():
    while True:
        rows = fetch_batch(100)  # send up to 100 rows each cycle
        if not rows:
            time.sleep(0.25)  # no data, sleep 250ms
            continue

        ids, payloads = zip(*rows)
        try:
            payload_list = [json.loads(p) for p in payloads]
            r = requests.post(API_URL, json=payload_list, timeout=5)
            if r.status_code == 200:
                delete_ids(ids)
                print(f"[API OK] Sent {len(ids)} rows")
            else:
                print(f"[API FAIL] Status {r.status_code}")
                time.sleep(1)
        except Exception as e:
            print(f"[API ERROR] {e}")
            time.sleep(1)

# =========================
# PLC Reading Loop
# =========================
def plc_reader():
    client = snap7.client.Client()
    client.connect(PLC_IP, PLC_RACK, PLC_SLOT)

    while True:
        start_time = time.time()

        try:
            data = client.db_read(DB_NUMBER, 0, DB_SIZE)
            payload = {
                "ts": datetime.now().isoformat(),
                "data": list(data)  # example: raw bytes
            }
            insert_data(payload)

        except Exception as e:
            print(f"[PLC ERROR] {e}")
            time.sleep(1)

        elapsed = (time.time() - start_time) * 1000
        if elapsed < TARGET_MS:
            time.sleep((TARGET_MS - elapsed) / 1000)

# =========================
# Main
# =========================
if __name__ == "__main__":
    init_db()

    # Start API sender in background
    threading.Thread(target=sender_thread, daemon=True).start()

    # Start PLC reader loop
    plc_reader()
