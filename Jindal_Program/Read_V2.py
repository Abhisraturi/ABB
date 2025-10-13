import os
import json
import struct
import snap7
import pandas as pd
import threading
import time
from datetime import datetime, timedelta
from queue import Queue


# ---------------- CONFIG ----------------
BASE_DIR = r"D:\AI_PROGRAMS\JSON"  # safer with raw string
PLC_IP = "172.16.12.40"
PLC_RACK = 0
PLC_SLOT = 5
DB_NUMBER = 199
DB_SIZE = 364
CSV_FILE = "DB199.csv"
TARGET_INTERVAL = 0.02  # 20 ms grid
SAMPLES_PER_FILE = 50   # 50 samples → 1 sec
# ----------------------------------------

# Load tags
df = pd.read_csv(CSV_FILE)
tags = df.to_dict("records")

# Precompute address info
for t in tags:
    dtype = t["Type"].lower()
    if dtype == "bool":
        addr = int(t["Address"])
        t["byte"], t["bit"] = divmod(addr, 8)
    else:
        t["addr"] = int(t["Address"])

# Ensure folder
os.makedirs(BASE_DIR, exist_ok=True)

# PLC connection
plc = snap7.client.Client()
plc.connect(PLC_IP, PLC_RACK, PLC_SLOT)

# Queue between producer and consumer
queue = Queue(maxsize=5000)  # add limit for safety

# ---------------- FUNCTIONS ----------------
def read_plc():
    """Read DB from PLC and decode tags"""
    raw = plc.db_read(DB_NUMBER, 0, DB_SIZE)
    data = {}
    for t in tags:
        name, dtype = t["Name"], t["Type"].lower()
        try:
            if dtype == "int":
                val = int.from_bytes(raw[t["addr"]:t["addr"]+2], byteorder="big", signed=True)
            elif dtype == "real":
                val = struct.unpack(">f", raw[t["addr"]:t["addr"]+4])[0]
            elif dtype == "bool":
                val = bool((raw[t["byte"]] >> t["bit"]) & 1)
            elif dtype == "time":
                addr = t["addr"]
                ms_val = int.from_bytes(raw[addr:addr+4], byteorder="big", signed=True)
                sign = "-" if ms_val < 0 else ""
                ms_val = abs(ms_val)
                hours, remainder = divmod(ms_val, 3600000)
                minutes, remainder = divmod(remainder, 60000)
                seconds, ms = divmod(remainder, 1000)
                val = f"{sign}{hours:02}:{minutes:02}:{seconds:02}.{ms:03}"
            else:
                val = None
        except Exception:
            val = None
        data[name] = val
    return data

def producer():
    """Read PLC as fast as possible and push to queue"""
    while True:
        ts = datetime.now()
        data = read_plc()
        try:
            queue.put_nowait((ts, data))
        except:
            # drop oldest if full
            try:
                queue.get_nowait()
                queue.put_nowait((ts, data))
            except:
                pass

def find_nearest(samples, target_ts):
    """Find sample closest to target timestamp"""
    if not samples:
        return None
    return min(samples, key=lambda x: abs((x[0] - target_ts).total_seconds()))

def safe_filename(ts: datetime):
    return ts.strftime("%Y-%m-%d_%H-%M-%S").replace(":", "-").replace(" ", "_")

def consumer():
    """Take samples from queue and build aligned JSON files"""
    now = datetime.now()
    start_sec = (now.replace(microsecond=0) + timedelta(seconds=1))

    while True:
        end_sec = start_sec + timedelta(seconds=1)
        samples = []
        while True:
            try:
                ts, data = queue.get(timeout=0.001)
                if ts < end_sec:
                    samples.append((ts, data))
                else:
                    queue.put((ts, data))
                    break
            except:
                if datetime.now() >= end_sec:
                    break

        buffer = []
        for i in range(SAMPLES_PER_FILE):
            target_ts = start_sec + timedelta(milliseconds=i*20)
            nearest = find_nearest(samples, target_ts)
            if nearest:
                buffer.append({
                    "timestamp": target_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "tags": nearest[1]
                })

        # Safe file path
        safe_time = safe_filename(start_sec)
        filename = os.path.normpath(os.path.join(BASE_DIR, f"DB{DB_NUMBER}_{safe_time}.json"))


        # Debug print to see hidden characters
        print("Saving file:", repr(filename))

        with open(filename, "w") as f:
            json.dump(buffer, f, separators=(',', ':'), indent=2)
        print(f"💾 Saved {len(buffer)} samples → {filename}")

        start_sec = end_sec

# ---------------- START THREADS ----------------
try:
    threading.Thread(target=producer, daemon=True).start()
    consumer()  # run in main thread
except KeyboardInterrupt:
    print("Stopping...")
    plc.disconnect()
