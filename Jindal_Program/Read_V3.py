import os
import re
import json
import struct
import snap7
import pandas as pd
import threading
import time
from datetime import datetime, timedelta
from queue import Queue, Empty
import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ---------------- CONFIG ----------------
OUT_DIR       = r"D:\AI_PROGRAMS\JSON"            # where per-second JSON files go
CSV_MAP_PATH  = r"D:\AI_PROGRAMS\DB199.csv"  # tag map: Name,Type,Address

PLC_IP, PLC_RACK, PLC_SLOT = "172.16.12.40", 0, 5
DB_NUMBER, DB_SIZE = 199, 364

GRID_MS = 20                          # 20 ms grid → 50 rows/second
QUEUE_MAXSIZE = 8000                  # buffer size
TIME_FORMAT = "string"                # "string" = hh:mm:ss.mmm, "ms" = raw milliseconds
PRINT_QUEUE = True
# ----------------------------------------

def parse_bool_address(addr_str):
    s = str(addr_str).strip()
    m = re.fullmatch(r"(\d+)\.(\d+)", s)     # Siemens "byte.bit"
    if m:
        byte, bit = int(m.group(1)), int(m.group(2))
        if not (0 <= bit <= 7):
            raise ValueError(f"Bit must be 0..7: {s}")
        return byte, bit
    # flat bit offset (e.g., 123 or "123.0")
    bit_offset = int(float(s))
    return divmod(bit_offset, 8)

def time_ms_to_string(ms_val: int) -> str:
    sign = "-" if ms_val < 0 else ""
    ms = abs(ms_val)
    h, rem = divmod(ms, 3_600_000)
    m, rem = divmod(rem, 60_000)
    s, ms = divmod(rem, 1_000)
    return f"{sign}{h:02}:{m:02}:{s:02}.{ms:03}"

# ---- Load tag map
df_map = pd.read_csv(CSV_MAP_PATH)
df_map.columns = [c.strip() for c in df_map.columns]
for need in ("Name", "Type", "Address"):
    if need not in df_map.columns:
        raise ValueError(f"Map CSV missing column: {need}")

tags = []
def tag_col_name(t):
    return t["name"]   # only the tag name, no address


for _, row in df_map.iterrows():
    typ  = str(row["Type"]).strip().lower()
    name = str(row["Name"]).strip()
    if typ == "bool":
        b, bit = parse_bool_address(row["Address"])
        t = {"name": name, "type": typ, "byte": b, "bit": bit}
    else:
        addr = int(float(row["Address"]))
        t = {"name": name, "type": typ, "addr": addr}
    t["col"] = tag_col_name(t)
    tags.append(t)

TAG_COLS = [t["col"] for t in tags]

# ---- Decode one snapshot
def decode_snapshot(raw: bytes) -> dict:
    out = {}
    for t in tags:
        col = t["col"]
        try:
            if t["type"] == "bool":
                out[col] = bool((raw[t["byte"]] >> t["bit"]) & 1)
            elif t["type"] == "int":
                a = t["addr"]
                out[col] = int.from_bytes(raw[a:a+2], "big", signed=True)
            elif t["type"] == "real":
                a = t["addr"]
                out[col] = struct.unpack(">f", raw[a:a+4])[0]
            elif t["type"] == "time":
                a = t["addr"]
                ms_val = int.from_bytes(raw[a:a+4], "big", signed=True)
                out[col] = ms_val if TIME_FORMAT == "ms" else time_ms_to_string(ms_val)
            else:
                out[col] = None
        except Exception:
            out[col] = None
    return out

def safe_json_name(ts: datetime) -> str:
    return f"DB{DB_NUMBER}_{ts.strftime('%Y-%m-%d_%H-%M-%S')}.json"

# ---- Producer (fast loop)
queue = Queue(maxsize=QUEUE_MAXSIZE)

def producer(plc):
    while True:
        try:
            raw = plc.db_read(DB_NUMBER, 0, DB_SIZE)
        except Exception:
            time.sleep(0.001)
            continue
        ts = datetime.now()
        snap = decode_snapshot(raw)
        try:
            queue.put_nowait((ts, snap))
        except:
            try:
                queue.get_nowait()
                queue.put_nowait((ts, snap))
            except:
                pass

# ---- Consumer (1 JSON per second, 50 rows)
def consumer():
    os.makedirs(OUT_DIR, exist_ok=True)
    sec_start = (datetime.now().replace(microsecond=0) + timedelta(seconds=1))
    last_snapshot = {c: None for c in TAG_COLS}
    last_source_ts = None

    while True:
        sec_end = sec_start + timedelta(seconds=1)
        samples = []
        while True:
            try:
                ts, snap = queue.get(timeout=0.001)
                if ts < sec_end:
                    samples.append((ts, snap))
                else:
                    queue.put((ts, snap))
                    break
            except Empty:
                if datetime.now() >= sec_end:
                    break
            except Exception:
                break

        samples.sort(key=lambda x: x[0])
        i = 0
        n = len(samples)

        rows = []
        for k in range(1000 // GRID_MS):
            grid_ts = sec_start + timedelta(milliseconds=k * GRID_MS)
            while i < n and samples[i][0] <= grid_ts:
                i += 1
            chosen = samples[i - 1] if i > 0 else None
            if chosen:
                last_source_ts, last_snapshot = chosen[0], chosen[1]
            row = {
                "timestamp": grid_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "tags": last_snapshot
            }
            rows.append(row)

        out_path = os.path.join(OUT_DIR, safe_json_name(sec_start))
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, separators=(',', ':'), indent=2)

        if PRINT_QUEUE:
            print(f"{datetime.now().strftime('%H:%M:%S')}  wrote 50 → {os.path.basename(out_path)}  | queue={queue.qsize():5d}", end="\r")

        sec_start = sec_end

def main():
    plc = snap7.client.Client()
    plc.connect(PLC_IP, PLC_RACK, PLC_SLOT)
    print(f"✅ Connected to PLC {PLC_IP}  DB{DB_NUMBER} ({DB_SIZE} bytes)")

    threading.Thread(target=producer, args=(plc,), daemon=True).start()
    try:
        consumer()
    except KeyboardInterrupt:
        print("\n⏹️ Stopping…")
    finally:
        try:
            plc.disconnect()
        except:
            pass

if __name__ == "__main__":
    main()
