
import os, re, json, time
from datetime import datetime, timedelta
import pandas as pd

# ---------------- CONFIG ----------------
JSON_DIR    = r"D:\AI_PROGRAMS\JSON"                 # folder with per-second JSON files
OUTPUT_CSV  = r"D:\AI_PROGRAMS\Monitor_tags.csv"
DB_NUMBER   = 199

# EXACT keys as they appear in JSON "tags":
TAGS_TO_MONITOR = ["TB2_1A_RollsCloseCmd",
       "TB2_1B_RollsCloseCmd",
       "TB2_2A_RollsCloseCmd",
       "TB2_2B_RollsCloseCmd",
       "TB2_1A_HighPressClose",
       "TB2_1B_HighPressClose",
       "TB2_2A_HighPressClose",
       "TB2_2B_HighPressClose"
]

CHECK_INTERVAL_SEC = 0.5
APPEND_CHUNK = 10000
SAFETY_DELAY_SEC = 5  # <-- process only files older than 5s
# ----------------------------------------

FNAME_RE = re.compile(rf"^DB{DB_NUMBER}_(\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}-\d{{2}}-\d{{2}})\.json$")

def parse_fname_dt(fname: str):
    m = FNAME_RE.match(fname)
    if not m: return None
    # filenames are local time like 2025-09-05_18-15-42
    return datetime.strptime(m.group(1), "%Y-%m-%d_%H-%M-%S")

def is_eligible(fname: str, now: datetime) -> bool:
    if not fname.endswith(".json"): return False
    dt = parse_fname_dt(fname)
    if dt is None: return False
    return dt <= (now - timedelta(seconds=SAFETY_DELAY_SEC))

def list_unprocessed(json_dir, processed_set):
    now = datetime.now()
    try:
        files = [f for f in os.listdir(json_dir) if is_eligible(f, now)]
    except FileNotFoundError:
        return []
    files.sort()  # chronological by name
    return [os.path.join(json_dir, f) for f in files if f not in processed_set]

# forward-fill cache so CSV never shows blanks
last_seen = {key: None for key in TAGS_TO_MONITOR}

def extract_rows_from_file(path):
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)  # list of 50 entries
    except Exception:
        return rows

    for item in data:
        ts = item.get("timestamp")
        tags = item.get("tags") or {}
        row = {"timestamp": ts}
        for key in TAGS_TO_MONITOR:
            val = tags.get(key, None)
            if val is None:
                val = last_seen.get(key, None)  # forward-fill
            else:
                last_seen[key] = val
            row[key] = val
        rows.append(row)
    return rows

def write_chunk(buf, csv_path, wrote_header_flag):
    if not buf: return wrote_header_flag
    df = pd.DataFrame(buf)
    df.to_csv(csv_path, index=False, mode="a", header=not wrote_header_flag)
    return True  # header has been written

def watch_and_append():
    os.makedirs(JSON_DIR, exist_ok=True)
    processed = set()
    buf = []
    wrote_header = os.path.exists(OUTPUT_CSV) and os.path.getsize(OUTPUT_CSV) > 0

    while True:
        for path in list_unprocessed(JSON_DIR, processed):
            rows = extract_rows_from_file(path)
            if rows:
                buf.extend(rows)
            processed.add(os.path.basename(path))
            if len(buf) >= APPEND_CHUNK:
                wrote_header = write_chunk(buf, OUTPUT_CSV, wrote_header)
                buf = []

        if buf:
            wrote_header = write_chunk(buf, OUTPUT_CSV, wrote_header)
            buf = []

        time.sleep(CHECK_INTERVAL_SEC)

if __name__ == "__main__":
    # Run continuously; builds/updates monitor_8tags.csv safely 5s behind real time
    watch_and_append()
