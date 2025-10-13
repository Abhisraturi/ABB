#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PLC Data Logger + DB Inserter + API Sender
------------------------------------------
- Reads from Siemens PLC via snap7
- Aggregates to 50 samples/sec (20 ms grid)
- Inserts into PostgreSQL (if psycopg2+pandas available)
- Sends per-second payloads to API (requests)
- Per-minute sid reset + minute key in logs
- CPU/RAM resource logging every 10s (psutil)
- Dedicated PLC/API error logs (+ timestamps)
- Watchdog restarts the program on persistent stalls
- last_sec/lastsec.json overwrites once per minute
"""

import os, re, sys, json, time, struct, queue, signal, logging, psutil
from logging.handlers import TimedRotatingFileHandler
from io import StringIO
from datetime import datetime, timedelta, time as dt_time
from threading import Thread, Event, Lock
from typing import Dict, Any, List, Tuple

# ------------------- CONFIG -------------------
CSV_MAP_PATH  = os.getenv("CSV_MAP_PATH", r"D:\\AI_PROGRAMS\\DB199.csv")
PLC_IP        = os.getenv("PLC_IP", "172.16.12.40")
PLC_RACK      = int(os.getenv("PLC_RACK", 0))
PLC_SLOT      = int(os.getenv("PLC_SLOT", 5))
DB_NUMBER     = int(os.getenv("DB_NUMBER", 199))
DB_SIZE       = int(os.getenv("DB_SIZE", 364))

GRID_MS       = int(os.getenv("GRID_MS", 20))           # 20ms → 50 rows/sec
QUEUE_MAXSIZE = int(os.getenv("QUEUE_MAXSIZE", 8000))   # raw snapshots buffer
TIME_FORMAT   = os.getenv("TIME_FORMAT", "string")      # "string" or "ms"
PRINT_QUEUE   = os.getenv("PRINT_QUEUE", "True").lower() == "true"

PG = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    dbname=os.getenv("PG_DB", "postgres"),
    user=os.getenv("PG_USER", "postgres"),
    password=os.getenv("PG_PASS", "Pascal@123"),
)
TABLE  = os.getenv("PG_TABLE", "plc_logs")

API_URL = os.getenv("API_URL", "http://104.154.22.115:8001/app/v0.0.1/plc/raw-post")

LOG_DIR = os.getenv("LOG_DIR", r"D:\\AI_PROGRAMS\\PLC_LOGS")
DB_NAME = os.getenv("LOG_DBNAME", "JINDAL_DB199")

SENDER_THREADS = int(os.getenv("SENDER_THREADS", "2"))
MAX_RETRIES    = int(os.getenv("MAX_RETRIES", "5"))

RETRY_SLEEP_BASE = float(os.getenv("RETRY_SLEEP_BASE", "0.5"))
MAX_RETRY_SLEEP  = float(os.getenv("MAX_RETRY_SLEEP", "10"))

# ------------------- OPTIONAL IMPORTS -------------------
try:
    import pandas as pd
except Exception:
    pd = None

try:
    import requests
except Exception:
    requests = None

try:
    import psycopg2
    import psycopg2.extras as pgx
except Exception:
    psycopg2 = None

try:
    import snap7
except Exception:
    snap7 = None

# ------------------- LOGGING -------------------
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger("PLC_PIPE")
logger.setLevel(logging.INFO)

fh = TimedRotatingFileHandler(
    os.path.join(LOG_DIR, "plc_sender.log"),
    when="midnight", interval=1, backupCount=7, encoding="utf-8"
)

def _namer(default_name):
    # plc_sender.log.YYYY-MM-DD → plc_sender_YYYY-MM-DD.log
    base, ext = os.path.splitext(default_name)
    parts = base.split(".")
    if len(parts) > 1:
        date_part = parts[-1]
        return f"{parts[0]}_{date_part}.log"
    return default_name

fh.namer = _namer
ch = logging.StreamHandler()

main_fmt = logging.Formatter(
    "%(asctime)s | [%(levelname)s] [DB:%(db)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

fh.setFormatter(main_fmt)
ch.setFormatter(main_fmt)
logger.addHandler(fh)
logger.addHandler(ch)

class DBLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        if "extra" not in kwargs:
            kwargs["extra"] = {}
        kwargs["extra"]["db"] = DB_NAME
        return msg, kwargs

log = DBLoggerAdapter(logger, {"db": DB_NAME})

# Dedicated error logs with timestamps
os.makedirs("logs", exist_ok=True)
err_fmt = logging.Formatter(
    "%(asctime)s | [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

plc_err_logger = logging.getLogger("PLC_ERR")
plc_err_logger.setLevel(logging.WARNING)
plc_fh = logging.FileHandler("logs/plc_errors.log", encoding="utf-8")
plc_fh.setFormatter(err_fmt)
plc_err_logger.addHandler(plc_fh)

api_err_logger = logging.getLogger("API_ERR")
api_err_logger.setLevel(logging.WARNING)
api_fh = logging.FileHandler("logs/api_errors.log", encoding="utf-8")
api_fh.setFormatter(err_fmt)
api_err_logger.addHandler(api_fh)

# ------------------- SHARED STATE -------------------
stop_event = Event()

raw_q: "queue.Queue[Tuple[datetime, Dict[str, Any]]]" = queue.Queue(maxsize=QUEUE_MAXSIZE)

sec_id_lock = Lock()
sec_id = 0
last_sid_minute = None

pending_lock = Lock()
# pending[sid] = {"rows": List[dict], "db": False, "api": False}
pending: Dict[int, Dict[str, Any]] = {}

db_q: "queue.Queue[int]" = queue.Queue(maxsize=1024)
api_q: "queue.Queue[int]" = queue.Queue(maxsize=1024)

read_count = 0
last_report = time.time()
last_plc_read = time.time()
last_api_success = time.time()

# ------------------- UTILITIES -------------------
def restart_program():
    log.warning("Restarting program due to persistent failure...")
    # Exit with non-zero so NSSM/Task Scheduler restarts us.
    sys.exit(1)

def time_ms_to_string(ms_val: int) -> str:
    sign = "-" if ms_val < 0 else ""
    ms = abs(ms_val)
    h, rem = divmod(ms, 3_600_000)
    m, rem = divmod(rem, 60_000)
    s, ms = divmod(rem, 1_000)
    return f"{sign}{h:02}:{m:02}:{s:02}.{ms:03}"

def parse_bool_address(addr_str):
    s = str(addr_str).strip()
    m = re.fullmatch(r"(\d+)\.(\d+)", s)
    if m:
        return int(m.group(1)), int(m.group(2))
    bit_offset = int(float(s))
    return divmod(bit_offset, 8)

class Tag:
    __slots__ = ("name", "typ", "addr", "byte", "bit", "col")
    def __init__(self, name, typ, addr=None, byte=None, bit=None):
        self.name = name
        self.typ  = typ
        self.addr = addr
        self.byte = byte
        self.bit  = bit
        self.col  = name

def load_tags(csv_path: str) -> List[Tag]:
    import pandas as _pd
    df_map = _pd.read_csv(csv_path)
    # Expect columns Name,Type,Address
    tags: List[Tag] = []
    for _, row in df_map.iterrows():
        typ  = str(row["Type"]).strip().lower()
        name = str(row["Name"]).strip()
        if typ == "bool":
            b, bit = parse_bool_address(row["Address"])
            tags.append(Tag(name, typ, byte=b, bit=bit))
        else:
            tags.append(Tag(name, typ, addr=int(float(row["Address"]))))
    return tags

def decode_snapshot(raw: bytes, tags: List[Tag]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for t in tags:
        try:
            if t.typ == "bool":
                out[t.col] = bool((raw[t.byte] >> t.bit) & 1)
            elif t.typ == "int":
                out[t.col] = int.from_bytes(raw[t.addr:t.addr+2], "big", signed=True)
            elif t.typ == "real":
                out[t.col] = struct.unpack(">f", raw[t.addr:t.addr+4])[0]
            elif t.typ == "time":
                ms_val = int.from_bytes(raw[t.addr:t.addr+4], "big", signed=True)
                out[t.col] = ms_val if TIME_FORMAT == "ms" else time_ms_to_string(ms_val)
            else:
                out[t.col] = None
        except Exception:
            out[t.col] = None
    return out

def flatten_record(rec: dict) -> dict:
    row = {}
    if "timestamp" in rec:
        row["timestamp"] = rec["timestamp"]
    if "tags" in rec and isinstance(rec["tags"], dict):
        row.update(rec["tags"])
    return row

# ------------------- THREADS -------------------
def plc_producer():
    global read_count, last_report, last_plc_read
    if snap7 is None:
        log.warning("snap7 not available; PLC producer disabled")
        return

    plc = snap7.client.Client()

    # Connect loop
    while not stop_event.is_set():
        try:
            plc.connect(PLC_IP, PLC_RACK, PLC_SLOT)
            log.info(f"✅ Connected to PLC {PLC_IP} DB{DB_NUMBER} ({DB_SIZE} bytes)")
            break
        except Exception as e:
            plc_err_logger.warning(f"PLC connect error: {e}")
            time.sleep(0.5)

    # Read loop
    while not stop_event.is_set():
        try:
            raw = plc.db_read(DB_NUMBER, 0, DB_SIZE)
            ts = datetime.now()
            snap = decode_snapshot(raw, TAGS)

            # metrics
            read_count += 1
            now = time.time()
            last_plc_read = now
            if now - last_report >= 1.0:
                log.info(f"[PLC] Reads in last second: {read_count}")
                read_count = 0
                last_report = now

            try:
                raw_q.put_nowait((ts, snap))
            except queue.Full:
                # Drop oldest to keep up
                try:
                    raw_q.get_nowait()
                    raw_q.put_nowait((ts, snap))
                except Exception:
                    pass

        except Exception as e:
            plc_err_logger.warning(f"PLC read failed: {e}")
            time.sleep(0.001)

def second_aggregator():
    """Builds 50 samples per second (20ms grid) and fans out to DB/API.
       Resets sid every minute; logs carry minute key."""
    global sec_id, last_sid_minute

    sec_start = (datetime.now().replace(microsecond=0) + timedelta(seconds=1))
    last_snapshot = {t.col: None for t in TAGS}

    while not stop_event.is_set():
        sec_end = sec_start + timedelta(seconds=1)
        samples: List[Tuple[datetime, Dict[str, Any]]] = []

        # Collect raw samples for this second
        while True:
            try:
                ts, snap = raw_q.get(timeout=0.001)
                if ts < sec_end:
                    samples.append((ts, snap))
                else:
                    raw_q.put((ts, snap))
                    break
            except queue.Empty:
                if datetime.now() >= sec_end:
                    break

        # Sort + grid
        samples.sort(key=lambda x: x[0])
        i, n = 0, len(samples)
        rows: List[Dict[str, Any]] = []

        for k in range(1000 // GRID_MS):
            grid_ts = sec_start + timedelta(milliseconds=k * GRID_MS)
            while i < n and samples[i][0] <= grid_ts:
                i += 1
            if i > 0:
                last_snapshot = samples[i - 1][1]
            rows.append({
                "timestamp": grid_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "tags": last_snapshot,
            })

        # Per-minute sid
        cur_min = datetime.now().strftime("%Y-%m-%d_%H-%M")
        with sec_id_lock:
            if last_sid_minute != cur_min:
                sec_id = 0
                last_sid_minute = cur_min
            sec_id += 1
            sid = sec_id

        # Fan out
        with pending_lock:
            pending[sid] = {"rows": rows, "db": False, "api": False}

        db_q.put(sid)
        api_q.put(sid)

        if PRINT_QUEUE:
            print(
                f"{datetime.now().strftime('%H:%M:%S')}  built 50 rows for {sec_start.strftime('%H:%M:%S')}  "
                f"| sid={sid} minute={last_sid_minute} raw_q={raw_q.qsize():5d}",
                end="\r",
            )

        sec_start = sec_end

class DBWriter(Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.conn = None
        self.col_types: Dict[str, str] = {}

    def connect(self):
        if psycopg2 is None or pd is None:
            log.warning("DBWriter disabled (missing psycopg2 or pandas)")
            return
        self.conn = psycopg2.connect(**PG)
        self.conn.autocommit = False
        # Ensure table exists (minimal)
        with self.conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"public.{TABLE}",))
            exists = cur.fetchone()[0] is not None
            if not exists:
                cur.execute(f"CREATE TABLE {TABLE} (timestamp timestamptz NOT NULL)")
                self.conn.commit()
        # Fetch column types
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s
                ORDER BY ordinal_position
            """, (TABLE,))
            self.col_types = {name: dtype for name, dtype in cur.fetchall()}

    def run(self):
        # If DB libs missing, mark DB as done so pending can be reaped
        if psycopg2 is None or pd is None:
            while not stop_event.is_set():
                try:
                    sid = db_q.get(timeout=0.5)
                except queue.Empty:
                    continue
                with pending_lock:
                    if sid in pending:
                        pending[sid]["db"] = True
                log.info(f"[{last_sid_minute}] DB (disabled) marked done for sid={sid}")
            return

        while not stop_event.is_set():
            try:
                sid = db_q.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                if self.conn is None:
                    self.connect()

                with pending_lock:
                    rec = pending.get(sid)
                    rows = rec["rows"] if rec else None

                if not rows:
                    continue

                # Flatten and copy
                flat = [flatten_record(r) for r in rows]
                df = pd.DataFrame(flat)

                if not df.empty:
                    buf = StringIO()
                    df.to_csv(buf, index=False)
                    buf.seek(0)
                    cols = ", ".join(f'"{c}"' for c in df.columns)
                    with self.conn.cursor() as cur:
                        cur.copy_expert(
                            f"COPY {TABLE} ({cols}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')",
                            buf,
                        )
                    self.conn.commit()

                with pending_lock:
                    if sid in pending:
                        pending[sid]["db"] = True
                log.info(f"[{last_sid_minute}] DB +{len(df)} rows for sid={sid}")

            except Exception as e:
                try:
                    if self.conn:
                        self.conn.rollback()
                except Exception:
                    pass
                log.warning(f"DB insert error sid={sid}: {e}")
                time.sleep(RETRY_SLEEP_BASE)
                try:
                    db_q.put_nowait(sid)  # retry
                except queue.Full:
                    pass

class APISender(Thread):
    def __init__(self, idx: int):
        super().__init__(daemon=True)
        self.idx = idx
        self.session = requests.Session() if requests else None
        self.last_minute = None

    def run(self):
        global last_api_success
        if requests is None:
            log.warning("requests missing; API sender disabled")
            return

        while not stop_event.is_set():
            try:
                sid = api_q.get(timeout=0.5)
            except queue.Empty:
                continue

            with pending_lock:
                rec = pending.get(sid)
                rows = rec["rows"] if rec else None
            if not rows:
                continue

            backoff = RETRY_SLEEP_BASE
            ok = False
            resp_text = ""

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    resp = self.session.post(API_URL, json=rows, timeout=5)
                    resp.raise_for_status()
                    resp_text = resp.text
                    log.info(
                        f"[{last_sid_minute}] API✅ sid={sid} (try {attempt}) "
                        f"Status:{resp.status_code} Resp:{resp.text[:120]}"
                    )
                    ok = True
                    last_api_success = time.time()
                    break
                except Exception as e:
                    api_err_logger.warning(
                        f"API{self.idx} send error sid={sid} try={attempt}: {e}"
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * 2, MAX_RETRY_SLEEP)

            if ok:
                # Write last-second payload once per minute
                try:
                    cur_min = datetime.now().strftime("%Y-%m-%d_%H-%M")
                    if cur_min != self.last_minute:
                        self.last_minute = cur_min
                        os.makedirs("last_sec", exist_ok=True)
                        with open(os.path.join("last_sec", "lastsec.json"), "w", encoding="utf-8") as f:
                            json.dump(
                                {"timestamp": cur_min, "payload": rows, "response": resp_text},
                                f,
                                indent=2,
                            )
                except Exception as e:
                    log.warning(f"Failed to write lastsec.json: {e}")

                with pending_lock:
                    if sid in pending:
                        pending[sid]["api"] = True

            else:
                api_err_logger.error(f"API{self.idx} send failed sid={sid} → restarting program")
                restart_program()

def reaper():
    """Cleans up per-second entries once DB & API have both succeeded."""
    done = 0
    last_log = time.time()
    while not stop_event.is_set():
        time.sleep(0.5)
        remove: List[int] = []
        with pending_lock:
            for sid, rec in list(pending.items()):
                if rec.get("db") and rec.get("api"):
                    remove.append(sid)
            for sid in remove:
                pending.pop(sid, None)
        done += len(remove)
        if time.time() - last_log >= 5:
            last_log = time.time()
            log.info(
                f"ok={done} pending={len(pending)} "
                f"q_raw={raw_q.qsize()} q_db={db_q.qsize()} q_api={api_q.qsize()}"
            )

def watchdog():
    """Resource logging and health-check restarts."""
    plc_fail_count = 0
    api_fail_count = 0

    while not stop_event.is_set():
        time.sleep(10)

        # Resource usage
        p = psutil.Process(os.getpid())
        mem = p.memory_info().rss / 1024 / 1024
        cpu = p.cpu_percent(interval=None)
        msg = f"[RESOURCE] RAM={mem:.1f} MB | CPU={cpu:.1f}%"
        log.info(msg)                # main log
        plc_err_logger.info(msg)     # also in PLC error log
        api_err_logger.info(msg)     # also in API error log

        # PLC health
        if time.time() - last_plc_read > 10:
            plc_fail_count += 1
            log.warning(f"[WATCHDOG] No PLC reads {plc_fail_count*10}s (strike {plc_fail_count}/3)")
            if plc_fail_count >= 3:
                plc_err_logger.error("PLC stalled >30s → restart")
                restart_program()
        else:
            plc_fail_count = 0

        # API health
        if time.time() - last_api_success > 10:
            api_fail_count += 1
            log.warning(f"[WATCHDOG] No API success {api_fail_count*10}s (strike {api_fail_count}/3)")
            if api_fail_count >= 3:
                api_err_logger.error("API stalled >30s → restart")
                restart_program()
        else:
            api_fail_count = 0

# ------------------- MAIN -------------------
def handle_signal(signum, frame):
    log.info("Stopping…")
    stop_event.set()

def main():
    global TAGS
    try:
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
    except Exception:
        pass

    # Load tag map
    try:
        TAGS = load_tags(CSV_MAP_PATH)
        log.info(f"Loaded {len(TAGS)} tags from {CSV_MAP_PATH}")
    except Exception as e:
        log.error(f"Failed to load tag map: {e}")
        return 2

    threads: List[Thread] = []

    t = Thread(target=second_aggregator, name="Aggregator", daemon=True)
    t.start(); threads.append(t)

    t = Thread(target=plc_producer, name="PLC", daemon=True)
    t.start(); threads.append(t)

    dbw = DBWriter()
    dbw.start(); threads.append(dbw)

    for i in range(SENDER_THREADS):
        s = APISender(i + 1)
        s.start(); threads.append(s)

    t = Thread(target=reaper, name="Reaper", daemon=True)
    t.start(); threads.append(t)

    t = Thread(target=watchdog, name="Watchdog", daemon=True)
    t.start(); threads.append(t)

    # Wait loop
    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    finally:
        for t in threads:
            try:
                t.join(timeout=2.0)
            except Exception:
                pass
        log.info("✅ Clean shutdown.")

# ------------------- ENTRYPOINT -------------------
if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        # Catch truly fatal errors and request an external restart
        try:
            log.error(f"💥 Uncaught fatal exception: {e}", exc_info=True)
            with open(os.path.join("logs", "fatal_errors.log"), "a", encoding="utf-8") as f:
                f.write(f"{datetime.now()} FATAL ERROR: {e}\n")
        finally:
            restart_program()
