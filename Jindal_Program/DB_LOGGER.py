import os
import glob
import json
import re
import time
from io import StringIO
from datetime import time as dt_time
import pandas as pd
import psycopg2

# -------- CONFIG --------
FOLDER = r"D:\AI_PROGRAMS\JSON"   # folder with JSON files
TABLE  = "plc_logs"
PG = dict(
    host="localhost",
    port=5432,
    dbname="postgres",
    user="LOGGER",
    password="Pascal@123",
)
BATCH_SIZE = 500
SLEEP_SECS = 30

# Regexes for time formats
TIME_MILLIS_RE = re.compile(r"^\d{2}:\d{2}:\d{2}\.\d{3}$")
TIME_SECS_RE   = re.compile(r"^\d{2}:\d{2}:\d{2}$")

def load_json_records(obj):
    """
    Normalize into a list of records shaped like:
    { "timestamp": "...", "tags": {...} }
    Accepts:
      - [ {...}, {...} ]
      - {"timestamp": "...", "tags": {...}}
      - {"data"/"records"/"rows"/"items": [ ... ]}
    """
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for k in ("data", "records", "rows", "items"):
            if k in obj and isinstance(obj[k], list):
                return obj[k]
        if "timestamp" in obj and "tags" in obj and isinstance(obj["tags"], dict):
            return [obj]
    raise ValueError("Unsupported JSON structure.")

def flatten_record(rec: dict) -> dict:
    """Flatten {'timestamp','tags':{...}} to a single dict of columns."""
    row = {}
    if "timestamp" in rec:
        row["timestamp"] = rec["timestamp"]
    if "tags" in rec and isinstance(rec["tags"], dict):
        row.update(rec["tags"])
    else:
        for k, v in rec.items():
            if k != "timestamp":
                row[k] = v
    return row

def get_table_columns_and_types(cur, table):
    """Return {col: data_type} for public.table."""
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        ORDER BY ordinal_position
    """, (table,))
    return {name: dtype for name, dtype in cur.fetchall()}

def coerce_time_strings(series: pd.Series) -> pd.Series:
    """Keep strings like HH:MM:SS.mmm or HH:MM:SS; convert python time -> string."""
    if series.dtype == 'object':
        series = series.map(lambda v: v.strftime("%H:%M:%S.%f")[:-3] if isinstance(v, dt_time) else v)
    return series

def prepare_dataframe(df: pd.DataFrame, col_types: dict) -> pd.DataFrame:
    """Keep only known columns and coerce common types for COPY."""
    keep_cols = [c for c in df.columns if c in col_types]
    if "timestamp" in df.columns and "timestamp" not in keep_cols:
        keep_cols.insert(0, "timestamp")
    df = df[keep_cols].copy()

    # Timestamps → uniform millisecond precision
    if "timestamp" in df.columns:
        ts = pd.to_datetime(df["timestamp"], errors="coerce")
        df.loc[ts.notna(), "timestamp"] = ts[ts.notna()].dt.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3]

    # Booleans → 'true'/'false' strings for COPY
    for col, dtype in col_types.items():
        if col in df.columns and dtype == "boolean":
            df[col] = df[col].map(lambda v: None if pd.isna(v)
                                  else (str(v).lower() in ("1", "true", "t", "yes")))
            df[col] = df[col].map(lambda v: None if v is None else ("true" if v else "false"))

    # TIME columns
    for col, dtype in col_types.items():
        if col in df.columns and dtype == "time without time zone":
            df[col] = coerce_time_strings(df[col])

    # NaN -> NULL
    df = df.where(pd.notna(df), None)

    # Order: timestamp first
    if "timestamp" in df.columns:
        other = [c for c in df.columns if c != "timestamp"]
        df = df[["timestamp"] + other]
    return df

def copy_dataframe(conn, df: pd.DataFrame, table: str):
    """COPY a DataFrame into Postgres using CSV over STDIN."""
    if df.empty:
        return 0
    cols = list(df.columns)
    buf = StringIO()
    df.to_csv(buf, index=False)     # None becomes empty string
    buf.seek(0)
    col_list = ", ".join(f'"{c}"' for c in cols)
    copy_sql = f"COPY {table} ({col_list}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buf)
    return len(df)

def process_batch(conn, batch_files, col_types):
    """Read, flatten, prep, COPY one batch; commit and delete on success."""
    all_rows = []
    eligible_files = []  # only delete if commit succeeds
    for fp in batch_files:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                obj = json.load(f)
            records = load_json_records(obj)
            flat = [flatten_record(r) for r in records]
            if not flat:
                print(f"[SKIP] {os.path.basename(fp)} had 0 records.")
                continue
            all_rows.extend(flat)
            eligible_files.append(fp)
            print(f"[OK] {os.path.basename(fp)} → {len(flat)} record(s)")
        except Exception as e:
            # Skip bad file but continue the batch
            print(f"[ERROR] {os.path.basename(fp)} → {e}")

    if not all_rows:
        print("[BATCH] No valid rows to insert in this batch.")
        return 0, []

    df = pd.DataFrame(all_rows)
    df = prepare_dataframe(df, col_types)
    inserted = copy_dataframe(conn, df, TABLE)
    return inserted, eligible_files

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]

def main():
    files = sorted(glob.glob(os.path.join(FOLDER, "*.json")))
    if not files:
        print(f"No JSON files found in {FOLDER}")
        return

    conn = psycopg2.connect(**PG)
    conn.autocommit = False
    total_inserted = 0
    try:
        with conn.cursor() as cur:
            col_types = get_table_columns_and_types(cur, TABLE)
            if not col_types:
                raise RuntimeError(f"Table '{TABLE}' not found in public schema.")

        for batch_idx, batch_files in enumerate(chunked(files, BATCH_SIZE), start=1):
            print(f"\n[BATCH {batch_idx}] Processing {len(batch_files)} file(s)...")
            try:
                inserted, to_delete = process_batch(conn, batch_files, col_types)
                if inserted > 0:
                    conn.commit()
                    total_inserted += inserted
                    print(f"[BATCH {batch_idx}] Inserted {inserted} row(s). Committed.")
                    # Delete processed files
                    for fp in to_delete:
                        try:
                            os.remove(fp)
                            print(f"[DEL] {os.path.basename(fp)} removed.")
                        except Exception as e:
                            print(f"[WARN] Could not delete {os.path.basename(fp)} → {e}")
                else:
                    conn.rollback()
                    print(f"[BATCH {batch_idx}] Nothing inserted; rolled back.")
            except Exception as e:
                conn.rollback()
                print(f"[BATCH {batch_idx}] FATAL during insert → {e}. Rolled back.")
            # Sleep between batches
            print(f"[BATCH {batch_idx}] Sleeping {SLEEP_SECS}s...")
            time.sleep(SLEEP_SECS)

        print(f"\nDone. Inserted total {total_inserted} row(s) into {TABLE}.")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
