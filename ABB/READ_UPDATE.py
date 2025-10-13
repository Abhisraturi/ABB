import snap7
from snap7.util import get_bool, get_int, get_dint, get_real
from snap7.types import Areas
import pandas as pd
import json
import time
import os
import re

PLC_IP = "172.16.12.40"
PLC_RACK = 0
PLC_SLOT = 5

CSV_FILE = r"C:\Users\Administrator\Documents\tags.csv"
OUTPUT_JSON = r"C:\Users\Administrator\Documents\tags_output.json"
LOG_FILE = r"C:\Users\Administrator\Documents\skipped_tags.log"
READ_INTERVAL = 0.5  # seconds between cycles

# ---------- PARSER FIX FOR COMPLEX TAGS ----------
def clean_address(address):
    match = re.search(r"(DB\d+\.(DB[XDWB]+\s*\d+(\.\d+)?))", address)
    if match:
        return match.group(1)
    return address.strip()

def parse_address(addr_type, raw_address):
    address = clean_address(raw_address)
    try:
        if address.startswith("DB"):
            db_number = int(address.split('.')[0].replace("DB", ""))
            if ".DBX" in address:
                byte_bit = address.split("DBX")[1].strip()
                byte, bit = map(int, byte_bit.split('.'))
                return Areas.DB, db_number, byte, bit, "BOOL"
            elif ".DBW" in address:
                byte = int(address.split("DBW")[1].strip())
                return Areas.DB, db_number, byte, None, "INT"
            elif ".DBD" in address:
                byte = int(address.split("DBD")[1].strip())
                if addr_type == "DINT":
                    return Areas.DB, db_number, byte, None, "DINT"
                else:
                    return Areas.DB, db_number, byte, None, "REAL"
            elif ".DBB" in address:
                byte = int(address.split("DBB")[1].strip())
                return Areas.DB, db_number, byte, None, "BYTE"

        elif address.startswith("I ") or address.startswith("Q ") or address.startswith("M "):
            area_code = address[0]
            rest = address.split()[1]
            byte = int(rest.split('.')[0])
            bit = int(rest.split('.')[1]) if '.' in rest else None
            if area_code == "I":
                return Areas.PE, 0, byte, bit, addr_type
            elif area_code == "Q":
                return Areas.PA, 0, byte, bit, addr_type
            elif area_code == "M":
                return Areas.MK, 0, byte, bit, addr_type

        elif address.startswith("MD"):
            byte = int(address.replace("MD", "").strip())
            return Areas.MK, 0, byte, None, "REAL"
        elif address.startswith("MW"):
            byte = int(address.replace("MW", "").strip())
            return Areas.MK, 0, byte, None, "INT"
        elif address.startswith("MB"):
            byte = int(address.replace("MB", "").strip())
            return Areas.MK, 0, byte, None, "BYTE"

    except Exception as e:
        raise ValueError(f"Error parsing address {raw_address} → {address}: {e}")

    raise ValueError(f"Unsupported address format: {raw_address}")

# ---------- SNAP7 READ ----------
def read_tag_value(client, area, db_num, byte, bit, tag_type):
    try:
        if tag_type == "BOOL":
            data = client.read_area(area, db_num, byte, 1)
            return get_bool(data, 0, bit)
        elif tag_type == "INT":
            data = client.read_area(area, db_num, byte, 2)
            return get_int(data, 0)
        elif tag_type == "DINT":
            data = client.read_area(area, db_num, byte, 4)
            return get_dint(data, 0)
        elif tag_type == "REAL":
            data = client.read_area(area, db_num, byte, 4)
            return get_real(data, 0)
        elif tag_type == "BYTE":
            data = client.read_area(area, db_num, byte, 1)
            return int.from_bytes(data, byteorder='big')
    except Exception as e:
        raise RuntimeError(f"Read error: {e}")
    return None

def connect_plc():
    client = snap7.client.Client()
    client.connect(PLC_IP, PLC_RACK, PLC_SLOT)
    if not client.get_connected():
        raise RuntimeError("Failed to connect to PLC")
    return client

def read_tags_from_csv(file_path):
    df = pd.read_csv(file_path, header=None, names=["Type", "Address"])
    return df.to_dict(orient="records")

# ---------- MAIN LOOP ----------
def run_forever():
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    client = connect_plc()
    print("✅ Connected to PLC")

    tags_df = read_tags_from_csv(CSV_FILE)
    print(f"📜 Loaded {len(tags_df)} tags from CSV")

    while True:
        cycle_start = time.time()
        all_data = {}
        skipped_tags = []

        # First try reading all tags
        for tag in tags_df:
            tag_type = tag["Type"].strip()
            address = tag["Address"].strip()

            try:
                area, db_num, byte, bit, resolved_type = parse_address(tag_type, address)
                val = read_tag_value(client, area, db_num, byte, bit, resolved_type)
                all_data[address] = val
            except Exception as e:
                skipped_tags.append((address, str(e)))

        # Retry skipped tags individually
        retry_data = {}
        if skipped_tags:
            for addr, reason in skipped_tags:
                try:
                    # Re-parse again in case it's a parsing issue
                    tag_type = next(t["Type"] for t in tags_df if t["Address"] == addr)
                    area, db_num, byte, bit, resolved_type = parse_address(tag_type, addr)
                    val = read_tag_value(client, area, db_num, byte, bit, resolved_type)
                    retry_data[addr] = val
                except Exception as e:
                    # Still failing → log permanent skip
                    pass

        # Merge successful retries
        all_data.update(retry_data)

        # Save JSON
        with open(OUTPUT_JSON, "w") as jf:
            json.dump(all_data, jf, indent=4)

        # Log skipped tags
        with open(LOG_FILE, "w") as lf:
            for addr, reason in skipped_tags:
                if addr not in retry_data:  # still skipped
                    lf.write(f"{addr} → {reason}\n")

        cycle_end = time.time()
        elapsed = cycle_end - cycle_start
        total_read = len(all_data)
        total_skipped = len(tags_df) - total_read

        print(f"✅ Cycle done: {total_read} tags read, {total_skipped} skipped | Time: {elapsed:.3f}s")

        time.sleep(READ_INTERVAL)

if __name__ == "__main__":
    try:
        run_forever()
    except KeyboardInterrupt:
        print("\n⏹ Stopped manually")
