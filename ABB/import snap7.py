import snap7
from snap7.util import get_int, get_real, get_bool, get_dint
from datetime import datetime, timedelta
import csv
import json

PLC_IP = "172.16.12.40"   # PLC IP address
PLC_RACK = 0
PLC_SLOT = 5
DB_NUMBER = 199
DB_SIZE = 358
CSV_FILE = "DB199.csv"
OUTPUT_JSON = "plc_data.json"

def read_db_structure(csv_file):
    structure = []
    with open(csv_file, mode='r', newline='') as file:
        reader = csv.DictReader(file)  # ✅ Reads by header names
        for row in reader:
            address = row['Address'].strip()
            name = row['Name'].strip()
            data_type = row['Type'].strip().upper()

            addr_parts = address.replace("DBD", "").replace("DBW", "").replace("DBB", "").split(".")
            byte_offset = int(addr_parts[0])
            bit_offset = int(addr_parts[1]) if len(addr_parts) > 1 else 0

            structure.append({
                "byte": byte_offset,
                "bit": bit_offset,
                "name": name,
                "type": data_type
            })
    return structure



def parse_time(value):
    """Convert milliseconds (S7 TIME) to HH:MM:SS.mmm"""
    try:
        td = timedelta(milliseconds=value)
        return str(td)
    except Exception:
        return None

def read_values_from_db(plc, db_data, structure):
    """Parse DB values according to structure"""
    results = {}
    for item in structure:
        byte = item['byte']
        bit = item['bit']
        name = item['name']
        dtype = item['type']

        if dtype == "INT":
            results[name] = get_int(db_data, byte)
        elif dtype == "REAL":
            results[name] = round(get_real(db_data, byte), 3)
        elif dtype == "BOOL" and bit is not None:
            results[name] = get_bool(db_data, byte, bit)
        elif dtype == "TIME":
            ms_value = get_dint(db_data, byte)  # TIME is stored as DINT in ms
            results[name] = parse_time(ms_value)
        else:
            results[name] = None
    return results

def main():
    plc = snap7.client.Client()
    plc.connect(PLC_IP, PLC_RACK, PLC_SLOT)
    if not plc.get_connected():
        print("Failed to connect to PLC")
        return

    structure = read_db_structure(CSV_FILE)
    db_data = plc.db_read(DB_NUMBER, 0, DB_SIZE)
    results = read_values_from_db(plc, db_data, structure)

    # Add timestamp
    results_with_time = {
        "timestamp": datetime.now().isoformat(),
        "data": results
    }

    # Save to JSON
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(results_with_time, f, indent=4)

    print(f"Data saved to {OUTPUT_JSON}")
    plc.disconnect()

if __name__ == "__main__":
    main()
