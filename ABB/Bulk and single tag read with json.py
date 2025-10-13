import snap7
import pandas as pd
import json
import time
import re
from datetime import datetime
from collections import defaultdict
from snap7.util import get_bool, get_int, get_real, get_dint, get_byte
from snap7.types import Areas

PLC_IP = "172.16.12.40"
PLC_RACK = 0
PLC_SLOT = 5

TAGS_CSV = "Tags.csv"         
MAPPING_CSV = "mapping.csv"   
def connect_to_plc(ip, rack, slot):
    client = snap7.client.Client()
    client.connect(ip, rack, slot)
    if client.get_connected():
        print(f"? Connected to PLC at {ip}")
    else:
        raise Exception("? Connection to PLC failed.")
    return client
def parse_tag(tag_type, address):
    tag_type = tag_type.strip().upper()
    address = address.strip().upper()

    db_match = re.match(r"DB\s*(\d+)\s*\.?\s*DB([XBWLD])\s*(\d+)(?:\.(\d))?", address)
    if db_match:
        db_number = int(db_match.group(1))  
        byte = int(db_match.group(3))        
        bit = int(db_match.group(4)) if db_match.group(4) else None
        return Areas.DB, db_number, byte, bit, tag_type

    io_match = re.match(r"([IQM])\s*(\d+)(?:\.(\d))?", address)
    if io_match:
        area = {"I": Areas.PE, "Q": Areas.PA, "M": Areas.MK}[io_match.group(1)]
        byte = int(io_match.group(2))
        bit = int(io_match.group(3)) if io_match.group(3) else None
        return area, 0, byte, bit, tag_type

    #
    if address.startswith("MD"):
        byte = int(address.replace("MD", "").strip())
        return Areas.MK, 0, byte, None, tag_type

    raise ValueError(f"Unsupported address format: {address}")


def get_read_length(tag_type):
    return 4 if tag_type in ["REAL", "DINT"] else 2 if tag_type == "INT" else 1


def load_mapping_file(mapping_csv):
    df = pd.read_csv(mapping_csv)
    if "Tags" not in df.columns or "Address" not in df.columns:
        raise Exception("mapping.csv must have 'Tags' and 'Address' columns")

    mapping_dict = defaultdict(list)
    for _, row in df.iterrows():
        addr = row["Address"].strip().upper()
        name = row["Tags"].strip()
        mapping_dict[addr].append(name)

    print(f"? Loaded {sum(len(v) for v in mapping_dict.values())} name mappings for {len(mapping_dict)} unique addresses")
    return mapping_dict

def load_and_group_tags(csv_path):
    df = pd.read_csv(csv_path)
    if 'Type' not in df.columns or 'Address' not in df.columns:
        raise Exception("tags.csv must contain 'Type' and 'Address' columns")

    grouped = defaultdict(list)

    for _, row in df.iterrows():
        try:
            area, db, byte, bit, parsed_type = parse_tag(row['Type'], row['Address'])
            grouped[(area, db)].append((byte, bit, parsed_type, row['Address'].strip().upper()))
        except Exception as e:
            print(f"?? Skipping tag {row['Address']} ? {e}")

    return grouped

def read_grouped_tags(client, grouped_tags):
    raw_values = {}

    for (area, db), tag_list in grouped_tags.items():
        if not tag_list:
            continue

        min_byte = min(byte for byte, _, _, _ in tag_list)
        max_byte = max(byte + get_read_length(tp) for byte, _, tp, _ in tag_list)
        length = max_byte - min_byte

        try:
            data = client.read_area(area, db, min_byte, length)
        except Exception as e:
            print(f"? Failed bulk read area={area} db={db}: {e}")
            for _, _, _, address in tag_list:
                raw_values[address] = None
            continue

        for byte, bit, tag_type, address in tag_list:
            offset = byte - min_byte
            try:
                if tag_type == "BOOL":
                    value = get_bool(data, offset, bit if bit is not None else 0)
                elif tag_type == "INT":
                    value = get_int(data, offset)
                elif tag_type == "REAL":
                    value = get_real(data, offset)
                elif tag_type == "DINT":
                    value = get_dint(data, offset)
                elif tag_type == "BYTE":
                    value = get_byte(data, offset)
                else:
                    value = None
                raw_values[address] = value
            except Exception as ve:
                print(f"?? Parse error for {address}: {ve}")
                raw_values[address] = None

    return raw_values

def expand_to_friendly_names(raw_values, mapping_dict):
    expanded_json = {}

    for addr, value in raw_values.items():
        if addr in mapping_dict:
            for name in mapping_dict[addr]:
                expanded_json[name] = value
        else:
            expanded_json[addr] = value

    return expanded_json

def main():
    client = connect_to_plc(PLC_IP, PLC_RACK, PLC_SLOT)

    mapping_dict = load_mapping_file(MAPPING_CSV)

    grouped_tags = load_and_group_tags(TAGS_CSV)

    try:
        while True:
            start_time = time.time()

            raw_values = read_grouped_tags(client, grouped_tags)

            final_json = expand_to_friendly_names(raw_values, mapping_dict)

            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            json_filename = f"plc_data_{timestamp}.json"
            with open(json_filename, "w") as f:
                json.dump(final_json, f, indent=4)

            elapsed = time.time() - start_time
            print(f"? Cycle finished: {len(final_json)} entries ? {json_filename} | Time: {elapsed:.3f}s")

            time.sleep(1)

    except KeyboardInterrupt:
        print("?? Stopped by user.")
    finally:
        client.disconnect()
        print("?? Disconnected from PLC.")

if __name__ == "__main__":
    main()
