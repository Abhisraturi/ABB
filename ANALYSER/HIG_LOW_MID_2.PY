import time
import struct
import threading
from datetime import datetime
from pymodbus.client import ModbusTcpClient
import pyodbc

# ==============================
# --- CONFIGURATION SECTION ---
# ==============================

STATUS_DEVICE_IP = "192.168.0.10"
VALUE_DEVICE_IP = "192.168.0.4"
ACTUAL_VALUE_IP = "127.0.0.1"
SLAVE_ID = 1

STATUS_REGISTERS = [0, 1, 2]
VALUE_REGISTER = 0
ACTUAL_VALUE_REGISTER = 111
HIGH_LEVEL_ACTUAL_REGISTER = 123
LOW_LEVEL_ACTUAL_REGISTER = None  # For clarity

LEVEL_NAMES = {0: "Low Level", 1: "Mid Level", 2: "High Level"}

# --- SQL CONFIG ---
SQL_SERVER = "DESKTOP-F4FK4GN"
SQL_DB = "DATA"
TABLE_NAME = "LevelCapture"

CONN_STR = (
    f"DRIVER={{SQL Server}};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DB};"
    f"Trusted_Connection=yes;"
)


# ==============================
# --- DATABASE FUNCTIONS ---
# ==============================

def get_connection():
    """Return a fresh database connection (auto-reconnect if dropped)."""
    try:
        return pyodbc.connect(CONN_STR)
    except Exception as e:
        print(f"[DB] Connection error: {e}")
        return None


def insert_value(cursor, timestamp, level_name, value_type, value):
    """Insert one value row safely into SQL."""
    try:
        query = f"""
            INSERT INTO {TABLE_NAME} ([Timestamp], [LevelName], [ValueType], [Value])
            VALUES (?, ?, ?, ?)
        """
        cursor.execute(query, timestamp, level_name, value_type, value)
    except Exception as e:
        print(f"[DB] Insert error: {e}")


# ==============================
# --- MODBUS UTILS ---
# ==============================

def decode_float(w1, w2, order="ABCD"):
    """Decode two 16-bit Modbus registers into a 32-bit float."""
    b1 = struct.pack('>H', w1)
    b2 = struct.pack('>H', w2)

    if order == "ABCD":
        data = b1 + b2
    elif order == "CDAB":
        data = b2 + b1
    elif order == "BADC":
        data = b1[::-1] + b2[::-1]
    elif order == "DCBA":
        data = b2[::-1] + b1[::-1]
    else:
        raise ValueError("Unknown byte order")

    return struct.unpack('>f', data)[0]


def read_input_status(client, address):
    try:
        res = client.read_discrete_inputs(address=address, count=1)
        if res.isError():
            return None
        return res.bits[0]
    except Exception as e:
        print(f"[Modbus] Error reading input {address}: {e}")
        return None


def read_float_input(client, address, order="ABCD"):
    try:
        res = client.read_input_registers(address=address, count=2)
        if not res.isError() and hasattr(res, "registers") and len(res.registers) == 2:
            return decode_float(res.registers[0], res.registers[1], order)
    except Exception as e:
        print(f"[Modbus] Error reading input float: {e}")
    return None


def read_float_holding(client, address, order="CDAB"):
    try:
        res = client.read_holding_registers(address=address, count=2)
        if not res.isError() and hasattr(res, "registers") and len(res.registers) == 2:
            return decode_float(res.registers[0], res.registers[1], order)
    except Exception as e:
        print(f"[Modbus] Error reading holding float: {e}")
    return None


# ==============================
# --- CAPTURE LOGIC ---
# ==============================

def capture_and_insert(level_name, actual_register):
    """Thread-safe capture sequence for one level."""
    print(f"[{level_name}] Thread started.")
    delays = [120, 10, 10]
    values = []

    # New SQL connection per thread
    conn = get_connection()
    if conn is None:
        print("[DB] Could not open connection. Skipping capture.")
        return
    cursor = conn.cursor()

    with ModbusTcpClient(host=VALUE_DEVICE_IP, port=502) as value_client:
        for i, delay in enumerate(delays, 1):
            time.sleep(delay)
            val = read_float_input(value_client, VALUE_REGISTER)
            timestamp = datetime.now()

            if val is not None:
                print(f"  Read {i}: {val:.4f}")
                insert_value(cursor, timestamp, level_name, f"Val {i}", val)
                values.append(val)
            else:
                print(f"  Read {i} failed")

        # Compute average and store
        if values:
            avg_val = sum(values) / len(values)
            insert_value(cursor, datetime.now(), level_name, "Avg", avg_val)
            print(f"  → Avg: {avg_val:.4f} inserted")

            # Read actual value
            if actual_register is None:
                actual_val = None
                print(f"  [Low Level] → No actual register, skipping error calc.")
            else:
                with ModbusTcpClient(host=ACTUAL_VALUE_IP, port=502) as actual_client:
                    actual_val = read_float_holding(actual_client, actual_register)

            # Compute error
            if actual_val is not None:
                error = avg_val - actual_val
                percent_error = ((error / actual_val) * 100) if actual_val != 0 else 0.0
                print(f"  Actual Value: {actual_val:.4f}")
                print(f"  Error: {error:.4f}")
                print(f"  % Error: {percent_error:.2f}%")

                t = datetime.now()
                insert_value(cursor, t, level_name, "Actual", actual_val)
                insert_value(cursor, t, level_name, "Error", error)
                insert_value(cursor, t, level_name, "% Error", percent_error)

    conn.commit()
    conn.close()
    print(f"[{level_name}] Thread finished successfully.\n")


# ==============================
# --- MAIN LOOP ---
# ==============================

def main():
    print(" Level capture system started...")
    status_flags = {reg: False for reg in STATUS_REGISTERS}

    with ModbusTcpClient(host=STATUS_DEVICE_IP, port=502) as status_client:
        while True:
            for reg in STATUS_REGISTERS:
                status = read_input_status(status_client, reg)
                if status is None:
                    continue

                if status and not status_flags[reg]:
                    level_name = LEVEL_NAMES.get(reg, f"Level {reg}")
                    print(f"[{level_name}] Triggered → Launching capture thread...")

                    if level_name == "High Level":
                        reg_addr = HIGH_LEVEL_ACTUAL_REGISTER
                    elif level_name == "Low Level":
                        reg_addr = LOW_LEVEL_ACTUAL_REGISTER
                    else:
                        reg_addr = ACTUAL_VALUE_REGISTER

                    t = threading.Thread(
                        target=capture_and_insert,
                        args=(level_name, reg_addr),
                        daemon=True
                    )
                    t.start()
                    status_flags[reg] = True

                elif not status and status_flags[reg]:
                    print(f"[{LEVEL_NAMES[reg]}] Reset to 0 → Ready again")
                    status_flags[reg] = False

            time.sleep(0.5)


if __name__ == "__main__":
    main()
