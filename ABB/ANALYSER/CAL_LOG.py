import threading
import queue
import time
import struct
from datetime import datetime
import sys
import datetime
from pymodbus.client import ModbusTcpClient
import pyodbc


date= datetime.datetime.now()
# ---------------------------------------------------------------------
# STDOUT CONFIG
# ---------------------------------------------------------------------
sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------
# CONFIGURATION (unchanged semantics for offset and CDAB order + factor)
# ---------------------------------------------------------------------
PLC_IP = "192.168.0.4"
RANGE_IP = "127.0.0.1"
SERVER_NAME = "DESKTOP-F4FK4GN"
DATABASE_NAME = "DATA"
TABLE_NAME = "CAL"

# Modbus client defaults
MODBUS_TIMEOUT_S = 3
RETRY_DELAY_S = 5
POLL_INTERVAL_S = 1  # discrete input poll interval
CAPTURE_INTERVAL_S = 10  # capture while trigger active

# Calibration triggers
INPUT_STATUS_REGS = {
    1201: {"name": "N20 ZERO", "delay": 120, "capture": ["N2O", "NO", "O2"]},
    1202: {"name": "N2O SPAN", "delay": 60, "capture": ["N2O"]},
    1203: {"name": "NO SPAN", "delay": 60, "capture": ["NO"]},
    1204: {"name": "O2 SPAN", "delay": 30, "capture": ["O2"]},
}

# Input Register Map: input_reg, byte_order, expected_reg, min_reg, max_reg, factor
# NOTE: DO NOT change byte-order mapping or addressing semantics.
INPUT_REG_MAP = {
    "N2O": (0, "ABCD", 124, 100, 102, 100),
    "NO":  (2, "CDAB", 126, 104, 106, 101),
    "O2":  (6, "ABCD", 128, 108, 110, 102),
}

# ---------------------------------------------------------------------
# UTILS
# ---------------------------------------------------------------------

def decode_float(w1, w2, order="ABCD"):
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
        raise ValueError("Invalid byte order")
    return struct.unpack('>f', data)[0]

# ---------------------------------------------------------------------
# MODBUS CLIENT WRAPPERS (auto-reconnect, thread-safe)
# ---------------------------------------------------------------------

class SafeModbusClient:
    """Thread-safe Modbus TCP client with auto-reconnect."""

    def __init__(self, host: str, timeout: float = MODBUS_TIMEOUT_S):
        self.host = host
        self.timeout = timeout
        self._lock = threading.RLock()
        self._client = None
        self._connect()

    def _connect(self):
        with self._lock:
            if self._client:
                try:
                    self._client.close()
                except Exception:
                    pass
            self._client = ModbusTcpClient(self.host, timeout=self.timeout)
            ok = self._client.connect()
            if not ok:
                print(f"⚠️ Could not connect to {self.host}; retrying in {RETRY_DELAY_S}s...")

    def ensure(self):
        with self._lock:
            if not self._client or not self._client.connected:
                self._connect()
                time.sleep(RETRY_DELAY_S)

    def read_discrete_input(self, address, count=1, unit=1):
        with self._lock:
            self.ensure()
            try:
                return self._client.read_discrete_inputs(address=address, count=count)
            except Exception as e:
                print(f"❌ read_discrete_input({address}) error: {e}")
                return None

    def read_input_registers(self, address, count=2, unit=1):
        with self._lock:
            self.ensure()
            try:
                return self._client.read_input_registers(address=address, count=count)
            except Exception as e:
                print(f"❌ read_input_registers({address}) error: {e}")
                return None

    def read_holding_registers(self, address, count=2, unit=1):
        with self._lock:
            self.ensure()
            try:
                return self._client.read_holding_registers(address=address, count=count)
            except Exception as e:
                print(f"❌ read_holding_registers({address}) error: {e}")
                return None

    def close(self):
        with self._lock:
            try:
                if self._client:
                    self._client.close()
            except Exception:
                pass

# ---------------------------------------------------------------------
# SQL WRITER (producer/consumer)
# ---------------------------------------------------------------------

def connect_sql():
    try:
        conn_str = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={SERVER_NAME};"
            f"Database={DATABASE_NAME};"
            f"Trusted_Connection=yes;"
        )
        return pyodbc.connect(conn_str, timeout=5)
    except Exception as e:
        print(f"❌ Failed to connect to SQL Server: {e}")
        return None

class SQLWriter(threading.Thread):
    def __init__(self, table_name: str, q: queue.Queue, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.table = table_name
        self.q = q
        self.stop_event = stop_event
        self.conn = None

    def run(self):
        while not self.stop_event.is_set():
            try:
                if self.conn is None:
                    self.conn = connect_sql()
                    if self.conn is None:
                        time.sleep(RETRY_DELAY_S)
                        continue

                # Batch insert up to N rows or wait up to 1s
                rows = []
                try:
                    item = self.q.get(timeout=1)
                    rows.append(item)
                    while len(rows) < 100:
                        try:
                            rows.append(self.q.get_nowait())
                        except queue.Empty:
                            break
                except queue.Empty:
                    continue

                cursor = self.conn.cursor()
                for row in rows:
                    cursor.execute(
                        f"""INSERT INTO {self.table} (DATE, CAL_NAME, NAME, VAL, EXPECTED, ERROR, ACCURACY, STATUS)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        row["DATE"], row["CAL_NAME"], row["NAME"],
                        row["VAL"], row["EXPECTED"], row["ERROR"],
                        row["ACCURACY"], row["STATUS"]
                    )
                self.conn.commit()
                print(date,f"✅ Inserted {len(rows)} rows into SQL table {self.table}")
            except pyodbc.Error as e:
                print(f"❌ SQL error: {e}; reconnecting...")
                try:
                    if self.conn:
                        self.conn.close()
                except Exception:
                    pass
                self.conn = None
                time.sleep(RETRY_DELAY_S)
            except Exception as e:
                print(f"❌ SQL writer unexpected error: {e}")
                time.sleep(1)

        # Cleanup
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass

# ---------------------------------------------------------------------
# READING / EVALUATION LOGIC (respecting original factor and CDAB choices)
# ---------------------------------------------------------------------

def read_input_status(plc_client: SafeModbusClient, address: int):
    res = plc_client.read_discrete_input(address, count=1, unit=1)
    try:
        return res.bits[0] if res and not res.isError() else None
    except Exception:
        return None


def read_input_register(plc_client: SafeModbusClient, reg_addr: int, fmt: str):
    res = plc_client.read_input_registers(address=reg_addr, count=2, unit=1)
    if res and not res.isError() and getattr(res, 'registers', None) and len(res.registers) >= 2:
        return decode_float(res.registers[0], res.registers[1], fmt)
    return None


def read_expected_range_from_holding(range_client: SafeModbusClient, tag: str):
    try:
        if tag not in INPUT_REG_MAP:
            print(f"⚠️ Tag '{tag}' not found in INPUT_REG_MAP")
            return None, None, None

        _, _, expected_reg, min_reg, max_reg, _ = INPUT_REG_MAP[tag]
        fmt = "CDAB"  # DO NOT CHANGE per user request

        def read_float(address):
            # DO NOT CHANGE the off-by-one: read at address-1 per original code
            res = range_client.read_holding_registers(address=address-1, count=2, unit=1)
            if res and not res.isError() and getattr(res, 'registers', None) and len(res.registers) >= 2:
                return decode_float(res.registers[0], res.registers[1], fmt)
            else:
                print(f"❌ Failed to read register {address} for {tag}")
                return None

        expected = read_float(expected_reg)
        min_val = read_float(min_reg)
        max_val = read_float(max_reg)

        if expected is not None and min_val is not None and max_val is not None:
            print(f"✅ {tag} Holding Expected={expected}, Min={min_val}, Max={max_val}")
            return expected, min_val, max_val
        else:
            print(f"❌ Could not read complete expected range for {tag}")
            return None, None, None

    except Exception as e:
        print(f"❌ Error reading expected range for {tag}: {e}")
        return None, None, None


def evaluate_value(name: str, val: float, range_client: SafeModbusClient, cal_type: str | None = None):
    # Keep factor semantics unchanged
    _, _, _, _, _, factor = INPUT_REG_MAP[name]

    if cal_type == "N20 ZERO":
        expected = 0.0
        min_range = -0.2
        max_range = 0.2
        print(f"{name} N20 ZERO active. Using fixed range: [0.0, 0.2]")
    else:
        expected, min_range, max_range = read_expected_range_from_holding(range_client, name)
        if expected is None:
            # Maintain return SHAPE (6 values) to avoid unpacking crashes
            # Keep original accuracy rule when expected == 0
            accuracy = abs(val) * factor
            return 0.0, val, accuracy, "READ ERROR", float('nan'), float('nan')

    error = val - expected
    # Keep the exact accuracy formula the same as the original
    accuracy = (abs(error) / expected * factor) if expected != 0 else (abs(val) * factor)
    status = "CAL OK" if (min_range is not None and max_range is not None and min_range <= val <= max_range) else "OUT OF CONTROL"

    return expected, error, accuracy, status, min_range, max_range


def capture_values(plc_client: SafeModbusClient, range_client: SafeModbusClient, cal_name: str, params: list[str]):
    results = []
    timestamp = datetime.datetime.now()
    for param in params:
        addr, fmt, *_ = INPUT_REG_MAP[param]
        val = read_input_register(plc_client, addr, fmt)
        if val is not None:
            expected, error, accuracy, status, min_range, max_range = evaluate_value(param, val, range_client, cal_name)
            try:
                exp_s = f"{expected:.3f}" if expected is not None else "NA"
                min_s = f"{min_range:.3f}" if min_range is not None else "NA"
                max_s = f"{max_range:.3f}" if max_range is not None else "NA"
                print(date, f"{param} | Value: {val:.3f} | Expected: {exp_s} | Min: {min_s} | Max: {max_s} | Error: {error:.3f} | Accuracy: {accuracy:.3f} | Status: {status}")
            except Exception:
                print(date, f"{param} | Value: {val} | Expected: {expected} | Min: {min_range} | Max: {max_range} | Error: {error} | Accuracy: {accuracy} | Status: {status}")

            results.append({
                "DATE": timestamp,
                "CAL_NAME": cal_name,
                "NAME": param,
                "VAL": val,
                "EXPECTED": expected,
                "ERROR": error,
                "ACCURACY": accuracy,
                "STATUS": status
            })
        else:
            print(date, f"{param} | Failed to read sensor value")
    return results

# ---------------------------------------------------------------------
# THREADS: Trigger monitor + per-trigger capture workers
# ---------------------------------------------------------------------

class CaptureWorker(threading.Thread):
    def __init__(self, reg: int, cal_name: str, delay_s: int, capture_tags: list[str],
                 plc_client: SafeModbusClient, range_client: SafeModbusClient,
                 out_q: queue.Queue, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.reg = reg
        self.cal_name = cal_name
        self.delay_s = delay_s
        self.tags = capture_tags
        self.plc = plc_client
        self.range = range_client
        self.q = out_q
        self.stop_event = stop_event

    def run(self):
        print(date,f"▶️ {self.cal_name} triggered. Waiting {self.delay_s} seconds before capture...")
        # Initial stabilization delay
        end_wait = time.time() + self.delay_s
        while time.time() < end_wait and not self.stop_event.is_set():
            time.sleep(0.2)

        # Capture loop while trigger stays active
        while not self.stop_event.is_set():
            status = read_input_status(self.plc, self.reg)
            if not status:
                print(date,f"⏹ {self.cal_name} trigger ended.")
                break

            results = capture_values(self.plc, self.range, self.cal_name, self.tags)
            for row in results:
                self.q.put(row)
            time.sleep(CAPTURE_INTERVAL_S)


class TriggerMonitor(threading.Thread):
    def __init__(self, plc_client: SafeModbusClient, range_client: SafeModbusClient,
                 out_q: queue.Queue, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.plc = plc_client
        self.range = range_client
        self.q = out_q
        self.stop_event = stop_event
        self.last_status = {reg: False for reg in INPUT_STATUS_REGS}
        self.active_workers: dict[int, CaptureWorker] = {}
        self._lock = threading.RLock()

    def run(self):
        print(date,"🟢 Monitoring calibration input triggers...")
        while not self.stop_event.is_set():
            for reg, info in INPUT_STATUS_REGS.items():
                cal_name = info["name"]
                delay = info["delay"]
                capture_tags = info["capture"]

                status = read_input_status(self.plc, reg)
                if status is None:
                    # Read failed; skip this cycle but keep polling
                    continue

                # Rising edge: start a worker if not already running
                if status and not self.last_status[reg]:
                    with self._lock:
                        if reg not in self.active_workers or not self.active_workers[reg].is_alive():
                            worker = CaptureWorker(reg, cal_name, delay, capture_tags,
                                                   self.plc, self.range, self.q, self.stop_event)
                            self.active_workers[reg] = worker
                            worker.start()

                # Falling edge: worker will self-terminate; we update status
                self.last_status[reg] = bool(status)

            time.sleep(POLL_INTERVAL_S)

# ---------------------------------------------------------------------
# MAIN ENTRY
# ---------------------------------------------------------------------

def main():
    stop_event = threading.Event()

    plc_client = SafeModbusClient(PLC_IP, timeout=MODBUS_TIMEOUT_S)
    range_client = SafeModbusClient(RANGE_IP, timeout=MODBUS_TIMEOUT_S)

    out_q: queue.Queue = queue.Queue(maxsize=10000)

    sql_thread = SQLWriter(TABLE_NAME, out_q, stop_event)
    sql_thread.start()

    monitor = TriggerMonitor(plc_client, range_client, out_q, stop_event)
    monitor.start()

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("👋 Shutting down...")
    finally:
        stop_event.set()
        monitor.join(timeout=5)
        sql_thread.join(timeout=5)
        plc_client.close()
        range_client.close()
        print("✅ Clean exit")


if __name__ == "__main__":
    main()
