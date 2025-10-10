import struct
import logging
from logging.handlers import TimedRotatingFileHandler
from pymodbus.client import ModbusTcpClient
from time import sleep
import pyodbc
from datetime import datetime


# ----------------------------
# Logging setup (keep last 24h)
# ----------------------------
def setup_logger():
    logger = logging.getLogger("PLC_Logger")
    logger.setLevel(logging.INFO)

    # Handler rotates every 24h and keeps only the last one
    handler = TimedRotatingFileHandler(
        "Analyser_Tcp_IP_error_log.log", when="midnight", interval=1, backupCount=1, encoding="utf-8"
    )
    handler.setLevel(logging.WARNING)

    # Formatter for log entries
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    # Console handler
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.setLevel(logging.INFO)

    logger.addHandler(handler)
    logger.addHandler(console)

    return logger


logger = setup_logger()


# ----------------------------
# Modbus read and decode logic
# ----------------------------
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
        raise ValueError("Unknown byte order")

    return struct.unpack('>f', data)[0]


def read_registers(ip, reg_map, slave_id=1):
    client = ModbusTcpClient(host=ip, port=502, timeout=3)
    decoded = []

    if not client.connect():
        logger.warning(f"Could not connect to PLC {ip}")
        return [0.0] * len(reg_map)

    try:
        max_reg = max(i for i, _ in reg_map) + 2
        response = client.read_input_registers(address=0, count=max_reg)
        if response.isError():
            logger.warning(f"Error reading from PLC {ip}")
            return [0.0] * len(reg_map)

        words = response.registers
        for i, fmt in reg_map:
            if i + 1 >= len(words):
                logger.warning(f"Not enough registers for {ip} at index {i}, inserting 0")
                decoded.append(0.0)
                continue
            try:
                val = decode_float(words[i], words[i + 1], fmt)
                decoded.append(val)
            except Exception as e:
                logger.warning(f"Decode error at {ip} reg {i}: {e}, inserting 0")
                decoded.append(0.0)
    finally:
        client.close()

    return decoded


# ----------------------------
# SQL Server functions
# ----------------------------
def init_db_sqlserver(server_name, database_name, table_name="Value"):
    conn_str = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={server_name};"
        f"Database={database_name};"
        f"Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute(f"""
        IF NOT EXISTS (
            SELECT * FROM sys.tables WHERE name = '{table_name}'
        )
        CREATE TABLE {table_name} (
            [DATE] DATETIME NOT NULL,
            VAL_1 FLOAT NULL,
            VAL_2 FLOAT NULL,
            VAL_3 FLOAT NULL,
            VAL_4 FLOAT NULL,
            VAL_5 FLOAT NULL
        )
    """)
    conn.commit()
    cursor.close()
    return conn


def store_values_sqlserver(conn, values, table_name="Value"):
    cursor = conn.cursor()
    timestamp = datetime.now()
    vals_to_store = values + [None] * (5 - len(values))

    try:
        cursor.execute(
            f"INSERT INTO {table_name} ([DATE], VAL_1, VAL_2, VAL_3, VAL_4, VAL_5) VALUES (?, ?, ?, ?, ?, ?)",
            (timestamp, vals_to_store[0], vals_to_store[1], vals_to_store[2], vals_to_store[3], vals_to_store[4])
        )
        conn.commit()
    except pyodbc.Error as e:
        logger.warning(f"Database write failed: {e}. Attempting reconnection...")
        conn.close()
        raise
    finally:
        cursor.close()


# ----------------------------
# Main continuous loop (24×7)
# ----------------------------
def main():
    server_name = "DESKTOP-F4FK4GN"
    database_name = "DATA"
    conn = init_db_sqlserver(server_name, database_name)

    plc_list = [
        {"ip": "192.168.0.4", "regs": [(0, "ABCD"), (2, "CDAB"), (6, "ABCD")]},
        {"ip": "192.168.0.5", "regs": [(0, "ABCD"), 2, "CDAB",]},  # intentionally left buggy
    ]

    logger.info("✅ Starting 24×7 Modbus monitoring with 24-hour error log rotation...")

    while True:
        try:
            all_values = []
            for plc in plc_list:
                vals = read_registers(plc["ip"], plc["regs"])
                all_values.extend(vals)

            logger.info(f"Read OK @ {datetime.now():%Y-%m-%d %H:%M:%S} → {all_values}")

            try:
                store_values_sqlserver(conn, all_values)
            except pyodbc.Error:
                conn = init_db_sqlserver(server_name, database_name)
                store_values_sqlserver(conn, all_values)

        except KeyboardInterrupt:
            logger.info("🛑 Manual stop detected. Shutting down gracefully...")
            break

        except Exception as e:
            logger.warning(f"Unexpected error: {e}")

        sleep(60)


if __name__ == "__main__":
    main()
