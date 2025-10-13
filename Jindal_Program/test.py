import os
from datetime import datetime, timedelta

def generate_fake_logs(start_time: str, end_time: str, out_file="fake_logs.log"):
    """
    Generate fake PLC/API logs every second between start_time and end_time.
    Format: YYYY-MM-DD HH:MM:SS | [LEVEL] message
    """
    start = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    with open(out_file, "w", encoding="utf-8") as f:
        t = start
        count = 0
        while t <= end:
            if count % 60 == 0:  # every 60 sec add an API warning
                log_line = f"{t.strftime('%Y-%m-%d %H:%M:%S')} | [WARNING] [WATCHDOG] No API success in 10s (strike 1/3)\n"
            else:
                log_line = f"{t.strftime('%Y-%m-%d %H:%M:%S')} | [WARNING] PLC connect error: b' TCP : Unreachable peer'\n"

            f.write(log_line)
            t += timedelta(seconds=1)
            count += 1

    print(f"✅ Fake logs written to {out_file} ({count:,} lines)")

if __name__ == "__main__":
    # Change these times as needed
    generate_fake_logs("2025-09-19 18:16:45", "2025-09-20 08:02:00")
