from datetime import datetime, timedelta

def get_shift_range(shift: str, date: str):
    base = datetime.strptime(date, "%Y-%m-%d")
    if shift == "A":
        return base.replace(hour=6), base.replace(hour=14)
    elif shift == "B":
        return base.replace(hour=14), base.replace(hour=22)
    else:
        return base.replace(hour=22), base + timedelta(days=1, hours=6)

def detect_shift(row):
    h = row.hour
    if 6 <= h < 14:
        return "A"
    elif 14 <= h < 22:
        return "B"
    else:
        return "C"
