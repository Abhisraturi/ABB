import os, json, asyncio, aiohttp, time
from datetime import datetime, timezone
from collections import OrderedDict

folder_path = r"E:\COBBLE DETECTION"
api_url = "http://104.154.22.115:8001/app/v0.0.1/plc/raw-post"
sent_folder = os.path.join(folder_path, "sent")
os.makedirs(sent_folder, exist_ok=True)

def get_latest_json_file(folder):
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".json")]
    valid = []
    for f in files:
        try:
            os.path.getmtime(f)
            valid.append(f)
        except FileNotFoundError:
            pass
    return max(valid, key=os.path.getmtime) if valid else None

def read_and_format_json_safe(file_path):
    try:
        with open(file_path, "r") as f:
            raw_data = json.load(f)
            tags = raw_data.get("tags", raw_data)
    except (json.JSONDecodeError, FileNotFoundError):
        print(f"⚠ Error reading {file_path}, using empty tags.")
        tags = {}  # ✅ FIX: prevent UnboundLocalError if JSON is invalid
    t = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    return OrderedDict([("timestamp", t), ("tags", tags)])

def save_payload_txt(payload):
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")
    fname = os.path.join(sent_folder, f"payload_{ts}.txt")
    with open(fname, "w") as f:
        f.write(json.dumps(payload, indent=2))
    # keep only last 5 txt
    txt_files = [os.path.join(sent_folder, f) for f in os.listdir(sent_folder) if f.startswith("payload_")]
    if len(txt_files) > 5:
        txt_files.sort(key=os.path.getmtime, reverse=True)
        for old in txt_files[5:]:
            try:
                os.remove(old)
            except:
                pass

async def send_payload(session, payload):
    start = time.time()
    save_payload_txt(payload)
    try:
        await session.post(api_url, json=payload, timeout=2)
    except Exception as e:
        print("⚠ Send error:", e)
    elapsed = time.time() - start
    print(f"➡ Sent payload in {elapsed:.4f}s")

async def main_loop():
    async with aiohttp.ClientSession() as session:
        last_file = None
        last_mtime = None
        current_payload = None
        
        while True:
            latest = get_latest_json_file(folder_path)
            if latest:
                mtime = os.path.getmtime(latest)
                if latest != last_file or mtime != last_mtime:
                    new_payload = read_and_format_json_safe(latest)
                    if new_payload:
                        current_payload = new_payload
                        last_file = latest
                        last_mtime = mtime
            
            if current_payload:
                await send_payload(session, current_payload)
            
            # No sleep as per your requirement (tight loop)

if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main_loop())
        except Exception as e:
            print("Error:", e)
