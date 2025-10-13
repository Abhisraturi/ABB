import os
import time
import requests
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


BASE_FOLDER = r"E:\COBBLE DETECTION\PLC_JSON"
API_URL = "http://104.154.22.115:8001/app/v0.0.1/plc/raw-post"
PAYLOAD_BASE = os.path.join(BASE_FOLDER, "Payload")
MAX_WORKERS = 20
POLL_INTERVAL = 0.002
KEEP_MINUTES = 5  

sent_files = set()
last_folder = None


def save_payload(payload):
    folder_name = "Payload_" + datetime.now().strftime("%Y%m%d_%H%M")
    folder_path = os.path.join(PAYLOAD_BASE, folder_name)
    os.makedirs(folder_path, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    file_path = os.path.join(folder_path, f"payload_{ts}.json")
    with open(file_path, "w") as f:
        json.dump(payload, f, indent=4)

    # Cleanup folders 
    cutoff = datetime.now() - timedelta(minutes=KEEP_MINUTES)
    if os.path.exists(PAYLOAD_BASE):
        for folder in os.listdir(PAYLOAD_BASE):
            folder_full = os.path.join(PAYLOAD_BASE, folder)
            if os.path.isdir(folder_full) and folder.startswith("Payload_"):
                try:
                    folder_time = datetime.strptime(folder[8:], "%Y%m%d_%H%M")
                    if folder_time < cutoff:
                        # delete old folder
                        for f in os.listdir(folder_full):
                            os.remove(os.path.join(folder_full, f))
                        os.rmdir(folder_full)
                        print(f"?? Deleted old payload folder: {folder_full}")
                except Exception:
                    continue


def send_file(file_path):
    
    try:
        with open(file_path, "r") as f:
            original_data = json.load(f)

        # Wrap original data inside 'tags' and add timestamp
        data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "tags": original_data
        }

        # Send JSON payload
        response = requests.post(API_URL, json=data, timeout=10)

        if response.status_code == 200:
            save_payload(data) 

        return file_path, response.status_code, response.text
    except Exception as e:
        return file_path, None, str(e)


def get_latest_folder(base_folder):
    json_folders = [
        f for f in os.listdir(base_folder)
        if os.path.isdir(os.path.join(base_folder, f)) and f.startswith("JSON_")
    ]
    if not json_folders:
        return None
    json_folders.sort(key=lambda x: datetime.strptime(x[5:], "%Y%m%d_%H%M"))
    return os.path.join(base_folder, json_folders[-1])


def get_json_files(folder_path):
    return sorted(
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".json")
    )


def main():
    global last_folder
    print("?? Starting  continuous JSON upload with payload folders...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while True:
            latest_folder = get_latest_folder(BASE_FOLDER)
            if latest_folder:
                if latest_folder != last_folder:
                    last_folder = latest_folder
                    sent_files.clear()
                    print(f"?? Switched to new folder: {latest_folder}")

                json_files = get_json_files(latest_folder)
                new_files = [f for f in json_files if f not in sent_files]

                if new_files:
                    futures = [executor.submit(send_file, f) for f in new_files]

                    for future in as_completed(futures):
                        file_path, status, text = future.result()
                        sent_files.add(file_path)
                        print(f"? {file_path} -> Status: {status}, Response: {text[:200]}")

            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
