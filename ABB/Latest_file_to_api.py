import os
import json
import time
import asyncio
import aiohttp
from datetime import datetime, timezone
from collections import OrderedDict

folder_path = r"E:\COBBLE DETECTION"
sent_folder = os.path.join(folder_path, "sent")
api_url = "http://104.154.22.115:8001/app/v0.0.1/plc/raw-post"
poll_interval = 0.05
max_sent_files = 100

os.makedirs(sent_folder, exist_ok=True)

def get_latest_json_file(folder_path):
    json_files = [f for f in os.listdir(folder_path) if f.endswith(".json")]
    if not json_files:
        return None
    json_paths = [os.path.join(folder_path, f) for f in json_files]
    return max(json_paths, key=os.path.getmtime)

def read_and_format_json(file_path):
    with open(file_path, "r") as f:
        raw_data = json.load(f)

    # Current UTC timestamp
    current_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # If file already has a timestamp, keep it, else add
    timestamp_value = raw_data.get("timestamp", current_time_utc)

    # Extract tags (ensure they exist)
    tags_value = raw_data.get("tags", raw_data)

    # Create an OrderedDict to enforce order: timestamp first, then tags
    ordered_json = OrderedDict()
    ordered_json["timestamp"] = timestamp_value
    ordered_json["tags"] = tags_value

    return ordered_json

def cleanup_sent_folder():
    sent_files = [os.path.join(sent_folder, f) for f in os.listdir(sent_folder)]
    sent_files.sort(key=os.path.getmtime, reverse=True)
    if len(sent_files) > max_sent_files:
        for old_file in sent_files[max_sent_files:]:
            os.remove(old_file)

def save_curl_format(file_name, json_data):
    json_pretty = json.dumps(json_data, indent=2)
    curl_format = (
        "--header 'Content-Type: application/json' \\\n"
        f"--data '{json_pretty}'"
    )
    timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = os.path.join(sent_folder, f"sent_curl_{timestamp_str}.txt")
    with open(log_filename, "w") as f:
        f.write(curl_format)

async def send_data_to_api(session, json_data, file_name):
    headers = {"Content-Type": "application/json"}
    try:
        start_time = time.time()
        async with session.post(api_url, json=json_data, headers=headers, timeout=5) as resp:
            response_text = await resp.text()
            total_time = time.time() - start_time
            print(f"File: {file_name} | Status: {resp.status} | Time: {total_time:.4f}s | Response: {response_text.strip()}")

            # Save curl-style format for reference
            save_curl_format(file_name, json_data)

            return resp.status == 200
    except Exception as e:
        print(f"Error sending {file_name}: {e}")
        return False

async def process_latest_file(session):
    latest_file = get_latest_json_file(folder_path)
    if not latest_file:
        return
    
    file_name = os.path.basename(latest_file)
    print(f"Reading file: {file_name}")
    
    # Read JSON, add timestamp if missing, enforce timestamp first
    json_data = read_and_format_json(latest_file)
    
    success = await send_data_to_api(session, json_data, file_name)
    
    if success:
        os.replace(latest_file, os.path.join(sent_folder, file_name))
        cleanup_sent_folder()

async def main_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            await process_latest_file(session)
            await asyncio.sleep(poll_interval)

if __name__ == "__main__":
    asyncio.run(main_loop())
#new