import os

BASE_DIR = r"D:\AI_PROGRAMS\PLC_JSON"

print("Checking folder:", repr(BASE_DIR))
print("Exists?", os.path.exists(BASE_DIR))
print("Is dir?", os.path.isdir(BASE_DIR))

# If folder exists, list its contents
if os.path.exists(BASE_DIR):
    print("Contents:", os.listdir(BASE_DIR))
else:
    print("❌ Folder not found. Creating it now...")
    os.makedirs(BASE_DIR, exist_ok=True)
    print("✅ Folder created."