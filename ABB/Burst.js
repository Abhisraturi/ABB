const fs = require("fs");
const path = require("path");
const axios = require("axios");

const BASE_FOLDER = "C:\\My_Data\\PLC_JSON";
const API_URL = "http://104.154.22.115:8001/app/v0.0.1/plc/raw-post";
const BURST_SIZE = 10; // send 40 files per burst
const INTERVAL_MS = 30; // 30 ms apart

// Format timestamp like: "2025-08-13 10:25:49.074038"
function getFormattedTimestamp() {
  const now = new Date();
  const pad = (n, z = 2) => n.toString().padStart(z, "0");

  const year = now.getFullYear();
  const month = pad(now.getMonth() + 1);
  const day = pad(now.getDate());

  const hour = pad(now.getHours());
  const min = pad(now.getMinutes());
  const sec = pad(now.getSeconds());
  const ms = pad(now.getMilliseconds(), 3);

  // microseconds (random 3 digits)
  const micro = pad(Math.floor(Math.random() * 1000), 3);

  return `${year}-${month}-${day} ${hour}:${min}:${sec}.${ms}${micro}`;
}

// Load all JSON files from the latest folder
function loadFiles() {
  const folders = fs.readdirSync(BASE_FOLDER)
    .map(f => path.join(BASE_FOLDER, f))
    .filter(f => fs.lstatSync(f).isDirectory() && path.basename(f) !== "sent");

  if (!folders.length) return [];

  const latestFolder = folders.sort().pop();
  const files = fs.readdirSync(latestFolder)
    .filter(f => f.endsWith(".json"))
    .map(f => path.join(latestFolder, f))
    .sort(); // ensure consistent order

  return files;
}

// Send a single file asynchronously
async function sendFile(filePath) {
  const fileName = path.basename(filePath);
  try {
    const raw = fs.readFileSync(filePath, "utf8");
    const original = JSON.parse(raw);
    const payload = {
      timestamp: getFormattedTimestamp(),
      tags: original
    };

    // Fire & forget, don't await response to maintain 30ms frequency
    axios.post(API_URL, payload, { timeout: 10000 })
      .then(res => console.log(`✅ Sent: ${fileName}, status: ${res.status}`))
      .catch(err => console.error(`❌ Failed: ${fileName}, error: ${err.message}`));
  } catch (err) {
    console.error(`❌ Failed reading/parsing ${fileName}:`, err.message);
  }
}

// Main loop to send bursts
async function burstSend() {
  let files = loadFiles();
  let queue = [...files];

  setInterval(() => {
    if (!queue.length) {
      files = loadFiles();
      queue = [...files];
      console.log(`🔄 Loaded ${queue.length} files from latest folder`);
    }

    const burst = queue.splice(0, BURST_SIZE);
    burst.forEach(file => sendFile(file));

  }, INTERVAL_MS);
}

burstSend();
