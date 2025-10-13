const fs = require("fs");
const path = require("path");
const axios = require("axios");

const BASE_FOLDER = "C:\\My_Data\\PLC_JSON";
const API_URL = "http://104.154.22.115:8001/app/v0.0.1/plc/raw-post";
const BURST_SIZE = 40;

let successCount = 0;
let failCount = 0;

function getLatestFolder() {
  const folders = fs.readdirSync(BASE_FOLDER)
    .map(f => path.join(BASE_FOLDER, f))
    .filter(f => fs.lstatSync(f).isDirectory() && !f.endsWith("sent"));

  if (!folders.length) return null;
  return folders.sort().pop();
}

function loadFiles(folder) {
  const files = fs.readdirSync(folder)
    .filter(f => f.endsWith(".json"))
    .map(f => path.join(folder, f));
  return files;
}

function formatTimestamp() {
  const d = new Date();
  return d.toISOString().replace("T", " ").replace("Z", "");
}

async function sendFile(filePath) {
  try {
    const raw = fs.readFileSync(filePath, "utf8");
    const json = JSON.parse(raw);

    const payload = {
      Timestamp: formatTimestamp(),
      tags: json
    };

    await axios.post(API_URL, payload, { headers: { "Content-Type": "application/json" } });
    successCount++;
    console.log(`✅ Sent: ${path.basename(filePath)}`);
    return true;
  } catch (err) {
    failCount++;
    console.error(`❌ Failed: ${path.basename(filePath)} - ${err.message}`);
    return false;
  }
}

async function sendBurst(files) {
  const promises = files.map(f => sendFile(f));
  await Promise.all(promises);
}

async function runSender() {
  const folder = getLatestFolder();
  if (!folder) {
    console.error("❌ No folder found");
    return;
  }

  const allFiles = loadFiles(folder);
  console.log(`📂 Loaded ${allFiles.length} files from folder: ${folder}`);

  while (allFiles.length) {
    const burstFiles = allFiles.splice(0, BURST_SIZE);
    sendBurst(burstFiles); // do NOT await, fire-and-forget
    await new Promise(r => setTimeout(r, 30)); // next burst after 30ms
  }

  // Log summary every minute
  setInterval(() => {
    console.log(`⏱ Minute summary -> Success: ${successCount}, Fail: ${failCount}`);
    successCount = 0;
    failCount = 0;
  }, 60000);
}

runSender();
