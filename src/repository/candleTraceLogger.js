// ===============================================================
// candleTraceLogger.js
// FULL TRACE logging for candle debugging (replayable)
// ===============================================================

import fs from "fs";
import path from "path";

const LOG_DIR = "./logs/candle-trace"; // [ADDED]

if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

// [ADDED] async queue biar gak blocking
const queue = [];
let isWriting = false;

function flushQueue() {
  if (isWriting || queue.length === 0) return;

  isWriting = true;

  const item = queue.shift();

  fs.appendFile(item.file, item.data, (err) => {
    if (err) console.error("[TRACE LOG ERROR]", err.message);
    isWriting = false;
    flushQueue();
  });
}

// ===============================================================
// MAIN LOGGER
// ===============================================================

export function traceCandle(tokenAddress, payload) {

  try {

    const file = path.join(LOG_DIR, `${tokenAddress}.log`);

    const log = JSON.stringify({
      ts: Date.now(),
      ...payload
    }) + "\n";

    queue.push({ file, data: log });
    flushQueue();

  } catch (err) {
    console.error("[TRACE LOG FAIL]", err.message);
  }

}