// ===============================================================
// aggDebugBuffer.js
// Internal debug logger (NO console, NO IO)
// ===============================================================

// [ADDED]
const MAX_LOG = 20000; // safe limit (adjust if needed)

// [ADDED]
const buffer = [];

// ===============================================================
// PUSH LOG
// ===============================================================

// [ADDED]
export function pushAggLog(entry) {
  try {

    if (!entry || typeof entry !== "object") return;

    buffer.push({
      ts: Date.now(),
      ...entry,
    });

    // ===========================================================
    // FIFO (prevent memory leak)
    // ===========================================================
    if (buffer.length > MAX_LOG) {
      buffer.shift();
    }

  } catch (_) {
    // silent fail (never break main flow)
  }
}

// ===============================================================
// GET LOGS
// ===============================================================

// [ADDED]
export function getAggLogs({
  tokenAddress = null,
  stage = null,
  tf = null,
  limit = 1000,
} = {}) {

  let logs = buffer;

  if (tokenAddress) {
    logs = logs.filter(l => l.tokenAddress === tokenAddress);
  }

  if (stage) {
    logs = logs.filter(l => l.stage === stage);
  }

  if (tf) {
    logs = logs.filter(l => l.tf === tf);
  }

  return logs.slice(-limit);
}

// ===============================================================
// CLEAR LOG
// ===============================================================

// [ADDED]
export function clearAggLogs() {
  buffer.length = 0;
}

// ===============================================================
// EXPORT FULL (FOR REPORT)
// ===============================================================

// [ADDED]
export function exportAggLogs() {
  return [...buffer];
}