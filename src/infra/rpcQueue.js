import { rpcLogsProvider, rpcTxProvider } from "./provider.js";

// ================= CONFIG =================

const MAX_RPS        = 18;
const INTERVAL_MS    = 1000;
const MAX_RETRY      = 3;
const MAX_QUEUE_SIZE = 500;   // drop oldest if queue too large

// cache TTLs
const TTL_TX      = 0;        // tx/receipt immutable — never expire
const TTL_BLOCK   = 0;        // block immutable — never expire
const TTL_LOGS    = 4_000;    // logs cache 4s (2 blocks)
const CACHE_MAX   = 3_000;

// ================= RATE LIMITER =================

let reqCount    = 0;
let windowStart = Date.now();
const queue     = [];
let processing  = false;

async function processQueue() {
  if (processing) return;
  processing = true;

  while (queue.length > 0) {
    const now = Date.now();

    if (now - windowStart >= INTERVAL_MS) {
      reqCount    = 0;
      windowStart = now;
    }

    if (reqCount >= MAX_RPS) {
      const wait = INTERVAL_MS - (now - windowStart);
      await sleep(wait > 0 ? wait : 50);
      continue;
    }

    const { fn, resolve, reject } = queue.shift();
    reqCount++;

    _runWithRetry(fn, MAX_RETRY).then(resolve).catch(reject);
  }

  processing = false;
}

async function _runWithRetry(fn, retries) {
  for (let i = 0; i < retries; i++) {
    try {
      return await fn();
    } catch (err) {
      const isRateLimit =
        err?.error?.code === -32005 ||
        err?.code        === -32005 ||
        err?.message?.includes("exceeded the RPS");

      if (!isRateLimit || i === retries - 1) throw err;

      const tryAgainMs = err?.error?.data?.try_again_in
        ? Math.ceil(parseFloat(err.error.data.try_again_in) * 1000)
        : (i + 1) * 300;

      console.warn(`[QUEUE] Rate limit hit, retry ${i + 1}/${retries} in ${tryAgainMs}ms`);
      await sleep(tryAgainMs);
    }
  }
}

function enqueue(fn, priority = false) {
  return new Promise((resolve, reject) => {
    // drop oldest non-priority when queue too large
    if (queue.length >= MAX_QUEUE_SIZE) {
      const dropIdx = queue.findIndex(e => !e.priority);
      if (dropIdx !== -1) {
        const dropped = queue.splice(dropIdx, 1)[0];
        dropped.reject(new Error("queue_overflow"));
      } else {
        reject(new Error("queue_full"));
        return;
      }
    }

    const entry = { fn, resolve, reject, priority };
    if (priority) {
      queue.unshift(entry);
    } else {
      queue.push(entry);
    }

    processQueue();
  });
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ================= CACHE =================

const txCache      = new Map();
const receiptCache = new Map();
const blockCache   = new Map();
const logsCache    = new Map();

function cacheGet(map, key, ttl) {
  const entry = map.get(key);
  if (!entry) return null;
  if (ttl > 0 && Date.now() - entry.ts > ttl) {
    map.delete(key);
    return null;
  }
  return entry.data;
}

function cacheSet(map, key, data) {
  if (map.size >= CACHE_MAX) {
    map.delete(map.keys().next().value);
  }
  map.set(key, { data, ts: Date.now() });
}

function logsKey(filter) {
  return JSON.stringify({
    address:   filter.address,
    topics:    filter.topics,
    fromBlock: filter.fromBlock,
    toBlock:   filter.toBlock,
  });
}

// ================= PUBLIC API =================

export function getLogs(filter) {
  const key    = logsKey(filter);
  const cached = cacheGet(logsCache, key, TTL_LOGS);
  if (cached) return Promise.resolve(cached);

  return enqueue(async () => {
    const data = await rpcLogsProvider.getLogs(filter);
    cacheSet(logsCache, key, data);
    return data;
  });
}

export function getBlock(blockNumber) {
  const cached = cacheGet(blockCache, blockNumber, TTL_BLOCK);
  if (cached) return Promise.resolve(cached);

  return enqueue(async () => {
    const data = await rpcTxProvider.getBlock(blockNumber);
    if (data) cacheSet(blockCache, blockNumber, data);
    return data;
  });
}

export function getTransaction(txHash) {
  const cached = cacheGet(txCache, txHash, TTL_TX);
  if (cached) return Promise.resolve(cached);

  return enqueue(async () => {
    const data = await rpcTxProvider.getTransaction(txHash);
    if (data) cacheSet(txCache, txHash, data);
    return data;
  });
}

export function getTransactionReceipt(txHash) {
  const cached = cacheGet(receiptCache, txHash, TTL_TX);
  if (cached) return Promise.resolve(cached);

  return enqueue(async () => {
    const data = await rpcTxProvider.getTransactionReceipt(txHash);
    if (data) cacheSet(receiptCache, txHash, data);
    return data;
  });
}

export async function getContractFields(contract, fields) {
  const entries = Object.entries(fields);
  const results = await Promise.allSettled(
    entries.map(([, fn]) => enqueue(() => fn()))
  );
  const out = {};
  entries.forEach(([key], i) => {
    out[key] = results[i].status === "fulfilled" ? results[i].value : null;
  });
  return out;
}

// ================= STATS =================

export function getQueueStats() {
  return {
    queueLength: queue.length,
    reqCount,
    windowStart,
    cacheSize: {
      tx:      txCache.size,
      receipt: receiptCache.size,
      block:   blockCache.size,
      logs:    logsCache.size,
    }
  };
}

// ================= PERIODIC CLEANUP =================
// Prevent unbounded cache growth for logs (only logs has TTL)

setInterval(() => {
  const now = Date.now();
  for (const [k, v] of logsCache) {
    if (now - v.ts > TTL_LOGS * 5) logsCache.delete(k);
  }
}, 60_000);