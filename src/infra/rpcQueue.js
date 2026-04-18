// ===============================================================
// rpcQueue.js
// Semua RPC call lewat sini — rate limited + cached + auto-retry
// ChainStack free tier: 25 req/detik
// Strategi: max 18 req/detik + antrian FIFO + retry saat kena -32005
// ===============================================================

import { rpcLogsProvider, rpcTxProvider } from "./provider.js";

// ================= CONFIG =================

const MAX_RPS       = 18;     // aman di bawah limit 25
const INTERVAL_MS   = 1000;
const CACHE_TTL_MS  = 8000;   // 8 detik ~ 2-3 block
const CACHE_MAX     = 2000;
const MAX_RETRY     = 3;      // retry saat kena -32005

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

    // Jalankan dengan auto-retry kalau kena rate limit -32005
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
        err?.code === -32005 ||
        err?.message?.includes("exceeded the RPS");

      if (!isRateLimit || i === retries - 1) throw err;

      // Baca try_again_in dari response jika ada
      const tryAgainMs =
        err?.error?.data?.try_again_in
          ? Math.ceil(parseFloat(err.error.data.try_again_in) * 1000)
          : (i + 1) * 300;

      console.warn(`[QUEUE] Rate limit hit, retry ${i + 1}/${retries} in ${tryAgainMs}ms`);
      await sleep(tryAgainMs);

    }

  }

}

function enqueue(fn) {
  return new Promise((resolve, reject) => {
    queue.push({ fn, resolve, reject });
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

function cacheGet(map, key) {
  const entry = map.get(key);
  if (!entry) return null;
  if (Date.now() - entry.ts > CACHE_TTL_MS) {
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

// ================= PUBLIC API =================

export function getLogs(filter) {
  return enqueue(() => rpcLogsProvider.getLogs(filter));
}

export function getBlock(blockNumber) {
  const cached = cacheGet(blockCache, blockNumber);
  if (cached) return Promise.resolve(cached);

  return enqueue(async () => {
    const data = await rpcTxProvider.getBlock(blockNumber);
    cacheSet(blockCache, blockNumber, data);
    return data;
  });
}

export function getTransaction(txHash) {
  const cached = cacheGet(txCache, txHash);
  if (cached) return Promise.resolve(cached);

  return enqueue(async () => {
    const data = await rpcTxProvider.getTransaction(txHash);
    if (data) cacheSet(txCache, txHash, data);
    return data;
  });
}

export function getTransactionReceipt(txHash) {
  const cached = cacheGet(receiptCache, txHash);
  if (cached) return Promise.resolve(cached);

  return enqueue(async () => {
    const data = await rpcTxProvider.getTransactionReceipt(txHash);
    if (data) cacheSet(receiptCache, txHash, data);
    return data;
  });
}

export async function getContractFields(contract, fields) {
  const results = {};
  for (const [key, fn] of Object.entries(fields)) {
    try {
      results[key] = await enqueue(() => fn());
    } catch {
      results[key] = null;
    }
  }
  return results;
}

export function getQueueStats() {
  return {
    queueLength : queue.length,
    reqCount,
    windowStart,
    cacheSize   : {
      tx      : txCache.size,
      receipt : receiptCache.size,
      block   : blockCache.size
    }
  };
}