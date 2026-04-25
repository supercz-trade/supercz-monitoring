/**
 * rpcQueue.js
 *
 * Multi-provider RPC queue with:
 *  - Latency-aware provider selection (EWMA scoring)
 *  - Per-provider circuit breaker + automatic recovery
 *  - Global rate limiter (MAX_RPS across all providers)
 *  - Priority queue (stable ordering within same priority)
 *  - True LRU cache per call type
 *  - Adaptive timeout per call type
 *
 * Imports URL pools from provider.js — does not create WebSocket providers.
 */

import { JsonRpcProvider } from "ethers";
import { LOGS_RPC_URLS, TX_RPC_URLS } from "./provider.js";

// ================= CONFIG =================

const MAX_RPS        = 18;   // global across all providers combined
const INTERVAL_MS    = 1_000;
const MAX_RETRY      = 3;
const MAX_QUEUE_SIZE = 500;

// Adaptive timeouts (ms) per call type
const TIMEOUT = {
  logs:        8_000,   // getLogs on large ranges can be slow
  block:       4_000,
  transaction: 4_000,
  receipt:     4_000,
  contract:    5_000,
};

// Cache TTLs
const TTL_TX    = 0;       // immutable — never expire
const TTL_BLOCK = 0;       // immutable — never expire
const TTL_LOGS  = 4_000;   // ~2 BSC blocks
const CACHE_MAX = 3_000;

// Per-provider circuit breaker
const CB_FAILURE_THRESHOLD = 5;
const CB_RESET_MS          = 15_000;

// Latency scoring — EWMA (exponential weighted moving average)
// score = p(estimated latency ms); lower = better = preferred
const EWMA_ALPHA = 0.2;     // 0.2 = 20% new sample, 80% history
const PENALTY_MS = 5_000;   // added to score on each hard failure

// ================= PROVIDER POOL =================

/**
 * Build ethers JsonRpcProvider entries from a URL list.
 * Each entry owns: provider instance, circuit breaker state, EWMA score.
 */
function buildPool(urls) {
  return urls.map(url => ({
    url,
    provider:  new JsonRpcProvider(url, "bnb", { staticNetwork: true }),
    failures:  0,
    openSince: null,
    score:     500,   // initial assumed latency (ms)
  }));
}

const LOGS_POOL = buildPool(LOGS_RPC_URLS);
const TX_POOL   = buildPool(TX_RPC_URLS);

// ================= CIRCUIT BREAKER / SCORING =================

function isOpen(entry) {
  if (entry.openSince === null) return false;
  if (Date.now() - entry.openSince >= CB_RESET_MS) {
    entry.openSince = null;
    entry.failures  = 0;
    return false; // half-open: allow one probe
  }
  return true;
}

function recordSuccess(entry, latencyMs) {
  entry.failures = 0;
  entry.score    = EWMA_ALPHA * latencyMs + (1 - EWMA_ALPHA) * entry.score;
}

function recordFailure(entry, isRateLimit) {
  if (isRateLimit) return; // rate limit handled by retry backoff, not CB
  entry.failures++;
  entry.score += PENALTY_MS;
  if (entry.failures >= CB_FAILURE_THRESHOLD && entry.openSince === null) {
    entry.openSince = Date.now();
    console.warn(`[POOL] Circuit open: ${entry.url}`);
  }
}

/** Pick available provider with lowest EWMA latency score. */
function pickBest(pool) {
  const available = pool.filter(e => !isOpen(e));
  if (!available.length) return null;
  return available.reduce((a, b) => (a.score <= b.score ? a : b));
}

function allDown() {
  return !pickBest(LOGS_POOL) && !pickBest(TX_POOL);
}

// ================= RATE LIMITER =================

let inFlight    = 0;
let reqCount    = 0;
let windowStart = Date.now();

const queue    = [];
let processing = false;

async function processQueue() {
  if (processing) return;
  processing = true;

  while (queue.length > 0) {
    if (allDown()) {
      while (queue.length > 0) {
        queue.shift().reject(new Error("all_providers_unavailable"));
      }
      break;
    }

    const now = Date.now();
    if (now - windowStart >= INTERVAL_MS) {
      reqCount    = 0;
      windowStart = now;
    }

    if (reqCount >= MAX_RPS) {
      const wait = INTERVAL_MS - (Date.now() - windowStart);
      await sleep(wait > 0 ? wait : 50);
      continue;
    }

    const item = queue.shift();
    reqCount++;
    inFlight++;

    _runWithRetry(item.fn, MAX_RETRY)
      .then(v  => { inFlight--; item.resolve(v); })
      .catch(e => { inFlight--; item.reject(e);  });
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

      console.warn(`[QUEUE] Rate limit, retry ${i + 1}/${retries} in ${tryAgainMs}ms`);
      await sleep(tryAgainMs);
    }
  }
}

function enqueue(fn, priority = false) {
  return new Promise((resolve, reject) => {
    if (queue.length >= MAX_QUEUE_SIZE) {
      const dropIdx = queue.findIndex(e => !e.priority);
      if (dropIdx !== -1) {
        queue.splice(dropIdx, 1)[0].reject(new Error("queue_overflow"));
      } else {
        reject(new Error("queue_full"));
        return;
      }
    }

    const entry = { fn, resolve, reject, priority };
    if (priority) {
      const insertAt = queue.findIndex(e => !e.priority);
      if (insertAt === -1) queue.push(entry);
      else queue.splice(insertAt, 0, entry);
    } else {
      queue.push(entry);
    }

    processQueue();
  });
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ================= PROVIDER CALL WRAPPER =================
//
// Tries providers in score order (fastest first).
// Timeout:    record failure, failover to next provider.
// Hard error: record failure, failover to next provider.
// Rate limit: throw immediately — _runWithRetry handles backoff.

async function callWithPool(pool, method, args, timeoutMs) {
  const sorted = [...pool]
    .filter(e => !isOpen(e))
    .sort((a, b) => a.score - b.score);

  if (!sorted.length) throw new Error("all_providers_unavailable");

  let lastErr;

  for (const entry of sorted) {
    const t0 = Date.now();
    try {
      const result = await withTimeout(
        entry.provider[method](...args),
        timeoutMs,
        `${method} timeout (${timeoutMs}ms) — ${entry.url}`
      );
      recordSuccess(entry, Date.now() - t0);
      return result;
    } catch (err) {
      const isRateLimit =
        err?.error?.code === -32005 ||
        err?.code        === -32005 ||
        err?.message?.includes("exceeded the RPS");

      recordFailure(entry, isRateLimit);
      if (isRateLimit) throw err;

      console.warn(`[POOL] ${entry.url} → ${method} failed: ${err.message}`);
      lastErr = err;
    }
  }

  throw lastErr ?? new Error("all_providers_failed");
}

function withTimeout(promise, ms, label) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(label)), ms);
    promise.then(
      v => { clearTimeout(t); resolve(v); },
      e => { clearTimeout(t); reject(e);  }
    );
  });
}

// ================= CACHE (true LRU) =================

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
  map.delete(key);
  map.set(key, entry); // promote to MRU
  return entry.data;
}

function cacheSet(map, key, data) {
  if (map.size >= CACHE_MAX) {
    map.delete(map.keys().next().value); // evict LRU
  }
  map.delete(key);
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

export function getLogs(filter, priority = false) {
  const key    = logsKey(filter);
  const cached = cacheGet(logsCache, key, TTL_LOGS);
  if (cached) return Promise.resolve(cached);

  return enqueue(async () => {
    const data = await callWithPool(LOGS_POOL, "getLogs", [filter], TIMEOUT.logs);
    cacheSet(logsCache, key, data);
    return data;
  }, priority);
}

export function getBlock(blockNumber, priority = false) {
  const cached = cacheGet(blockCache, blockNumber, TTL_BLOCK);
  if (cached) return Promise.resolve(cached);

  return enqueue(async () => {
    const data = await callWithPool(TX_POOL, "getBlock", [blockNumber], TIMEOUT.block);
    if (data) cacheSet(blockCache, blockNumber, data);
    return data;
  }, priority);
}

export function getTransaction(txHash, priority = false) {
  const cached = cacheGet(txCache, txHash, TTL_TX);
  if (cached) return Promise.resolve(cached);

  return enqueue(async () => {
    const data = await callWithPool(TX_POOL, "getTransaction", [txHash], TIMEOUT.transaction);
    if (data) cacheSet(txCache, txHash, data);
    return data;
  }, priority);
}

export function getTransactionReceipt(txHash, priority = false) {
  const cached = cacheGet(receiptCache, txHash, TTL_TX);
  if (cached) return Promise.resolve(cached);

  return enqueue(async () => {
    const data = await callWithPool(TX_POOL, "getTransactionReceipt", [txHash], TIMEOUT.receipt);
    if (data) cacheSet(receiptCache, txHash, data);
    return data;
  }, priority);
}

/**
 * Fetch multiple contract read-calls in parallel via best available provider.
 *
 * @param {Record<string, (provider: import("ethers").JsonRpcProvider) => Promise<any>>} fields
 * @returns {Promise<Record<string, any | null>>}
 *
 * @example
 * const data = await getContractFields({
 *   totalSupply: (p) => new Contract(ADDR, ABI, p).totalSupply(),
 *   decimals:    (p) => new Contract(ADDR, ABI, p).decimals(),
 * });
 */
export async function getContractFields(fields) {
  const entries = Object.entries(fields);
  const results = await Promise.allSettled(
    entries.map(([, fn]) =>
      enqueue(async () => {
        const entry = pickBest(TX_POOL);
        if (!entry) throw new Error("all_providers_unavailable");
        const t0 = Date.now();
        try {
          const v = await withTimeout(fn(entry.provider), TIMEOUT.contract, "contract timeout");
          recordSuccess(entry, Date.now() - t0);
          return v;
        } catch (err) {
          recordFailure(entry, false);
          throw err;
        }
      })
    )
  );
  return Object.fromEntries(
    entries.map(([key], i) => [
      key,
      results[i].status === "fulfilled" ? results[i].value : null,
    ])
  );
}

// ================= STATS =================

export function getQueueStats() {
  const poolStats = pool =>
    pool.map(e => ({
      url:      e.url,
      score:    Math.round(e.score),
      failures: e.failures,
      open:     e.openSince !== null,
    }));

  return {
    queueLength: queue.length,
    inFlight,
    reqCount,
    windowStart,
    providers: {
      logs: poolStats(LOGS_POOL),
      tx:   poolStats(TX_POOL),
    },
    cacheSize: {
      tx:      txCache.size,
      receipt: receiptCache.size,
      block:   blockCache.size,
      logs:    logsCache.size,
    },
  };
}

// ================= PERIODIC CLEANUP =================

setInterval(() => {
  const now = Date.now();
  for (const [k, v] of logsCache) {
    if (now - v.ts > TTL_LOGS) logsCache.delete(k);
  }
}, TTL_LOGS);