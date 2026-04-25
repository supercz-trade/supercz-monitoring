/**
 * provider.js
 *
 * Responsibilities:
 *  - WebSocket provider lifecycle (connect, reconnect, heartbeat, watchdog)
 *  - Export RPC URL pools for rpcQueue.js to manage
 *
 * NOT responsible for:
 *  - HTTP RPC call routing or retries (handled by rpcQueue.js)
 *  - Provider scoring or circuit breaking (handled by rpcQueue.js)
 */

import { WebSocketProvider, Network } from "ethers";
import dotenv from "dotenv";

dotenv.config();

const BSC_NETWORK = new Network("bnb", 56);

// ================= CONFIG =================

const RECONNECT_MIN     = 3_000;
const RECONNECT_MAX     = 30_000;
const HEARTBEAT_MS      = 8_000;   // BSC ~3s blocks; detect stuck faster
const BLOCK_TIMEOUT_MS  = 30_000;  // force reconnect if no block in 30s

// ================= RPC URL POOLS =================
// Exported to rpcQueue.js which handles scoring, circuit breaking, failover.
// Split LOGS and TX pools so they don't share rate limit quota.

const RPC_BASE_URL = process.env.BSC_RPC_URL ?? "";

const LOGS_KEYS = process.env.RPC_LOGS_API_KEYS
  ? process.env.RPC_LOGS_API_KEYS.split(",").map(k => k.trim()).filter(Boolean)
  : process.env.API_KEY
    ? process.env.API_KEY.split(",").map(k => k.trim()).filter(Boolean)
    : [];

const TX_KEYS = process.env.RPC_TX_API_KEYS
  ? process.env.RPC_TX_API_KEYS.split(",").map(k => k.trim()).filter(Boolean)
  : LOGS_KEYS; // fallback: share same pool

export const LOGS_RPC_URLS = LOGS_KEYS.length > 0
  ? LOGS_KEYS.map(k => `${RPC_BASE_URL}${k}`)
  : [RPC_BASE_URL];

export const TX_RPC_URLS = TX_KEYS.length > 0
  ? TX_KEYS.map(k => `${RPC_BASE_URL}${k}`)
  : [RPC_BASE_URL];

console.log(`[PROVIDER] RPC logs pool — ${LOGS_RPC_URLS.length} endpoint(s)`);
console.log(`[PROVIDER] RPC tx pool   — ${TX_RPC_URLS.length} endpoint(s)`);

// ================= WSS POOL =================

const WSS_BASE_URL = process.env.BSC_WSS_BLOCK ?? "";

const WSS_KEYS = process.env.WSS_API_KEY
  ? process.env.WSS_API_KEY.split(",").map(k => k.trim()).filter(Boolean)
  : [];

const WSS_URLS = WSS_KEYS.length > 0
  ? WSS_KEYS.map(k => `${WSS_BASE_URL}${k}`)
  : [WSS_BASE_URL];

console.log(`[PROVIDER] WSS pool — ${WSS_URLS.length} endpoint(s)`);

// Round-robin WSS selection (spread reconnects across endpoints)
let _wssRoundRobin = 0;
function nextWssUrl() {
  const url = WSS_URLS[_wssRoundRobin % WSS_URLS.length];
  _wssRoundRobin++;
  return url;
}

// ================= WSS STATE =================

let _wssProvider    = null;
let _blockListeners = [];
let _reconnectTimer = null;
let _destroyed      = false;
let _reconnectDelay = RECONNECT_MIN;
let _lastBlockTime  = Date.now();

// ================= PUBLIC WSS API =================

export function getWssProvider() {
  return _wssProvider;
}

/** Register a block listener — survives reconnects automatically. */
export function onBlock(listener) {
  if (!_blockListeners.includes(listener)) {
    _blockListeners.push(listener);
  }
  if (_wssProvider) _wssProvider.on("block", listener);
}

/** Unregister a block listener. */
export function offBlock(listener) {
  _blockListeners = _blockListeners.filter(l => l !== listener);
  _wssProvider?.off("block", listener);
}

/** Gracefully shut down WSS — stops reconnect loop. */
export function destroyWss() {
  _destroyed = true;
  clearTimeout(_reconnectTimer);
  _wssProvider?.removeAllListeners();
  _wssProvider?.websocket?.close();
  _wssProvider = null;
}

// ================= WSS LIFECYCLE =================

function createProvider() {
  const url = nextWssUrl();
  console.log(`[WSS] connecting → ${url.slice(0, 60)}…`);

  const provider = new WebSocketProvider(
    url,
    BSC_NETWORK,
    { staticNetwork: BSC_NETWORK }
  );

  provider.websocket?.addEventListener("open", () => {
    console.log("[WSS] connected");
    _reconnectDelay = RECONNECT_MIN;  // reset backoff on successful connect
    _lastBlockTime  = Date.now();
  });

  provider.websocket?.addEventListener("close", () => {
    console.warn("[WSS] disconnected");
    if (!_destroyed) scheduleReconnect();
  });

  provider.websocket?.addEventListener("error", err => {
    console.error("[WSS] error:", err?.message ?? err);
  });

  // Track last block time for watchdog
  provider.on("block", () => { _lastBlockTime = Date.now(); });

  // Re-attach all registered listeners
  for (const listener of _blockListeners) {
    provider.on("block", listener);
  }

  return provider;
}

function scheduleReconnect() {
  if (_reconnectTimer || _destroyed) return;

  console.log(`[WSS] reconnecting in ${_reconnectDelay}ms`);

  _reconnectTimer = setTimeout(() => {
    _reconnectTimer = null;

    try {
      _wssProvider?.removeAllListeners();
      _wssProvider?.websocket?.close();
    } catch { /* ignore close errors */ }

    _wssProvider = createProvider();

    // Exponential backoff — cap at RECONNECT_MAX
    _reconnectDelay = Math.min(_reconnectDelay * 2, RECONNECT_MAX);
  }, _reconnectDelay);
}

// ================= INIT =================

_wssProvider = createProvider();

// ================= HEARTBEAT =================
// Actively probe the connection; if it fails, trigger reconnect.

setInterval(async () => {
  if (!_wssProvider || _destroyed) return;
  try {
    await _wssProvider.getBlockNumber();
  } catch (err) {
    console.warn("[WSS] heartbeat failed:", err?.message);
    scheduleReconnect();
  }
}, HEARTBEAT_MS);

// ================= BLOCK WATCHDOG =================
// Detect cases where the socket stays open but stops receiving blocks.

setInterval(() => {
  if (_destroyed) return;
  const stale = Date.now() - _lastBlockTime;
  if (stale > BLOCK_TIMEOUT_MS) {
    console.warn(`[WATCHDOG] no block for ${stale}ms — forcing reconnect`);
    scheduleReconnect();
  }
}, BLOCK_TIMEOUT_MS / 2);