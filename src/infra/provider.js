import { WebSocketProvider, JsonRpcProvider, Network } from "ethers";
import dotenv from "dotenv";

dotenv.config();

const BSC_NETWORK = new Network("bnb", 56);

// ================= CONFIG =================

const RECONNECT_MIN = 3000;
const RECONNECT_MAX = 30000;

let reconnectDelay  = RECONNECT_MIN;
let _wssProvider    = null;
let lastBlockTime   = Date.now();
let _blockListeners = [];
let _reconnectTimer = null;
let _destroyed      = false;

// ================= RPC POOL =================

const RPC_BASE_URL = process.env.BSC_RPC_URL;

const API_KEYS = process.env.API_KEY
  ? process.env.API_KEY.split(",").map(k => k.trim()).filter(Boolean)
  : [];

const RPC_URLS = API_KEYS.length > 0
  ? API_KEYS.map(key => `${RPC_BASE_URL}${key}`)
  : [RPC_BASE_URL];

const _rpcLogsCache = new Map();
const _rpcTxCache   = new Map();

function getOrCreate(url, cache) {
  if (!cache.has(url)) {
    cache.set(url, new JsonRpcProvider(url, BSC_NETWORK, { staticNetwork: BSC_NETWORK }));
  }
  return cache.get(url);
}

function randomUrl() {
  return RPC_URLS[Math.floor(Math.random() * RPC_URLS.length)];
}

// ================= RPC PROXY =================

function makeRandomProxy(cache) {
  return new Proxy({}, {
    get(_, prop) {
      const provider = getOrCreate(randomUrl(), cache);
      const val = provider[prop];
      return typeof val === "function" ? val.bind(provider) : val;
    }
  });
}

export const rpcLogsProvider = makeRandomProxy(_rpcLogsCache);
export const rpcTxProvider   = makeRandomProxy(_rpcTxCache);

// ================= WSS POOL =================

const WSS_BASE_URL = process.env.BSC_WSS_BLOCK; // wss://bsc-mainnet.core.chainstack.com/

const WSS_API_KEYS = process.env.WSS_API_KEY
  ? process.env.WSS_API_KEY.split(",").map(k => k.trim()).filter(Boolean)
  : [];

const WSS_URLS = WSS_API_KEYS.length > 0
  ? WSS_API_KEYS.map(key => `${WSS_BASE_URL}${key}`)
  : [WSS_BASE_URL];

function randomWssUrl() {
  return WSS_URLS[Math.floor(Math.random() * WSS_URLS.length)];
}

console.log(`[PROVIDER] WSS pool ready — ${WSS_URLS.length} endpoint(s)`);

// ================= WSS GETTER =================

export function getWssProvider() {
  return _wssProvider;
}

export function onBlock(listener) {
  _blockListeners.push(listener);
  if (_wssProvider) _wssProvider.on("block", listener);
}

// ================= CREATE WSS =================

function createProvider() {

  const url = randomWssUrl();
  console.log("[WSS] connecting...", url.slice(0, 50) + "...");

  const provider = new WebSocketProvider(
    url,
    BSC_NETWORK,
    { staticNetwork: BSC_NETWORK }
  );

  provider.websocket?.addEventListener("open", () => {
    console.log("[WSS] connected");
    reconnectDelay = RECONNECT_MIN;
    lastBlockTime  = Date.now();
  });

  provider.websocket?.addEventListener("close", () => {
    console.warn("[WSS] disconnected");
    if (!_destroyed) scheduleReconnect();
  });

  provider.websocket?.addEventListener("error", (err) => {
    console.error("[WSS ERROR]", err?.message);
  });

  provider.on("block", () => {
    lastBlockTime = Date.now();
  });

  for (const listener of _blockListeners) {
    provider.on("block", listener);
  }

  return provider;

}

// ================= RECONNECT =================

function scheduleReconnect() {

  if (_reconnectTimer) return;

  // reset delay — langsung reconnect tanpa nunggu lama
  reconnectDelay = RECONNECT_MIN;
  console.log(`[WSS] reconnecting in ${reconnectDelay}ms`);

  _reconnectTimer = setTimeout(() => {
    _reconnectTimer = null;

    try {
      _wssProvider?.removeAllListeners();
      _wssProvider?.websocket?.close();
    } catch {}

    _wssProvider = createProvider();
  }, reconnectDelay);

}

// ================= START =================

_wssProvider = createProvider();

// ================= HEARTBEAT =================

setInterval(async () => {

  try {
    if (!_wssProvider) return;
    await _wssProvider.getBlockNumber();
  } catch (err) {
    console.warn("[WSS] heartbeat failed:", err?.message);
    scheduleReconnect();
  }

}, 20000);

// ================= BLOCK WATCHDOG =================

setInterval(() => {

  const diff = Date.now() - lastBlockTime;

  if (diff > 60000) {
    console.warn("[WATCHDOG] block stuck, forcing reconnect");
    scheduleReconnect();
  }

}, 30000);

// ================= INFO =================

console.log(`[PROVIDER] WSS block listener ready`);
console.log(`[PROVIDER] RPC pool ready — ${RPC_URLS.length} endpoint(s)`);