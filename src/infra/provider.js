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
let _blockListeners = [];   // simpan semua listener block yang didaftarkan
let _reconnectTimer = null;
let _destroyed      = false;

// ================= GETTER =================
// FIX: export fungsi getter, bukan nilai langsung.
// Kalau export nilai: blockDispatcher.js pegang referensi provider LAMA
// selamanya — provider baru dari reconnect tidak pernah dipakai.

export function getWssProvider() {
  return _wssProvider;
}

// Agar blockDispatcher bisa daftar listener tanpa pegang ref provider
// langsung. Saat reconnect, listener otomatis dipasang ke provider baru.
export function onBlock(listener) {
  _blockListeners.push(listener);
  if (_wssProvider) _wssProvider.on("block", listener);
}

// ================= CREATE WSS =================

function createProvider() {

  console.log("[WSS] connecting...");

  const provider = new WebSocketProvider(
    process.env.BSC_WSS_BLOCK,
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
    // Pastikan tidak double-reconnect
    if (!_destroyed) scheduleReconnect();
  });

  provider.websocket?.addEventListener("error", (err) => {
    console.error("[WSS ERROR]", err?.message);
  });

  provider.on("block", () => {
    lastBlockTime = Date.now();
  });

  // FIX: pasang ulang semua listener block yang terdaftar ke provider baru
  for (const listener of _blockListeners) {
    provider.on("block", listener);
  }

  return provider;

}

// ================= RECONNECT =================

function scheduleReconnect() {

  // Hindari double-schedule
  if (_reconnectTimer) return;

  reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX);
  console.log(`[WSS] reconnecting in ${reconnectDelay}ms`);

  _reconnectTimer = setTimeout(() => {
    _reconnectTimer = null;

    // Destroy provider lama dengan bersih
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
// mencegah idle disconnect

setInterval(async () => {

  try {
    if (!_wssProvider) return;
    await _wssProvider.getBlockNumber();
  } catch (err) {
    console.warn("[WSS] heartbeat failed:", err?.message);
  }

}, 20000);

// ================= BLOCK WATCHDOG =================
// jika block berhenti > 60 detik → reconnect

setInterval(() => {

  const diff = Date.now() - lastBlockTime;

  if (diff > 60000) {
    console.warn("[WATCHDOG] block stuck, forcing reconnect");
    // Reset delay supaya reconnect cepat saat watchdog trigger
    reconnectDelay = RECONNECT_MIN;
    scheduleReconnect();
  }

}, 30000);

// ================= RPC PROVIDERS =================

export const rpcLogsProvider = new JsonRpcProvider(
  process.env.BSC_RPC_LOGS,
  BSC_NETWORK,
  { staticNetwork: BSC_NETWORK }
);

export const rpcTxProvider = new JsonRpcProvider(
  process.env.BSC_RPC_TX,
  BSC_NETWORK,
  { staticNetwork: BSC_NETWORK }
);

console.log("[PROVIDER] RPC LOGS connected");
console.log("[PROVIDER] RPC TX connected");