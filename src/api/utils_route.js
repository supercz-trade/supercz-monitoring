// ===============================================================
// utils_route.js
// GET /utils/gas-price → gas price BSC saat ini (cache 15s)
//
// Kenapa di backend bukan frontend:
//   - Hemat RPC call — semua user share 1 cache
//   - Frontend tinggal fetch endpoint ini, tidak perlu ethers.js
// ===============================================================

import { getFeeData } from "../infra/rpcQueue.js";

// ── Cache 15 detik ─────────────────────────────────────────────
let _cache = null;
let _cacheAt = 0;
const CACHE_TTL_MS = 15_000;

async function fetchGasPrice() {
  const now = Date.now();
  if (_cache && now - _cacheAt < CACHE_TTL_MS) return _cache;

  try {
    const feeData = await getFeeData();

    // gasPrice dalam wei → convert ke Gwei
    const gasPriceWei  = feeData.gasPrice ?? feeData.maxFeePerGas ?? 0n;
    const gasPriceGwei = Number(gasPriceWei) / 1e9;

    // Bulatkan ke 1 desimal
    const rounded = Math.round(gasPriceGwei * 10) / 10;

    _cache = {
      gwei:      rounded,
      wei:       gasPriceWei.toString(),
      fetchedAt: now,
    };
    _cacheAt = now;

    return _cache;
  } catch (err) {
    console.error("[GAS PRICE] fetch error:", err.message);

    // Kalau gagal dan ada cache lama, return cache lama daripada error
    if (_cache) return _cache;

    // Fallback default BSC gas price
    return { gwei: 3, wei: "3000000000", fetchedAt: now, fallback: true };
  }
}

// ===============================================================
// ROUTE HANDLER
// GET /utils/gas-price
// Response: { gwei: 3.1, wei: "3100000000", fetchedAt: 1234567890 }
// ===============================================================

export async function getGasPrice(req, reply) {
  try {
    const data = await fetchGasPrice();
    return reply.send(data);
  } catch (err) {
    console.error("[GAS PRICE API]", err.message);
    return reply.code(500).send({ error: "failed_to_fetch_gas_price" });
  }
}