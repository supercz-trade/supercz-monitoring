// ===============================================================
// candle_service.js
// FIX race condition:
//   - updateCandleFromTrade dijalankan sequential per token
//     menggunakan per-token promise queue (mutex sederhana)
//   - Mencegah 2 trade masuk bersamaan overwrite satu sama lain
//   - startTime selalu di detik :00 (floor ke bucket)
//   - open selalu close candle sebelumnya, kecuali candle pertama
// JUGA FIX:
//   1. Cache miss → load last close dari DB agar open tidak loncat
//   2. Timeframe lengkap: 1m 5m 15m 30m 1h 4h 1d
//   3. WS publish setiap trade (tidak hanya candle baru)
//   4. open SELALU dari close candle sebelumnya, kecuali candle pertama ever
// ===============================================================

import { upsertCandle, getLastClose } from "../repository/candle.repository.js";
import { publish } from "../infra/wsbroker.js";

// key → { startTime, open, high, low, close, volumeToken, volumeBase, volumeUsdt }
const candleCache = new Map();

// Per-token promise queue — mencegah race condition antar trade
const tradeQueue  = new Map();

export const TIMEFRAMES = {
  "1m" : 60,
  "5m" : 300,
  "15m": 900,
  "30m": 1800,
  "1h" : 3600,
  "4h" : 14400,
  "1d" : 86400
};

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

function getBucket(time, seconds) {
  const t      = Math.floor(new Date(time).getTime() / 1000);
  const bucket = Math.floor(t / seconds) * seconds;
  return new Date(bucket * 1000);
}

function getKey(token, timeframe) {
  return `${token.toLowerCase()}:${timeframe}`;
}

// ─────────────────────────────────────────────
// Core: update satu timeframe
// ─────────────────────────────────────────────

async function updateTimeframe(txData, timeframe, seconds) {
  const startTime = getBucket(txData.time, seconds);
  const key       = getKey(txData.tokenAddress, timeframe);

  let candle = candleCache.get(key);

  // ── Bucket berbeda → candle baru ─────────────────────────────────
  if (!candle || candle.startTime.getTime() !== startTime.getTime()) {

    // Ambil close terakhir dari cache dulu, fallback ke DB
    // Ini yang mencegah candle "loncat" setelah restart / candle baru
    let openPrice;

    if (candle) {
      // cache ada tapi bucket beda → pakai close candle sebelumnya
      openPrice = candle.close;
    } else {
      // cache kosong (restart) → cari close candle terakhir di DB
      const lastClose = await getLastClose(
        txData.tokenAddress,
        timeframe,
        startTime          // cari candle SEBELUM startTime ini
      );

      // Kalau ada → sambung. Kalau belum pernah ada → pakai harga tx ini
      openPrice = lastClose !== null ? lastClose : txData.priceUSDT;
    }

    candle = {
      startTime,
      open       : openPrice,
      high       : txData.priceUSDT,
      low        : txData.priceUSDT,
      close      : txData.priceUSDT,
      volumeToken: txData.amountReceive,
      volumeBase : txData.amountBasePayable,
      volumeUsdt : Math.abs(txData.volumeUSDT)
    };

  } else {
    // ── Candle bucket sama → update ────────────────────────────────
    candle.high        = Math.max(candle.high, txData.priceUSDT);
    candle.low         = Math.min(candle.low,  txData.priceUSDT);
    candle.close       = txData.priceUSDT;
    candle.volumeToken += txData.amountReceive;
    candle.volumeBase  += txData.amountBasePayable;
    candle.volumeUsdt  += Math.abs(txData.volumeUSDT);
  }

  candleCache.set(key, candle);

  // ── Persist ke DB ─────────────────────────────────────────────────
  await upsertCandle({
    tokenAddress: txData.tokenAddress,
    timeframe,
    startTime   : candle.startTime,
    open        : candle.open,
    high        : candle.high,
    low         : candle.low,
    close       : candle.close,
    volumeToken : candle.volumeToken,
    volumeBase  : candle.volumeBase,
    volumeUsdt  : candle.volumeUsdt
  });

  // ── WS publish setiap trade ───────────────────────────────────────
  publish(`candle:${txData.tokenAddress.toLowerCase()}`, {
    tokenAddress: txData.tokenAddress.toLowerCase(),
    timeframe,
    startTime   : candle.startTime,
    open        : candle.open,
    high        : candle.high,
    low         : candle.low,
    close       : candle.close,
    volume      : candle.volumeUsdt
  });
}

// ─────────────────────────────────────────────
// _processTradeSequential — semua timeframe satu per satu
// ─────────────────────────────────────────────

async function _processTradeSequential(txData) {
  for (const [tf, seconds] of Object.entries(TIMEFRAMES)) {
    await updateTimeframe(txData, tf, seconds);
  }
}

// ─────────────────────────────────────────────
// Entry point — dipanggil dari handler setiap trade
//
// Per-token queue: trade token A tidak block token B,
// tapi 2 trade token A yang masuk bersamaan diproses urut
// sehingga cache tidak saling overwrite.
// ─────────────────────────────────────────────

export function updateCandleFromTrade(txData) {
  const token = txData.tokenAddress.toLowerCase();

  const prev = tradeQueue.get(token) ?? Promise.resolve();

  const next = prev
    .then(() => _processTradeSequential(txData))
    .catch(err => console.error("[CANDLE] trade error:", token, err.message));

  tradeQueue.set(token, next);

  // Cleanup agar tidak leak memory
  next.finally(() => {
    if (tradeQueue.get(token) === next) tradeQueue.delete(token);
  });

  return next;
}

// ─────────────────────────────────────────────
// Warm-up cache saat startup (opsional tapi direkomendasikan)
// Panggil di server.js sebelum handler mulai
// ─────────────────────────────────────────────

export async function warmCandleCache(tokenAddress) {
  for (const [tf] of Object.entries(TIMEFRAMES)) {
    const key = getKey(tokenAddress, tf);
    if (candleCache.has(key)) continue;

    // Cukup load candle terakhir per token per timeframe
    const lastClose = await getLastClose(tokenAddress, tf, null);
    if (lastClose === null) continue;

    // Simpan sebagai sentinel agar updateTimeframe tahu close sebelumnya
    // startTime = epoch 0 → pasti berbeda dengan bucket real → selalu bikin candle baru
    // tapi openPrice sudah tersedia dari lastClose
    candleCache.set(key, {
      startTime  : new Date(0),
      open       : lastClose,
      high       : lastClose,
      low        : lastClose,
      close      : lastClose,
      volumeToken: 0,
      volumeBase : 0,
      volumeUsdt : 0
    });
  }
}