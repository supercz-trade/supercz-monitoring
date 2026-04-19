// ===============================================================
// candleBuilder.js (FULL TRACE + AGG DEBUG)
// ===============================================================

import { db } from "../infra/database.js";
import { publish } from "../infra/wsbroker.js";
import {
  processCandle1s,
  flushExpiredAggCandles,
} from "./candleAggregator.js";

// [ADDED]
import { pushAggLog } from "../infra/aggDebugBuffer.js";

const openCandles = new Map();
const lastClose = new Map();
const flushingSet = new Set();

let _flushInterval = null;

const FINALIZATION_DELAY_MS = 400;
const FIRST_CANDLE_PRICE = 0.0000035;

// [ADDED]
const lastFlushedTime = new Map();
const activeCandleTime = new Map();

// ===============================================================
// UPDATE CANDLE
// ===============================================================

export function updateCandle({ tokenAddress, priceUSDT, inUSDTPayable, time }) {

  if (!priceUSDT || priceUSDT <= 0) return;

  let ts;

  if (time instanceof Date) {
    ts = Math.floor(time.getTime() / 1000);
  } else if (typeof time === "number") {
    ts = time > 1e12 ? Math.floor(time / 1000) : Math.floor(time);
  } else if (typeof time === "string") {
    const ms = new Date(time).getTime();
    if (isNaN(ms)) return;
    ts = Math.floor(ms / 1000);
  } else {
    return;
  }

  const candleTime = ts;
  const volume = inUSDTPayable || 0;

  const existing = openCandles.get(tokenAddress);
  const lastFlushed = lastFlushedTime.get(tokenAddress);
  const activeTime = activeCandleTime.get(tokenAddress);

  // ===============================================================
  // [DEBUG] INPUT
  // ===============================================================
  pushAggLog({
    stage: "CANDLE_INPUT",
    tokenAddress,
    candleTime,
    price: priceUSDT,
    lastFlushed,
    activeTime
  });

  // ===============================================================
  // SAME SECOND MERGE
  // ===============================================================
  if (activeTime === candleTime) {

    const candle = openCandles.get(tokenAddress);
    if (!candle) return;

    pushAggLog({
      stage: "CANDLE_MERGE",
      tokenAddress,
      candleTime,
      price: priceUSDT
    });

    candle.high = Math.max(candle.high, priceUSDT);
    candle.low  = Math.min(candle.low,  priceUSDT);
    candle.close = priceUSDT;
    candle.volume += volume;
    candle.txCount++;

    _publishCandle(tokenAddress, candle, false);
    return;
  }

  // ===============================================================
  // LATE TX PROTECTION
  // ===============================================================
  if (lastFlushed && candleTime <= lastFlushed) {

    pushAggLog({
      stage: "CANDLE_SKIP_LATE",
      tokenAddress,
      candleTime,
      lastFlushed
    });

    return;
  }

  // ===============================================================
  // NEW CANDLE
  // ===============================================================
  let prevClose;

  if (existing) {

    lastClose.set(tokenAddress, existing.close);
    openCandles.delete(tokenAddress);

    prevClose = existing.close;

    pushAggLog({
      stage: "CANDLE_PREV_CLOSE",
      tokenAddress,
      prevClose
    });

  } else {
    prevClose = lastClose.get(tokenAddress);
  }

  const openPrice = prevClose ?? priceUSDT;

  pushAggLog({
    stage: "CANDLE_NEW",
    tokenAddress,
    candleTime,
    prevClose,
    openPrice
  });

  const newCandle = {
    time: candleTime,
    open: openPrice,
    high: priceUSDT,
    low: priceUSDT,
    close: priceUSDT,
    volume,
    txCount: 1,
  };

  openCandles.set(tokenAddress, newCandle);
  activeCandleTime.set(tokenAddress, candleTime);

  pushAggLog({
    stage: "CANDLE_UPDATE",
    tokenAddress,
    candle: newCandle
  });

  _publishCandle(tokenAddress, newCandle, false);
}

// ===============================================================
// FLUSH CANDLE
// ===============================================================

async function _flushCandle(tokenAddress, candle) {

  const key = `${tokenAddress}:${candle.time}`;
  if (flushingSet.has(key)) return;

  flushingSet.add(key);

  try {

    // ===============================================================
    // [DEBUG] FLUSH 1s
    // ===============================================================
    pushAggLog({
      stage: "CANDLE_FLUSH_1S",
      tokenAddress,
      candle
    });

    await db.query(`
      INSERT INTO token_candles (
        token_address, start_time, timeframe,
        open, high, low, close,
        volume_usdt, tx_count
      )
      VALUES ($1, to_timestamp($2), '1s', $3, $4, $5, $6, $7, $8)
      ON CONFLICT ON CONSTRAINT token_candle_unique
      DO UPDATE SET
        high        = GREATEST(token_candles.high, $4),
        low         = LEAST(token_candles.low,     $5),
        close       = CASE
                        WHEN token_candles.tx_count < $8
                        THEN $6
                        ELSE token_candles.close
                      END,
        volume_usdt = token_candles.volume_usdt + $7,
        tx_count    = token_candles.tx_count + $8
    `, [
      tokenAddress,
      candle.time,
      candle.open,
      candle.high,
      candle.low,
      candle.close,
      candle.volume,
      candle.txCount,
    ]);

    // ===============================================================
    // TRACK LAST FLUSH
    // ===============================================================
    lastFlushedTime.set(tokenAddress, candle.time);

    _publishCandle(tokenAddress, candle, true);
    processCandle1s(tokenAddress, candle);

  } catch (err) {

    pushAggLog({
      stage: "CANDLE_FLUSH_ERROR",
      tokenAddress,
      error: err.message
    });

  } finally {
    flushingSet.delete(key);
  }

}

// ===============================================================
// PUBLISH
// ===============================================================

function _publishCandle(tokenAddress, candle, closed) {
  publish(`candle:${tokenAddress}`, {
    tokenAddress,
    timeframe: "1s",
    time: candle.time,
    open: candle.open,
    high: candle.high,
    low: candle.low,
    close: candle.close,
    volume: candle.volume,
    txCount: candle.txCount,
    closed,
  });
}

// ===============================================================
// FLUSH LOOP
// ===============================================================

export function startCandleFlush() {

  if (_flushInterval) return;

  _flushInterval = setInterval(() => {

    const nowMs = Date.now();

    for (const [tokenAddress, candle] of openCandles.entries()) {

      const candleEndMs = (candle.time * 1000) + 1000;

      if (nowMs >= candleEndMs + FINALIZATION_DELAY_MS) {

        const key = `${tokenAddress}:${candle.time}`;
        if (flushingSet.has(key)) continue;

        openCandles.delete(tokenAddress);
        lastClose.set(tokenAddress, candle.close);

        _flushCandle(tokenAddress, candle);
      }
    }

    flushExpiredAggCandles();

  }, 200);

}

// ===============================================================
// WARMUP
// ===============================================================

export async function warmupLastClose() {

  try {

    const { rows } = await db.query(`
      SELECT DISTINCT ON (token_address)
        token_address, close
      FROM token_candles
      WHERE timeframe = '1s'
      ORDER BY token_address, start_time DESC
    `);

    for (const row of rows) {
      lastClose.set(row.token_address, Number(row.close));
    }

    console.log(`[CANDLE] Loaded last close for ${rows.length} tokens`);

  } catch (err) {
    console.error("[CANDLE] warmupLastClose error:", err.message);
  }

}

// ===============================================================
// HELPERS
// ===============================================================

export function getOpenCandle(tokenAddress) {
  return openCandles.get(tokenAddress) || null;
}

export function getLastClose(tokenAddress) {
  return lastClose.get(tokenAddress) || null;
}