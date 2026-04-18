// ===============================================================
// candleAggregator.js (FINAL FIX + STRICT OPEN CONSISTENCY)
// ===============================================================

import { db } from "../infra/database.js";
import { publish } from "../infra/wsbroker.js";
import { logCandleAgg } from "../infra/logger.js";
import { pushAggLog } from "../infra/aggDebugBuffer.js";

// ===============================================================
// TIMEFRAMES
// ===============================================================

const TIMEFRAMES = [
  { label: "15s", seconds: 15 },
  { label: "30s", seconds: 30 },
  { label: "1m", seconds: 60 },
  { label: "5m", seconds: 300 },
  { label: "15m", seconds: 900 },
  { label: "30m", seconds: 1800 },
  { label: "1h", seconds: 3600 },
  { label: "4h", seconds: 14400 },
  { label: "1d", seconds: 86400 },
];

// ===============================================================
// STATE
// ===============================================================

const openAggCandles = new Map();
const aggFlushingSet = new Set();

// [ADDED] store last close per TF
const lastAggCloseMap = new Map();

// ===============================================================
// ENTRY
// ===============================================================

export function processCandle1s(tokenAddress, candle1s) {

  pushAggLog({
    stage: "AGG_PROCESS_1S",
    tokenAddress,
    time: candle1s.time,
    close: candle1s.close
  });

  for (const tf of TIMEFRAMES) {
    _updateAggCandle(tokenAddress, tf, candle1s);
  }
}

// ===============================================================
// CORE LOGIC
// ===============================================================

function _updateAggCandle(tokenAddress, tf, candle1s) {

  const timeSec = Math.floor(Number(candle1s.time));
  const windowTime = Math.floor(timeSec / tf.seconds) * tf.seconds;

  const key = `${tokenAddress}:${tf.label}`;
  const existing = openAggCandles.get(key);

  pushAggLog({
    stage: "AGG_WINDOW",
    tokenAddress,
    tf: tf.label,
    timeSec,
    windowTime,
    existingTime: existing?.time ?? null
  });

  // ===============================================================
  // OUT OF ORDER PROTECTION
  // ===============================================================
  if (existing && windowTime < existing.time) {

    pushAggLog({
      stage: "AGG_SKIP_OLD",
      tokenAddress,
      tf: tf.label,
      incoming: windowTime,
      existing: existing.time
    });

    return;
  }

  // ===============================================================
  // NEW WINDOW
  // ===============================================================
  if (!existing || existing.time !== windowTime) {

    if (existing) {

      pushAggLog({
        stage: "AGG_WINDOW_SHIFT",
        tokenAddress,
        tf: tf.label,
        from: existing.time,
        to: windowTime
      });

      _flushAggCandle(tokenAddress, tf.label, existing);
    }

    // ===============================================================
    // [CRITICAL FIX] OPEN = PREVIOUS CLOSE
    // ===============================================================
    const prevClose = lastAggCloseMap.get(key);
    const openPrice = prevClose ?? candle1s.open;

    const newCandle = {
      time: windowTime,
      open: openPrice, // ✅ FIXED
      high: candle1s.high,
      low: candle1s.low,
      close: candle1s.close,
      volume: Number(candle1s.volume) || 0,
      txCount: Number(candle1s.txCount) || 0,
    };

    // [ADDED] save last close
    lastAggCloseMap.set(key, newCandle.close);

    pushAggLog({
      stage: "AGG_NEW",
      tokenAddress,
      tf: tf.label,
      prevClose,
      open: newCandle.open,
      close: newCandle.close
    });

    openAggCandles.set(key, newCandle);

  } else {

    // ===============================================================
    // UPDATE EXISTING
    // ===============================================================

    pushAggLog({
      stage: "AGG_UPDATE_BEFORE",
      tokenAddress,
      tf: tf.label,
      candle: existing
    });

    existing.high = Math.max(existing.high, Number(candle1s.high) || 0);
    existing.low  = Math.min(existing.low,  Number(candle1s.low)  || 0);

    // [IMPORTANT] ONLY CLOSE UPDATED
    existing.close = candle1s.close;

    existing.volume  += Number(candle1s.volume) || 0;
    existing.txCount += Number(candle1s.txCount) || 0;

    // [ADDED] update last close
    lastAggCloseMap.set(key, existing.close);

    pushAggLog({
      stage: "AGG_UPDATE_AFTER",
      tokenAddress,
      tf: tf.label,
      open: existing.open,
      close: existing.close
    });
  }

  const candle = openAggCandles.get(key);

  // ===============================================================
  // SAFETY
  // ===============================================================
  if (candle.low > candle.high) {

    pushAggLog({
      stage: "AGG_ERROR_LOW_GT_HIGH",
      tokenAddress,
      tf: tf.label,
      candle
    });

    return;
  }

  _publishAggCandle(tokenAddress, tf.label, candle, false);
}

// ===============================================================
// FLUSH
// ===============================================================

async function _flushAggCandle(tokenAddress, label, candle) {

  const key = `${tokenAddress}:${label}:${candle.time}`;
  if (aggFlushingSet.has(key)) return;

  aggFlushingSet.add(key);

  try {

    pushAggLog({
      stage: "AGG_FLUSH",
      tokenAddress,
      tf: label,
      candle
    });

    await db.query(`
      INSERT INTO token_candles (
        token_address, start_time, timeframe,
        open, high, low, close,
        volume_usdt, tx_count
      )
      VALUES ($1, to_timestamp($2), $3, $4, $5, $6, $7, $8, $9)
      ON CONFLICT ON CONSTRAINT token_candle_unique
      DO UPDATE SET
        high        = GREATEST(token_candles.high, $5),
        low         = LEAST(token_candles.low,     $6),
        close       = $7,
        volume_usdt = token_candles.volume_usdt + $8,
        tx_count    = token_candles.tx_count    + $9
    `, [
      tokenAddress,
      candle.time,
      label,
      candle.open,
      candle.high,
      candle.low,
      candle.close,
      candle.volume,
      candle.txCount,
    ]);

    // [ADDED] sync last close after DB
    lastAggCloseMap.set(`${tokenAddress}:${label}`, candle.close);

    _publishAggCandle(tokenAddress, label, candle, true);

  } catch (err) {

    pushAggLog({
      stage: "AGG_FLUSH_ERROR",
      tokenAddress,
      tf: label,
      error: err.message
    });

  } finally {
    aggFlushingSet.delete(key);
  }
}

// ===============================================================
// PUBLISH
// ===============================================================

function _publishAggCandle(tokenAddress, label, candle, closed) {

  logCandleAgg(tokenAddress, label, candle, closed);

  publish(`candle:${tokenAddress}`, {
    tokenAddress,
    timeframe: label,
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
// AUTO FLUSH
// ===============================================================

export function flushExpiredAggCandles() {

  const now = Math.floor(Date.now() / 1000);

  for (const [key, candle] of openAggCandles.entries()) {

    const colonIdx = key.lastIndexOf(":");
    const tokenAddress = key.slice(0, colonIdx);
    const label = key.slice(colonIdx + 1);

    const tf = TIMEFRAMES.find(t => t.label === label);
    if (!tf) continue;

    if (now >= candle.time + tf.seconds) {

      openAggCandles.delete(key);
      _flushAggCandle(tokenAddress, label, candle);
    }
  }
}

// ===============================================================

export function getOpenAggCandle(tokenAddress, label) {
  return openAggCandles.get(`${tokenAddress}:${label}`) || null;
}

export { TIMEFRAMES };