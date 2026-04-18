// ================================================================
// candleRepair.js
// Safety net — perbaiki candle open yang tidak sambung ke close
// sebelumnya. Dijalankan tiap 1 menit via setInterval.
//
// Hanya scan candle yang dibuat dalam 5 menit terakhir supaya
// query tidak berat. Candle lama diasumsikan sudah benar.
// ================================================================

import { db } from "../infra/database.js";

const REPAIR_INTERVAL_MS = 60_000;       // tiap 1 menit
const LOOKBACK_SECONDS   = 300;          // scan 5 menit terakhir
const TOLERANCE          = 0.0001;       // toleransi 0.01% (floating point)
const TIMEFRAMES         = ["1s", "15s", "30s", "1m", "5m", "15m", "30m"];

let _interval  = null;
let _isRunning = false;

// ================================================================
// REPAIR — fix open = close candle sebelumnya
// ================================================================

async function repairCandleOpen() {

  // Guard: skip kalau run sebelumnya belum selesai
  if (_isRunning) {
    console.warn("[REPAIR] previous run still in progress, skipping");
    return;
  }

  _isRunning = true;

  try {

    const since = new Date(Date.now() - LOOKBACK_SECONDS * 1000);

    const { rowCount } = await db.query(`
      UPDATE token_candles tc
      SET open = sub.prev_close
      FROM (
        SELECT
          tc2.token_address,
          tc2.timeframe,
          tc2.start_time,
          prev_c.close AS prev_close
        FROM token_candles tc2
        JOIN LATERAL (
          SELECT close
          FROM token_candles
          WHERE token_address = tc2.token_address
            AND timeframe     = tc2.timeframe
            AND start_time    < tc2.start_time
          ORDER BY start_time DESC
          LIMIT 1
        ) prev_c ON true
        WHERE tc2.timeframe  = ANY($1)
          AND tc2.start_time >= $2
          AND prev_c.close   >  0
          AND ABS(tc2.open - prev_c.close) / prev_c.close > $3
      ) sub
      WHERE tc.token_address = sub.token_address
        AND tc.timeframe     = sub.timeframe
        AND tc.start_time    = sub.start_time
    `, [TIMEFRAMES, since, TOLERANCE]);

    if (rowCount > 0) {
      console.log(`[REPAIR] fixed ${rowCount} candle(s) open mismatch`);
    }

  } catch (err) {
    console.error("[REPAIR] error:", err.message);
  } finally {
    _isRunning = false;
  }

}

// ================================================================
// START / STOP
// ================================================================

export function startCandleRepair() {

  if (_interval) return;

  // FIX: delay 30s supaya warmupStatsCache & loadTokenFlap selesai dulu
  // Query LATERAL JOIN di repairCandleOpen cukup berat — kalau jalan
  // bersamaan saat startup bisa menguras pool dan trigger ETIMEDOUT
  setTimeout(() => {
    repairCandleOpen();
    _interval = setInterval(repairCandleOpen, REPAIR_INTERVAL_MS);
  }, 30_000);

  console.log("[REPAIR] Candle repair job scheduled (30s delay, then 1 min interval, 5 min lookback)");

}

export function stopCandleRepair() {
  if (_interval) {
    clearInterval(_interval);
    _interval = null;
  }
}