// ===============================================================
// candle_repository.js
// FIX:
//   1. ON CONFLICT → open TIDAK di-update (dijaga nilai pertama)
//   2. Tambah getLastClose() untuk sambung open antar candle
// ===============================================================

import { db } from "../infra/database.js";

// ─────────────────────────────────────────────
// Upsert satu candle
// open → hanya diset saat INSERT, tidak pernah di-update
// ─────────────────────────────────────────────

export async function upsertCandle(data) {
  const query = `
    INSERT INTO token_candles (
      token_address,
      timeframe,
      start_time,
      open,
      high,
      low,
      close,
      volume_token,
      volume_base,
      volume_usdt
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)

    ON CONFLICT (token_address, timeframe, start_time)
    DO UPDATE SET
      -- open SENGAJA tidak di-update: nilai pertama harus dipertahankan
      high         = GREATEST(token_candles.high, EXCLUDED.high),
      low          = LEAST(token_candles.low,  EXCLUDED.low),
      close        = EXCLUDED.close,
      volume_token = token_candles.volume_token + EXCLUDED.volume_token,
      volume_base  = token_candles.volume_base  + EXCLUDED.volume_base,
      volume_usdt  = token_candles.volume_usdt  + EXCLUDED.volume_usdt
  `;

  await db.query(query, [
    data.tokenAddress,
    data.timeframe,
    data.startTime,
    data.open,
    data.high,
    data.low,
    data.close,
    data.volumeToken,
    data.volumeBase,
    data.volumeUsdt
  ]);
}

// ─────────────────────────────────────────────
// Ambil close dari candle terakhir SEBELUM startTime
// Dipakai oleh candle_service saat cache miss (restart)
//
// @param tokenAddress  string
// @param timeframe     string  e.g. "1m"
// @param beforeTime    Date|null  — null → ambil candle terbaru apapun
// ─────────────────────────────────────────────

export async function getLastClose(tokenAddress, timeframe, beforeTime) {
  let query, params;

  if (beforeTime) {
    query = `
      SELECT close
      FROM token_candles
      WHERE LOWER(token_address) = LOWER($1)
        AND timeframe             = $2
        AND start_time            < $3
      ORDER BY start_time DESC
      LIMIT 1
    `;
    params = [tokenAddress, timeframe, beforeTime];
  } else {
    query = `
      SELECT close
      FROM token_candles
      WHERE LOWER(token_address) = LOWER($1)
        AND timeframe             = $2
      ORDER BY start_time DESC
      LIMIT 1
    `;
    params = [tokenAddress, timeframe];
  }

  const { rows } = await db.query(query, params);
  return rows.length ? Number(rows[0].close) : null;
}