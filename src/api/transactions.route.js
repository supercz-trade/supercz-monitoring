// ===============================================================
// transactions.route.js
// FIX MARK PRESISI:
//   - Semua mark endpoint JOIN langsung ke token_candles
//   - time mark = start_time candle yang EXACT di DB
//   - Tidak ada lagi floor/epoch rounding yang bisa off-by-one
//   - Fallback: kalau candle belum ada di DB (terlalu baru),
//     snap ke candle terdekat via subquery
// ===============================================================

import { db } from "../infra/database.js";

// ===============================================================
// GET TRANSACTIONS BY TOKEN
// ===============================================================

export async function getTransactionsByToken(req, reply) {

  try {

    const { address }  = req.params;
    const limit        = Math.min(Number(req.query.limit) || 100, 500);
    const positionFilter = req.query.position?.toUpperCase();

    const params  = [address, limit];
    const posSQL  = positionFilter && ["BUY", "SELL"].includes(positionFilter)
      ? `AND position = $${params.push(positionFilter)}`
      : `AND position IN ('BUY', 'SELL')`;

    const { rows } = await db.query(`
      SELECT
        tx_hash,
        EXTRACT(EPOCH FROM time AT TIME ZONE 'UTC')::bigint AS time,
        position,
        price_usdt,
        amount_receive         AS amount_token,
        in_usdt_payable        AS amount_usdt,
        amount_base_payable    AS amount_base,
        base_payable           AS base_symbol,
        address_message_sender AS wallet,
        tag_address,
        is_dev
      FROM token_transactions
      WHERE LOWER(token_address) = LOWER($1)
        ${posSQL}
      ORDER BY time DESC
      LIMIT $2
    `, params);

    return rows.map(tx => ({
      txHash:      tx.tx_hash,
      time:        tx.time,
      position:    tx.position,
      wallet:      tx.wallet,
      tagAddress:  tx.tag_address || null,
      isDev:       tx.is_dev      || false,
      priceUsd:    Number(tx.price_usdt   || 0),
      amountToken: Number(tx.amount_token || 0),
      amountUsd:   Number(tx.amount_usdt  || 0),
      amountBase:  Number(tx.amount_base  || 0),
      baseSymbol:  tx.base_symbol || null,
    }));

  } catch (err) {
    console.error("[TX TOKEN API]", err.message);
    return reply.code(500).send({ error: "failed_to_fetch_transactions" });
  }

}

// ===============================================================
// GET TRANSACTIONS BY WALLET
// ===============================================================

export async function getTransactionsByWallet(req, reply) {

  try {

    const { address } = req.params;
    const limit       = Math.min(Number(req.query.limit) || 200, 1000);

    const [txResult, statsResult] = await Promise.all([

      db.query(`
        SELECT
          token_address,
          tx_hash,
          EXTRACT(EPOCH FROM time AT TIME ZONE 'UTC')::bigint AS time,
          position,
          price_usdt,
          amount_receive         AS amount_token,
          in_usdt_payable        AS amount_usdt,
          amount_base_payable    AS amount_base,
          base_payable           AS base_symbol,
          tag_address,
          is_dev
        FROM token_transactions
        WHERE LOWER(address_message_sender) = LOWER($1)
          AND position IN ('BUY', 'SELL')
        ORDER BY time DESC
        LIMIT $2
      `, [address, limit]),

      db.query(`
        SELECT
          COUNT(*)  FILTER (WHERE position = 'BUY')  AS buy_count,
          COUNT(*)  FILTER (WHERE position = 'SELL') AS sell_count,

          COALESCE(SUM(in_usdt_payable)     FILTER (WHERE position = 'BUY'),  0) AS buy_usd,
          COALESCE(SUM(in_usdt_payable)     FILTER (WHERE position = 'SELL'), 0) AS sell_usd,

          COALESCE(SUM(amount_base_payable) FILTER (WHERE position = 'BUY'),  0) AS buy_base,
          COALESCE(SUM(amount_base_payable) FILTER (WHERE position = 'SELL'), 0) AS sell_base,

          COALESCE(SUM(amount_receive)      FILTER (WHERE position = 'BUY'),  0) AS buy_tokens,
          COALESCE(SUM(amount_receive)      FILTER (WHERE position = 'SELL'), 0) AS sell_tokens

        FROM token_transactions
        WHERE LOWER(address_message_sender) = LOWER($1)
          AND position IN ('BUY', 'SELL')
      `, [address]),

    ]);

    const transactions = txResult.rows.map(tx => ({
      tokenAddress: tx.token_address,
      txHash:       tx.tx_hash,
      time:         tx.time,
      position:     tx.position,
      tagAddress:   tx.tag_address || null,
      isDev:        tx.is_dev      || false,
      priceUsd:     Number(tx.price_usdt   || 0),
      amountToken:  Number(tx.amount_token || 0),
      amountUsd:    Number(tx.amount_usdt  || 0),
      amountBase:   Number(tx.amount_base  || 0),
      baseSymbol:   tx.base_symbol || null,
    }));

    const s = statsResult.rows[0];

    const buyUsd     = Number(s.buy_usd     || 0);
    const sellUsd    = Number(s.sell_usd    || 0);
    const buyTokens  = Number(s.buy_tokens  || 0);
    const sellTokens = Number(s.sell_tokens || 0);

    const avgBuyPrice  = buyTokens > 0 ? buyUsd / buyTokens : 0;
    const realizedPnl  = sellTokens > 0
      ? sellUsd - (avgBuyPrice * sellTokens)
      : 0;
    const remainTokens = Math.max(buyTokens - sellTokens, 0);

    return {
      summary: {
        buyCount:     Number(s.buy_count  || 0),
        sellCount:    Number(s.sell_count || 0),
        buyUsd,
        sellUsd,
        buyBase:      Number(s.buy_base   || 0),
        sellBase:     Number(s.sell_base  || 0),
        buyTokens,
        sellTokens,
        remainTokens,
        avgBuyPrice,
        realizedPnl,
      },
      transactions,
    };

  } catch (err) {
    console.error("[TX WALLET API]", err.message);
    return reply.code(500).send({ error: "failed_to_fetch_transactions" });
  }

}

// ===============================================================
// TIMEFRAME SECONDS MAP
// ===============================================================

const TIMEFRAME_SECONDS = {
  "1s":  1,
  "15s": 15,
  "30s": 30,
  "1m":  60,
  "5m":  300,
  "15m": 900,
  "30m": 1_800,
  "1h":  3_600,
  "4h":  14_400,
  "1d":  86_400,
};

// ===============================================================
// HELPER — snap TX time → candle start_time yang EXACT dari DB
//
// Strategy:
//   1. Hitung window floor dari TX time (FLOOR(epoch/windowSec)*windowSec)
//   2. JOIN ke token_candles WHERE start_time = to_timestamp(floor)
//   3. Kalau candle belum ada (sangat baru), fallback ke candle
//      terdekat sebelum TX time (subquery MAX start_time <= tx.time)
//
// Ini LEBIH AKURAT dari pure floor karena:
//   - start_time di DB memakai timezone server PG yang bisa ada offset
//   - JOIN ke candle aktual memastikan time match 100% dengan bar LWC
// ===============================================================

function buildMarksQuery({ tokenAddress, timeframe, windowSec, extraWhere, extraParams, limit }) {
  // $1 = tokenAddress (lowercase), $2 = timeframe, $3 = windowSec, $4..N = extraParams

  // Offset params untuk extraParams (dimulai dari $4)
  const baseIdx   = 3;
  let   paramIdx  = baseIdx;

  const whereChunks = extraWhere.map(w => {
    // Ganti placeholder $X dengan offset yang benar
    return w.replace(/\$(\d+)/g, (_, n) => `$${Number(n) + baseIdx}`);
  });
  paramIdx += extraParams.length;

  const limitParam = `$${paramIdx + 1}`;

  return {
    sql: `
      WITH tx_data AS (
        SELECT
          tt.time                                              AS tx_time,
          EXTRACT(EPOCH FROM tt.time)::bigint                 AS tx_epoch,
          (FLOOR(EXTRACT(EPOCH FROM tt.time) / $3) * $3)     AS floor_epoch,
          tt.position,
          tt.price_usdt,
          tt.amount_receive   AS amount_token,
          tt.in_usdt_payable  AS amount_usd,
          tt.tx_hash,
          tt.address_message_sender AS wallet
        FROM token_transactions tt
        WHERE LOWER(tt.token_address) = LOWER($1)
          AND tt.position IN ('BUY', 'SELL')
          ${whereChunks.join("\n          ")}
        ORDER BY tt.time ASC
        LIMIT ${limitParam}
      ),
      -- Join ke candle EXACT (candle sudah di-flush ke DB)
      snapped AS (
        SELECT
          td.*,
          COALESCE(
            -- 1. Candle exact match (timeframe sudah flush)
            EXTRACT(EPOCH FROM tc_exact.start_time)::bigint,
            -- 2. Fallback: candle terdekat ≤ tx_time (candle 1s masih open atau baru flush)
            EXTRACT(EPOCH FROM tc_near.start_time)::bigint,
            -- 3. Last resort: pakai floor_epoch (candle belum ada sama sekali)
            td.floor_epoch::bigint
          ) AS candle_time
        FROM tx_data td
        -- Exact: start_time = to_timestamp(floor_epoch) dan timeframe match
        LEFT JOIN token_candles tc_exact
          ON LOWER(tc_exact.token_address) = LOWER($1)
         AND tc_exact.timeframe            = $2
         AND tc_exact.start_time           = to_timestamp(td.floor_epoch)
        -- Nearest: candle terdekat sebelum atau tepat di tx_time
        LEFT JOIN LATERAL (
          SELECT start_time
          FROM token_candles
          WHERE LOWER(token_address) = LOWER($1)
            AND timeframe            = $2
            AND start_time          <= td.tx_time
          ORDER BY start_time DESC
          LIMIT 1
        ) tc_near ON true
      )
      SELECT
        candle_time,
        position,
        price_usdt,
        amount_token,
        amount_usd,
        tx_hash,
        wallet
      FROM snapped
      ORDER BY candle_time ASC
    `,
    params: [tokenAddress, timeframe, windowSec, ...extraParams, limit],
  };
}

// ===============================================================
// GET WALLET CANDLE MARKS
// GET /tokens/:address/marks?wallet=0x...&timeframe=1m
// ===============================================================

export async function getWalletCandleMarks(req, reply) {

  try {

    const { address: tokenAddress } = req.params;
    const wallet    = req.query.wallet?.toLowerCase();
    const timeframe = req.query.timeframe || "1m";
    const limit     = Math.min(Number(req.query.limit) || 500, 2000);

    if (!wallet) {
      return reply.code(400).send({ error: "wallet query param required" });
    }

    const windowSec = TIMEFRAME_SECONDS[timeframe];
    if (!windowSec) {
      return reply.code(400).send({
        error: "invalid timeframe",
        valid: Object.keys(TIMEFRAME_SECONDS),
      });
    }

    const { sql, params } = buildMarksQuery({
      tokenAddress,
      timeframe,
      windowSec,
      extraWhere:  ["AND LOWER(tt.address_message_sender) = $1"],
      extraParams: [wallet],
      limit,
    });

    const { rows } = await db.query(sql, params);

    if (!rows.length) return [];

    // Group by (candle_time, position)
    const markMap = new Map();

    for (const tx of rows) {
      const key = `${tx.candle_time}:${tx.position}`;

      if (!markMap.has(key)) {
        markMap.set(key, {
          time:       Number(tx.candle_time),
          position:   tx.position,
          totalUsd:   0,
          totalToken: 0,
          totalPrice: 0,
          txCount:    0,
          txHashes:   [],
        });
      }

      const mark = markMap.get(key);
      mark.totalUsd   += Number(tx.amount_usd   || 0);
      mark.totalToken += Number(tx.amount_token || 0);
      mark.totalPrice += Number(tx.price_usdt   || 0);
      mark.txCount    += 1;
      mark.txHashes.push(tx.tx_hash);
    }

    return Array.from(markMap.values())
      .map(m => ({
        time:       m.time,
        position:   m.position,
        priceUsd:   m.txCount > 0 ? m.totalPrice / m.txCount : 0,
        totalUsd:   m.totalUsd,
        totalToken: m.totalToken,
        txCount:    m.txCount,
        txHashes:   m.txHashes,
      }))
      .sort((a, b) => a.time - b.time || a.position.localeCompare(b.position));

  } catch (err) {
    console.error("[CANDLE MARKS API]", err.message);
    return reply.code(500).send({ error: "failed_to_fetch_marks" });
  }

}

// ===============================================================
// GET MIGRATION MARK
// GET /tokens/:address/marks/migration?timeframe=1m
// ===============================================================

export async function getMigrationMark(req, reply) {

  try {

    const { address: tokenAddress } = req.params;
    const timeframe = req.query.timeframe || "1m";

    const windowSec = TIMEFRAME_SECONDS[timeframe];
    if (!windowSec) {
      return reply.code(400).send({
        error: "invalid timeframe",
        valid: Object.keys(TIMEFRAME_SECONDS),
      });
    }

    // Snap migration time langsung ke candle aktual di DB
    const { rows } = await db.query(`
      SELECT
        COALESCE(
          EXTRACT(EPOCH FROM tc_exact.start_time)::bigint,
          EXTRACT(EPOCH FROM tc_near.start_time)::bigint,
          (FLOOR(EXTRACT(EPOCH FROM tt.time) / $2) * $2)::bigint
        ) AS candle_time,
        tt.price_usdt,
        tt.amount_receive    AS amount_token,
        tt.in_usdt_payable   AS amount_usd,
        tt.tx_hash,
        tt.time              AS tx_time
      FROM token_transactions tt
      LEFT JOIN token_candles tc_exact
        ON LOWER(tc_exact.token_address) = LOWER($1)
       AND tc_exact.timeframe            = $3
       AND tc_exact.start_time           = to_timestamp(FLOOR(EXTRACT(EPOCH FROM tt.time) / $2) * $2)
      LEFT JOIN LATERAL (
        SELECT start_time
        FROM token_candles
        WHERE LOWER(token_address) = LOWER($1)
          AND timeframe            = $3
          AND start_time          <= tt.time
        ORDER BY start_time DESC
        LIMIT 1
      ) tc_near ON true
      WHERE LOWER(tt.token_address) = LOWER($1)
        AND tt.position = 'ADD_LIQUIDITY'
      ORDER BY tt.time ASC
      LIMIT 1
    `, [tokenAddress, windowSec, timeframe]);

    if (!rows.length) return null;

    const r = rows[0];

    return {
      time:       Number(r.candle_time),
      position:   "MIGRATION",
      priceUsd:   Number(r.price_usdt   || 0),
      totalUsd:   Number(r.amount_usd   || 0),
      totalToken: Number(r.amount_token || 0),
      txHash:     r.tx_hash,
      migratedAt: r.tx_time,
    };

  } catch (err) {
    console.error("[MIGRATION MARK API]", err.message);
    return reply.code(500).send({ error: "failed_to_fetch_migration_mark" });
  }

}

// ===============================================================
// GET DEV CANDLE MARKS
// GET /tokens/:address/marks/dev?timeframe=1m
// ===============================================================

export async function getDevCandleMarks(req, reply) {

  try {

    const { address: tokenAddress } = req.params;
    const timeframe = req.query.timeframe || "1m";
    const limit     = Math.min(Number(req.query.limit) || 500, 2000);

    const windowSec = TIMEFRAME_SECONDS[timeframe];
    if (!windowSec) {
      return reply.code(400).send({
        error: "invalid timeframe",
        valid: Object.keys(TIMEFRAME_SECONDS),
      });
    }

    const { sql, params } = buildMarksQuery({
      tokenAddress,
      timeframe,
      windowSec,
      extraWhere:  ["AND tt.is_dev = true"],
      extraParams: [],
      limit,
    });

    const { rows } = await db.query(sql, params);

    if (!rows.length) return [];

    // Group by (candle_time, position)
    const markMap = new Map();

    for (const tx of rows) {
      const key = `${tx.candle_time}:${tx.position}`;

      if (!markMap.has(key)) {
        markMap.set(key, {
          time:       Number(tx.candle_time),
          position:   tx.position,
          totalUsd:   0,
          totalToken: 0,
          totalPrice: 0,
          txCount:    0,
          txHashes:   [],
          wallet:     tx.wallet,
        });
      }

      const mark = markMap.get(key);
      mark.totalUsd   += Number(tx.amount_usd   || 0);
      mark.totalToken += Number(tx.amount_token || 0);
      mark.totalPrice += Number(tx.price_usdt   || 0);
      mark.txCount    += 1;
      mark.txHashes.push(tx.tx_hash);
    }

    return Array.from(markMap.values())
      .map(m => ({
        time:       m.time,
        position:   m.position,
        priceUsd:   m.txCount > 0 ? m.totalPrice / m.txCount : 0,
        totalUsd:   m.totalUsd,
        totalToken: m.totalToken,
        txCount:    m.txCount,
        txHashes:   m.txHashes,
        wallet:     m.wallet,
      }))
      .sort((a, b) => a.time - b.time || a.position.localeCompare(b.position));

  } catch (err) {
    console.error("[DEV MARKS API]", err.message);
    return reply.code(500).send({ error: "failed_to_fetch_dev_marks" });
  }

}