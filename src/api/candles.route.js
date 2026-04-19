// ===============================================================
// candles.route.js
// ===============================================================

import { db } from "../infra/database.js";

// ===============================================================
// CONFIG
// ===============================================================

const ALLOWED_TIMEFRAMES = new Set([
  "1s", "15s", "30s",
  "1m", "5m", "15m", "30m",
  "1h", "4h", "1d"
]);

// ===============================================================
// UTILS
// ===============================================================

function isValidAddress(address) {
  return typeof address === "string" && /^0x[a-fA-F0-9]{40}$/.test(address);
}

// ===============================================================
// GET CANDLES
// ===============================================================

export async function getCandles(req, reply) {

  try {

    const { address } = req.params;
    const timeframe = req.query.tf || "1m";
    const limit = Math.min(Number(req.query.limit) || 1000, 1000);

    if (!isValidAddress(address)) {
      return reply.code(400).send({ error: "invalid_address" });
    }

    if (!ALLOWED_TIMEFRAMES.has(timeframe)) {
      return reply.code(400).send({ error: "invalid_timeframe" });
    }

    const { rows } = await db.query(`
      SELECT
        EXTRACT(EPOCH FROM start_time AT TIME ZONE 'UTC')::bigint AS time_epoch,
        open,
        high,
        low,
        close,
        volume_usdt AS volume,
        tx_count
      FROM token_candles
      WHERE LOWER(token_address) = LOWER($1)
        AND timeframe = $2
      ORDER BY start_time DESC
      LIMIT $3
    `, [address, timeframe, limit]);

    return rows.map(c => ({
      time: Number(c.time_epoch),
      open: Number(c.open),
      high: Number(c.high),
      low: Number(c.low),
      close: Number(c.close),
      volume: Number(c.volume),
      txCount: Number(c.tx_count),
    }));

  } catch (err) {
    console.error("[CANDLE API ERROR]", err);
    return reply.code(500).send({ error: "failed_to_fetch_candles" });
  }

}

// ===============================================================
// GET EVENTS (MARKERS)
// ===============================================================
const TOTAL_SUPPLY = 1_000_000_000;
export async function getEvents(req, reply) {

  try {

    const { address } = req.params;
    const limit = Math.min(Number(req.query.limit) || 500, 100);

    if (!isValidAddress(address)) {
      return reply.code(400).send({ error: "invalid_address" });
    }

    const { rows } = await db.query(`
      SELECT
        EXTRACT(EPOCH FROM time AT TIME ZONE 'UTC')::bigint AS time_epoch,
        position,
        is_dev,
        tag_address,
        tx_hash,
        address_message_sender,
        amount_receive,
        in_usdt_payable,
        price_usdt
      FROM token_transactions
      WHERE LOWER(token_address) = LOWER($1)
        AND (
          is_dev = true
          OR position = 'ADD_LIQUIDITY'
        )
      ORDER BY time DESC
      LIMIT $2
    `, [address, limit]);

    const events = rows.map(tx => {

      const time = Number(tx.time_epoch);
      if (!time || isNaN(time)) return null;

      const usd   = Number(tx.in_usdt_payable ?? 0);
      const token = Number(tx.amount_receive ?? 0);
      const price = Number(tx.price_usdt ?? 0);
      const mc    = price > 0 ? price * TOTAL_SUPPLY : 0;

      const stats = {
        buy: {
          txCount: tx.position === "BUY" && tx.is_dev ? 1 : 0,
          totalUsd: tx.position === "BUY" && tx.is_dev ? usd : 0,
          totalToken: tx.position === "BUY" && tx.is_dev ? token : 0
        },
        sell: {
          txCount: tx.position === "SELL" && tx.is_dev ? 1 : 0,
          totalUsd: tx.position === "SELL" && tx.is_dev ? usd : 0,
          totalToken: tx.position === "SELL" && tx.is_dev ? Math.abs(token) : 0
        },
        avgMcUsd: mc
      };

      if (tx.position === "BUY" && tx.is_dev) {
        return {
          time,
          type: "DEV_BUY",
          label: "DB",
          color: "#22c55e",
          txHash: tx.tx_hash,
          wallet: tx.address_message_sender,
          stats
        };
      }

      if (tx.position === "SELL" && tx.is_dev) {
        return {
          time,
          type: "DEV_SELL",
          label: "DS",
          color: "#ef4444",
          txHash: tx.tx_hash,
          wallet: tx.address_message_sender,
          stats
        };
      }

      if (tx.position === "ADD_LIQUIDITY") {
        return {
          time,
          type: "MIGRATE",
          label: "M",
          color: "#3b82f6",
          txHash: tx.tx_hash,
          stats
        };
      }

      return null;

    }).filter(Boolean);

    return events;

  } catch (err) {
    console.error("[EVENT API ERROR FULL]", err.message);
    return reply.code(500).send({
      error: "failed_to_fetch_events",
      message: err.message
    });
  }

}

// ===============================================================
// GET EVENTS BY WALLET ADDRESS
// ===============================================================
export async function getEventsByAddress(req, reply) {

  try {

    const { address, wallet } = req.params;
    const limit = Math.min(Number(req.query.limit) || 500, 100);

    if (!isValidAddress(address) || !isValidAddress(wallet)) {
      return reply.code(400).send({ error: "invalid_address" });
    }

    const { rows } = await db.query(`
      SELECT
        EXTRACT(EPOCH FROM time AT TIME ZONE 'UTC')::bigint AS time_epoch,
        position,
        is_dev,
        tag_address,
        tx_hash,
        address_message_sender,
        amount_receive,
        in_usdt_payable,
        price_usdt
      FROM token_transactions
      WHERE LOWER(token_address) = LOWER($1)
        AND LOWER(address_message_sender) = LOWER($2)
      ORDER BY time DESC
      LIMIT $3
    `, [address, wallet, limit]);

    const events = rows.map(tx => {

      const time = Number(tx.time_epoch);
      if (!time || isNaN(time)) return null;

      const usd   = Number(tx.in_usdt_payable ?? 0);
      const token = Number(tx.amount_receive ?? 0);
      const price = Number(tx.price_usdt ?? 0);
      const mc    = price > 0 ? price * TOTAL_SUPPLY : 0;

      const stats = {
        buy: {
          txCount: tx.position === "BUY" ? 1 : 0,
          totalUsd: tx.position === "BUY" ? usd : 0,
          totalToken: tx.position === "BUY" ? token : 0
        },
        sell: {
          txCount: tx.position === "SELL" ? 1 : 0,
          totalUsd: tx.position === "SELL" ? usd : 0,
          totalToken: tx.position === "SELL" ? Math.abs(token) : 0
        },
        avgMcUsd: mc
      };

      if (tx.position === "BUY") {
        return {
          time,
          type: "BUY",
          label: "B",
          color: "#22c55e",
          txHash: tx.tx_hash,
          wallet: tx.address_message_sender,
          stats
        };
      }

      if (tx.position === "SELL") {
        return {
          time,
          type: "SELL",
          label: "S",
          color: "#ef4444",
          txHash: tx.tx_hash,
          wallet: tx.address_message_sender,
          stats
        };
      }

      return null;

    }).filter(Boolean);

    return events;

  } catch (err) {
    console.error("[EVENT BY ADDRESS ERROR]", err.message);
    return reply.code(500).send({
      error: "failed_to_fetch_events_by_address",
      message: err.message
    });
  }

}