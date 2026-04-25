// ===============================================================
// wallet.route.js
// GET /wallets/:address/about
// ===============================================================

import { db } from "../infra/database.js";

function isValidAddress(address) {
  return typeof address === "string" && /^0x[a-fA-F0-9]{40}$/.test(address);
}

export async function getWalletOverview(req, reply) {

  try {

    const { address } = req.params;

    if (!isValidAddress(address)) {
      return reply.code(400).send({ error: "invalid_address" });
    }

    const [
      deployResult,
      tradingResult,
      uniqueTokensResult,
      winRateResult,
      avgHoldResult
    ] = await Promise.all([

      // ── Deploy stats ─────────────────────────────────────────
      db.query(`
        SELECT
          lt.token_address,
          lt.symbol,
          lt.image_url,
          COALESCE(
            (
              SELECT MAX(price_usdt)
              FROM token_transactions
              WHERE LOWER(token_address) = LOWER(lt.token_address)
                AND position IN ('BUY', 'SELL')
            ), 0
          ) AS all_time_high
        FROM launch_tokens lt
        WHERE LOWER(lt.developer_address) = LOWER($1)
        ORDER BY lt.launch_time DESC
      `, [address]),

      // ── Trading stats ────────────────────────────────────────
      db.query(`
        SELECT
          COUNT(*) FILTER (WHERE position = 'BUY')  AS buy_count,
          COUNT(*) FILTER (WHERE position = 'SELL') AS sell_count,
          COUNT(*)                                   AS total_tx_count,

          COALESCE(SUM(in_usdt_payable) FILTER (WHERE position = 'BUY'),  0) AS buy_volume_usd,
          COALESCE(SUM(in_usdt_payable) FILTER (WHERE position = 'SELL'), 0) AS sell_volume_usd,
          COALESCE(SUM(in_usdt_payable), 0)                                  AS total_volume_usd,

          MIN(time) AS first_seen_at,
          MAX(time) AS last_seen_at
        FROM token_transactions
        WHERE LOWER(address_message_sender) = LOWER($1)
          AND position IN ('BUY', 'SELL')
      `, [address]),

      // ── Unique tokens traded ─────────────────────────────────
      db.query(`
        SELECT COUNT(DISTINCT token_address) AS unique_tokens
        FROM token_transactions
        WHERE LOWER(address_message_sender) = LOWER($1)
          AND position IN ('BUY', 'SELL')
      `, [address]),

      // ── Win rate — per token, hitung realized pnl ────────────
      db.query(`
        SELECT
          COUNT(*) FILTER (WHERE realized_pnl > 0) AS win_count,
          COUNT(*)                                  AS total_count
        FROM (
          SELECT
            token_address,
            SUM(in_usdt_payable) FILTER (WHERE position = 'SELL')
            - (
                SUM(in_usdt_payable)  FILTER (WHERE position = 'BUY')
              / NULLIF(SUM(amount_receive) FILTER (WHERE position = 'BUY'), 0)
              * LEAST(
                  SUM(amount_receive) FILTER (WHERE position = 'SELL'),
                  SUM(amount_receive) FILTER (WHERE position = 'BUY')
                )
              ) AS realized_pnl
          FROM token_transactions
          WHERE LOWER(address_message_sender) = LOWER($1)
            AND position IN ('BUY', 'SELL')
          GROUP BY token_address
          HAVING SUM(amount_receive) FILTER (WHERE position = 'BUY') > 0
             AND SUM(amount_receive) FILTER (WHERE position = 'SELL') > 0
        ) per_token
      `, [address]),

      // ── Total realized pnl + avg hold time ───────────────────
      db.query(`
        SELECT
          SUM(realized_pnl) AS total_realized_pnl,
          AVG(hold_time_seconds) FILTER (WHERE hold_time_seconds > 0) AS avg_hold_time_seconds
        FROM (
          SELECT
            token_address,

            -- realized pnl per token
            SUM(in_usdt_payable) FILTER (WHERE position = 'SELL')
            - (
                SUM(in_usdt_payable)  FILTER (WHERE position = 'BUY')
              / NULLIF(SUM(amount_receive) FILTER (WHERE position = 'BUY'), 0)
              * LEAST(
                  SUM(amount_receive) FILTER (WHERE position = 'SELL'),
                  SUM(amount_receive) FILTER (WHERE position = 'BUY')
                )
              ) AS realized_pnl,

            -- hold time: jarak firstBuy → lastSell dalam detik
            EXTRACT(EPOCH FROM (
              MAX(time) FILTER (WHERE position = 'SELL')
              - MIN(time) FILTER (WHERE position = 'BUY')
            )) AS hold_time_seconds

          FROM token_transactions
          WHERE LOWER(address_message_sender) = LOWER($1)
            AND position IN ('BUY', 'SELL')
          GROUP BY token_address
        ) per_token
      `, [address]),

    ]);

    // ── Parse ─────────────────────────────────────────────────

    const trading     = tradingResult.rows[0];
    const winRow      = winRateResult.rows[0];
    const pnlHoldRow  = avgHoldResult.rows[0];

    const winCount    = Number(winRow?.win_count   || 0);
    const totalCount  = Number(winRow?.total_count || 0);
    const winRate     = totalCount > 0
      ? Number(((winCount / totalCount) * 100).toFixed(2))
      : 0;

    return {

      wallet: address.toLowerCase(),

      // ── Deploy ───────────────────────────────────────────────
      deploy: {
        deployCount: deployResult.rows.length,
        deployData:  deployResult.rows.map(r => ({
          address:     r.token_address,
          symbol:      r.symbol,
          imageUrl:    r.image_url   || null,
          allTimeHigh: Number(r.all_time_high || 0)
        }))
      },

      // ── Trading ──────────────────────────────────────────────
      trading: {
        buyCount:         Number(trading?.buy_count        || 0),
        sellCount:        Number(trading?.sell_count       || 0),
        totalTxCount:     Number(trading?.total_tx_count   || 0),
        buyVolumeUsd:     Number(trading?.buy_volume_usd   || 0),
        sellVolumeUsd:    Number(trading?.sell_volume_usd  || 0),
        totalVolumeUsd:   Number(trading?.total_volume_usd || 0),
        totalRealizedPnl: Number(pnlHoldRow?.total_realized_pnl || 0),
        firstSeenAt:      trading?.first_seen_at || null,
        lastSeenAt:       trading?.last_seen_at  || null
      },

      // ── Behavior ─────────────────────────────────────────────
      behavior: {
        uniqueTokensTraded: Number(uniqueTokensResult.rows[0]?.unique_tokens || 0),
        winRate,
        winCount,
        totalCount,
        avgHoldTimeSeconds: Number(pnlHoldRow?.avg_hold_time_seconds || 0)
      }

    };

  } catch (err) {
    console.error("[WALLET ABOUT API]", err.message);
    console.error(err.stack);
    return reply.code(500).send({ error: "failed_to_fetch_wallet_about" });
  }

}