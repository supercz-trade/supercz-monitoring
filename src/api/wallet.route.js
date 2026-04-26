// ===============================================================
// wallet.route.js
// GET /wallets/:address/overview
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

    const addr = address.toLowerCase();

    const [deployResult, tradingResult] = await Promise.all([

      // ── Deploy stats — aggregate only, no array needed
      db.query(`
        SELECT
          COUNT(*)                                        AS deploy_count,
          COUNT(*) FILTER (WHERE lt.migrated = true)     AS migrated_count,
          COUNT(*) FILTER (WHERE lt.migrated = false)    AS active_count
        FROM launch_tokens lt
        WHERE lt.developer_address = $1
      `, [addr]),

      // ── Semua trading stats dalam 1 query pakai CTE ───────────
      db.query(`
        WITH base AS (
          SELECT
            token_address,
            position,
            in_usdt_payable,
            amount_receive,
            time
          FROM token_transactions
          WHERE address_message_sender = $1
            AND position IN ('BUY', 'SELL')
        ),
        summary AS (
          SELECT
            COUNT(*) FILTER (WHERE position = 'BUY')  AS buy_count,
            COUNT(*) FILTER (WHERE position = 'SELL') AS sell_count,
            COUNT(*)                                   AS total_tx_count,
            COALESCE(SUM(in_usdt_payable) FILTER (WHERE position = 'BUY'),  0) AS buy_volume_usd,
            COALESCE(SUM(in_usdt_payable) FILTER (WHERE position = 'SELL'), 0) AS sell_volume_usd,
            COALESCE(SUM(in_usdt_payable), 0)                                  AS total_volume_usd,
            COUNT(DISTINCT token_address)              AS unique_tokens,
            MIN(time)                                  AS first_seen_at,
            MAX(time)                                  AS last_seen_at
          FROM base
        ),
        per_token AS (
          SELECT
            token_address,
            SUM(in_usdt_payable) FILTER (WHERE position = 'SELL')
            - (
                SUM(in_usdt_payable) FILTER (WHERE position = 'BUY')
                / NULLIF(SUM(amount_receive) FILTER (WHERE position = 'BUY'), 0)
                * LEAST(
                    SUM(amount_receive) FILTER (WHERE position = 'SELL'),
                    SUM(amount_receive) FILTER (WHERE position = 'BUY')
                  )
              ) AS realized_pnl,
            EXTRACT(EPOCH FROM (
              MAX(time) FILTER (WHERE position = 'SELL')
              - MIN(time) FILTER (WHERE position = 'BUY')
            )) AS hold_time_seconds,
            SUM(amount_receive) FILTER (WHERE position = 'BUY')  AS total_bought,
            SUM(amount_receive) FILTER (WHERE position = 'SELL') AS total_sold
          FROM base
          GROUP BY token_address
        ),
        pnl_agg AS (
          SELECT
            SUM(realized_pnl)                                                                AS total_realized_pnl,
            AVG(hold_time_seconds) FILTER (WHERE hold_time_seconds > 0)                     AS avg_hold_time_seconds,
            COUNT(*) FILTER (WHERE realized_pnl > 0 AND total_bought > 0 AND total_sold > 0) AS win_count,
            COUNT(*) FILTER (WHERE total_bought > 0 AND total_sold > 0)                      AS total_count
          FROM per_token
        )
        SELECT s.*, p.total_realized_pnl, p.avg_hold_time_seconds, p.win_count, p.total_count
        FROM summary s, pnl_agg p
      `, [addr]),

    ]);

    // ── Parse ─────────────────────────────────────────────────

    const t          = tradingResult.rows[0];
    const winCount   = Number(t?.win_count   || 0);
    const totalCount = Number(t?.total_count || 0);
    const winRate    = totalCount > 0
      ? Number(((winCount / totalCount) * 100).toFixed(2))
      : 0;

    return {

      wallet: addr,

      // ── Deploy ───────────────────────────────────────────────
      deploy: {
        deployCount:   Number(deployResult.rows[0]?.deploy_count   || 0),
        migratedCount: Number(deployResult.rows[0]?.migrated_count || 0),
        activeCount:   Number(deployResult.rows[0]?.active_count   || 0),
      },

      // ── Trading ──────────────────────────────────────────────
      trading: {
        buyCount:         Number(t?.buy_count        || 0),
        sellCount:        Number(t?.sell_count       || 0),
        totalTxCount:     Number(t?.total_tx_count   || 0),
        buyVolumeUsd:     Number(t?.buy_volume_usd   || 0),
        sellVolumeUsd:    Number(t?.sell_volume_usd  || 0),
        totalVolumeUsd:   Number(t?.total_volume_usd || 0),
        totalRealizedPnl: Number(t?.total_realized_pnl || 0),
        firstSeenAt:      t?.first_seen_at || null,
        lastSeenAt:       t?.last_seen_at  || null
      },

      // ── Behavior ─────────────────────────────────────────────
      behavior: {
        uniqueTokensTraded: Number(t?.unique_tokens || 0),
        winRate,
        winCount,
        totalCount,
        avgHoldTimeSeconds: Number(t?.avg_hold_time_seconds || 0)
      }

    };

  } catch (err) {
    console.error("[WALLET ABOUT API]", err.message);
    console.error(err.stack);
    return reply.code(500).send({ error: "failed_to_fetch_wallet_about" });
  }

}