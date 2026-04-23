// ===============================================================
// top_traders.route.js
// GET /tokens/:address/top-traders?limit=50&sort=realized_pnl
//
// Ambil semua wallet yang pernah transaksi di token ini,
// walau saldo sudah 0 — murni dari token_transactions.
// Sort options: realized_pnl | volume | buy_count | sell_count
// ===============================================================

import { db } from "../infra/database.js";

export async function getTopTraders(req, reply) {

  try {

    const { address } = req.params;
    const limit       = Math.min(Number(req.query.limit) || 50, 200);
    const sort        = req.query.sort || "realized_pnl";

    const VALID_SORTS = ["realized_pnl", "volume", "buy_count", "sell_count", "pnl_pct"];
    const sortCol     = VALID_SORTS.includes(sort) ? sort : "realized_pnl";

    const { rows } = await db.query(`
      SELECT
        tt.address_message_sender                                    AS wallet,

        COUNT(*)  FILTER (WHERE tt.position = 'BUY')                AS buy_count,
        COUNT(*)  FILTER (WHERE tt.position = 'SELL')               AS sell_count,

        COALESCE(SUM(tt.in_usdt_payable) FILTER (WHERE tt.position = 'BUY'),  0) AS buy_usd,
        COALESCE(SUM(tt.in_usdt_payable) FILTER (WHERE tt.position = 'SELL'), 0) AS sell_usd,

        COALESCE(SUM(tt.amount_base_payable) FILTER (WHERE tt.position = 'BUY'),  0) AS buy_base,
        COALESCE(SUM(tt.amount_base_payable) FILTER (WHERE tt.position = 'SELL'), 0) AS sell_base,

        COALESCE(SUM(tt.amount_receive) FILTER (WHERE tt.position = 'BUY'),  0) AS buy_tokens,
        COALESCE(SUM(tt.amount_receive) FILTER (WHERE tt.position = 'SELL'), 0) AS sell_tokens,

        -- avg buy price
        CASE
          WHEN COALESCE(SUM(tt.amount_receive) FILTER (WHERE tt.position = 'BUY'), 0) > 0
          THEN SUM(tt.in_usdt_payable) FILTER (WHERE tt.position = 'BUY')
             / SUM(tt.amount_receive)  FILTER (WHERE tt.position = 'BUY')
          ELSE 0
        END AS avg_buy_price,

        -- avg sell price
        CASE
          WHEN COALESCE(SUM(tt.amount_receive) FILTER (WHERE tt.position = 'SELL'), 0) > 0
          THEN SUM(tt.in_usdt_payable) FILTER (WHERE tt.position = 'SELL')
             / SUM(tt.amount_receive)  FILTER (WHERE tt.position = 'SELL')
          ELSE 0
        END AS avg_sell_price,

        -- realized PnL — sell capped ke buy_tokens supaya token dari luar tidak distorsi
        CASE
          WHEN COALESCE(SUM(tt.amount_receive) FILTER (WHERE tt.position = 'BUY'), 0) > 0
           AND COALESCE(SUM(tt.amount_receive) FILTER (WHERE tt.position = 'SELL'), 0) > 0
          THEN
            SUM(tt.in_usdt_payable) FILTER (WHERE tt.position = 'SELL')
            - (
                SUM(tt.in_usdt_payable) FILTER (WHERE tt.position = 'BUY')
              / SUM(tt.amount_receive)  FILTER (WHERE tt.position = 'BUY')
              * LEAST(
                  SUM(tt.amount_receive) FILTER (WHERE tt.position = 'SELL'),
                  SUM(tt.amount_receive) FILTER (WHERE tt.position = 'BUY')
                )
              )
          ELSE 0
        END AS realized_pnl,

        -- total volume (buy + sell usd)
        COALESCE(SUM(tt.in_usdt_payable) FILTER (WHERE tt.position = 'BUY'),  0)
        + COALESCE(SUM(tt.in_usdt_payable) FILTER (WHERE tt.position = 'SELL'), 0) AS volume,

        -- is_dev flag
        bool_or(tt.is_dev)                                           AS is_dev,

        -- first & last tx time
        MIN(tt.time)                                                 AS first_tx,
        MAX(tt.time)                                                 AS last_tx,

        -- tag
        MAX(tt.tag_address)                                          AS tag_address,

        -- balance sisa dari token_holders (null kalau sudah tidak ada)
        COALESCE(th.balance, 0)                                      AS balance

      FROM token_transactions tt
      LEFT JOIN token_holders th
        ON  LOWER(th.token_address)  = LOWER($1)
        AND LOWER(th.holder_address) = LOWER(tt.address_message_sender)
      WHERE LOWER(tt.token_address) = LOWER($1)
        AND tt.position IN ('BUY', 'SELL')
      GROUP BY tt.address_message_sender, th.balance
      ORDER BY ${sortCol} DESC NULLS LAST
      LIMIT $2
    `, [address, limit]);

    return rows.map((r, i) => ({
      rank:         i + 1,
      wallet:       r.wallet,
      tagAddress:   r.tag_address   || null,
      isDev:        r.is_dev        || false,
      buyCount:     Number(r.buy_count     || 0),
      sellCount:    Number(r.sell_count    || 0),
      buyUsd:       Number(r.buy_usd       || 0),
      sellUsd:      Number(r.sell_usd      || 0),
      buyBase:      Number(r.buy_base      || 0),
      sellBase:     Number(r.sell_base     || 0),
      buyTokens:    Number(r.buy_tokens    || 0),
      sellTokens:   Number(r.sell_tokens   || 0),
      avgBuyPrice:  Number(r.avg_buy_price || 0),
      avgSellPrice: Number(r.avg_sell_price|| 0),
      realizedPnl:  Number(r.realized_pnl  || 0),
      volume:       Number(r.volume        || 0),
      balance:      Number(r.balance       || 0),
      firstTx:      r.first_tx,
      lastTx:       r.last_tx,
    }));

  } catch (err) {
    console.error("[TOP TRADERS API]", err.message);
    return reply.code(500).send({ error: "failed_to_fetch_top_traders" });
  }

}