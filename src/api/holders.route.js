// ===============================================================
// holders.route.js
// GET /tokens/:address/holders?limit=100
// ===============================================================

import { db } from "../infra/database.js";

export async function getHolders(req, reply) {

  try {

    const { address } = req.params;
    const limit       = Math.min(Number(req.query.limit) || 100, 500);

    const { rows } = await db.query(`
      SELECT
        th.holder_address,
        th.balance,
        th.first_buy_time,
        th.last_updated,
        th.is_paperhand,

        -- Hitung stats langsung dari transaksi
        COALESCE(tx.buy_count,   0)  AS buy_count,
        COALESCE(tx.sell_count,  0)  AS sell_count,

        COALESCE(tx.buy_usd,     0)  AS buy_usd,
        COALESCE(tx.sell_usd,    0)  AS sell_usd,

        COALESCE(tx.buy_base,    0)  AS buy_base,
        COALESCE(tx.sell_base,   0)  AS sell_base,

        COALESCE(tx.buy_tokens,  0)  AS buy_tokens,
        COALESCE(tx.sell_tokens, 0)  AS sell_tokens,

        -- Avg buy price
        CASE
          WHEN COALESCE(tx.buy_tokens, 0) > 0
          THEN tx.buy_usd / tx.buy_tokens
          ELSE 0
        END AS avg_buy_price,

        -- Realized PnL (proporsional dari token yang sudah dijual)
        CASE
          WHEN COALESCE(tx.buy_tokens, 0) > 0 AND COALESCE(tx.sell_tokens, 0) > 0
          THEN tx.sell_usd - (tx.buy_usd / tx.buy_tokens * tx.sell_tokens)
          ELSE 0
        END AS realized_pnl

      FROM token_holders th

      LEFT JOIN (
        SELECT
          address_message_sender,

          COUNT(*)  FILTER (WHERE position = 'BUY')  AS buy_count,
          COUNT(*)  FILTER (WHERE position = 'SELL') AS sell_count,

          COALESCE(SUM(in_usdt_payable)     FILTER (WHERE position = 'BUY'),  0) AS buy_usd,
          COALESCE(SUM(in_usdt_payable)     FILTER (WHERE position = 'SELL'), 0) AS sell_usd,

          COALESCE(SUM(amount_base_payable) FILTER (WHERE position = 'BUY'),  0) AS buy_base,
          COALESCE(SUM(amount_base_payable) FILTER (WHERE position = 'SELL'), 0) AS sell_base,

          COALESCE(SUM(amount_receive)      FILTER (WHERE position = 'BUY'),  0) AS buy_tokens,
          COALESCE(SUM(amount_receive)      FILTER (WHERE position = 'SELL'), 0) AS sell_tokens

        FROM token_transactions
        WHERE LOWER(token_address) = LOWER($1)
          AND position IN ('BUY', 'SELL')
        GROUP BY address_message_sender
      ) tx ON LOWER(th.holder_address) = LOWER(tx.address_message_sender)

      WHERE LOWER(th.token_address) = LOWER($1)
        AND th.balance > 0

      ORDER BY th.balance DESC
      LIMIT $2
    `, [address, limit]);

    const totalBalance = rows.reduce((sum, r) => sum + Number(r.balance || 0), 0);

    return rows.map((r, i) => ({
      rank:          i + 1,
      holderAddress: r.holder_address,
      balance:       Number(r.balance       || 0),
      pctOfSupply:   totalBalance > 0
                       ? Number(r.balance) / totalBalance * 100
                       : 0,
      firstBuyTime:  r.first_buy_time,
      lastUpdated:   r.last_updated,
      isPaperhand:   r.is_paperhand || false,
      buyCount:      Number(r.buy_count     || 0),
      sellCount:     Number(r.sell_count    || 0),
      buyUsd:        Number(r.buy_usd       || 0),
      sellUsd:       Number(r.sell_usd      || 0),
      buyBase:       Number(r.buy_base      || 0),
      sellBase:      Number(r.sell_base     || 0),
      buyTokens:     Number(r.buy_tokens    || 0),
      sellTokens:    Number(r.sell_tokens   || 0),
      avgBuyPrice:   Number(r.avg_buy_price || 0),
      realizedPnl:   Number(r.realized_pnl  || 0),
    }));

  } catch (err) {
    console.error("[HOLDERS API]", err.message);
    return reply.code(500).send({ error: "failed_to_fetch_holders" });
  }

}