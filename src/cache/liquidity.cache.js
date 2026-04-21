import { db } from "../infra/database.js";

const liquidityCache = new Map();

export function setLiquidityState(tokenAddress, state) {
  liquidityCache.set(tokenAddress, state);
}

export function getLiquidityStateCache(tokenAddress) {
  return liquidityCache.get(tokenAddress) || null;
}

export async function warmupLiquidityCache() {
  try {

    // ── 1. Repair DB dulu sebelum load ke cache ───────────────
    // Hitung ulang base_liquidity DEX dari TX history setiap restart
    // supaya tidak drift akibat TX yang terjadi saat server down
    await db.query(`
      UPDATE token_liquidity_state tls
      SET
        base_liquidity = GREATEST(liq.base_amount + COALESCE(net.net_flow, 0), 0),
        liquidity_usd  = GREATEST(liq.base_amount + COALESCE(net.net_flow, 0), 0) * COALESCE(
          (SELECT AVG(in_usdt_payable / NULLIF(amount_base_payable, 0))
           FROM token_transactions recent
           WHERE recent.token_address = tls.token_address
             AND recent.position IN ('BUY','SELL')
             AND recent.amount_base_payable > 0
             AND recent.time > NOW() - INTERVAL '2 hours'),
          638
        ),
        updated_at = NOW()
      FROM (
        SELECT token_address, amount_base_payable AS base_amount
        FROM token_transactions
        WHERE position = 'ADD_LIQUIDITY'
      ) liq
      LEFT JOIN (
        SELECT
          tt.token_address,
          SUM(CASE WHEN tt.position = 'BUY'  THEN  tt.amount_base_payable
                   WHEN tt.position = 'SELL' THEN -tt.amount_base_payable
                   ELSE 0 END) AS net_flow
        FROM token_transactions tt
        JOIN token_migrate tm ON tm.token_address = tt.token_address
        WHERE tt.position IN ('BUY', 'SELL')
          AND tt.time > tm.created_at
        GROUP BY tt.token_address
      ) net ON net.token_address = liq.token_address
      WHERE tls.token_address = liq.token_address
        AND tls.mode = 'dex'
    `);

    console.log(`[WARMUP] DEX liquidity repaired in DB`);

    // ── 2. Load semua state dari DB (sudah benar) ─────────────
    const { rows } = await db.query(`SELECT * FROM token_liquidity_state`);

    for (const row of rows) {
      liquidityCache.set(row.token_address, {
        base_liquidity: Number(row.base_liquidity || 0),
        liquidity_usd:  Number(row.liquidity_usd  || 0),

        bonding_base: Number(row.bonding_base || 0),
        bonding_usd:  Number(row.current      || 0),

        progress: Number(row.progress || 0),
        target:   Number(row.target   || 0),
        current:  Number(row.current  || 0),

        mode:         row.mode,
        platform:     row.platform,
        base_symbol:  row.base_symbol,
        pair_address: row.pair_address || null,
      });
    }

    console.log(`[WARMUP] liquidity cache loaded: ${rows.length}`);

  } catch (err) {
    console.error("[WARMUP LIQUIDITY ERROR]", err.message);
  }
}