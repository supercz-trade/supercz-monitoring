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

    // ── 1. Load semua state dari DB ───────────────────────────
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

        mode:        row.mode,
        platform:    row.platform,
        base_symbol: row.base_symbol,
        pair_address: row.pair_address || null,
      });
    }

    console.log(`[WARMUP] liquidity cache loaded: ${rows.length}`);

    // ── 2. Repair DEX tokens: base_liquidity dari TX history ──
    // Kalau server restart, cache akan load snapshot DB lama.
    // TX yang terjadi antara snapshot dan restart tidak ter-apply.
    // Fix: hitung ulang base_liquidity = ADD_LIQUIDITY + net BUY/SELL
    const { rows: dexRepair } = await db.query(`
      SELECT
        tls.token_address,
        tls.base_symbol,
        COALESCE(liq.base_amount, 0)  AS liq_base,
        COALESCE(net.net_flow,   0)   AS net_flow
      FROM token_liquidity_state tls

      -- Base amount saat migrate (ADD_LIQUIDITY TX)
      LEFT JOIN (
        SELECT token_address, amount_base_payable AS base_amount
        FROM token_transactions
        WHERE position = 'ADD_LIQUIDITY'
      ) liq ON liq.token_address = tls.token_address

      -- Net BUY/SELL setelah migrate
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
      ) net ON net.token_address = tls.token_address

      WHERE tls.mode = 'dex'
        AND liq.base_amount IS NOT NULL
    `);

    let repaired = 0;
    for (const row of dexRepair) {
      const correctBase = Math.max(
        Number(row.liq_base) + Number(row.net_flow),
        0
      );

      const cached = liquidityCache.get(row.token_address);
      if (!cached) continue;

      // Hanya update kalau selisih signifikan (>1%)
      const current = cached.base_liquidity || 0;
      if (current > 0 && Math.abs(correctBase - current) / current < 0.01) continue;

      cached.base_liquidity = correctBase;
      liquidityCache.set(row.token_address, cached);
      repaired++;
    }

    if (repaired > 0) {
      console.log(`[WARMUP] DEX liquidity repaired in cache: ${repaired}`);
    }

  } catch (err) {
    console.error("[WARMUP LIQUIDITY ERROR]", err.message);
  }
}