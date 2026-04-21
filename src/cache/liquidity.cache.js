import { db } from "../infra/database.js";

const liquidityCache = new Map();

const STABLECOINS = new Set(['UUSD', 'USDT', 'USDC', 'USD1']);

export function setLiquidityState(tokenAddress, state) {
  liquidityCache.set(tokenAddress, state);
}

export function getLiquidityStateCache(tokenAddress) {
  return liquidityCache.get(tokenAddress) || null;
}

export async function warmupLiquidityCache() {
  try {

    // ── 1. Repair DB DEX: base_liquidity dari TX history ──────
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

    // ── 2. Repair DB bonding stablecoin: current = bonding_base
    await db.query(`
      UPDATE token_liquidity_state
      SET
        current    = bonding_base,
        progress   = CASE WHEN target > 0 THEN bonding_base / target ELSE 0 END,
        updated_at = NOW()
      WHERE mode = 'bonding'
        AND base_symbol IN ('UUSD', 'USDT', 'USDC', 'USD1')
        AND ABS(current - bonding_base) > 1
    `);

    console.log(`[WARMUP] stablecoin bonding repaired in DB`);

    // ── 3. Load semua state dari DB ───────────────────────────
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

    // ── 4. Safety check cache: stablecoin bonding ─────────────
    // Kalau DB sudah benar tapi cache masih salah karena race
    for (const [tokenAddress, cached] of liquidityCache) {
      if (
        cached.mode === 'bonding' &&
        STABLECOINS.has(cached.base_symbol) &&
        Math.abs(cached.current - cached.bonding_base) > 1
      ) {
        cached.current     = cached.bonding_base;
        cached.bonding_usd = cached.bonding_base;
        cached.progress    = cached.target > 0 ? cached.bonding_base / cached.target : 0;
        liquidityCache.set(tokenAddress, cached);
      }
    }

  } catch (err) {
    console.error("[WARMUP LIQUIDITY ERROR]", err.message);
  }
}