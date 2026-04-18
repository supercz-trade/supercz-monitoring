// [ADDED] warmup cache from DB
import { db } from "../infra/database.js";

export async function warmupLiquidityCache() {
  try {
    const { rows } = await db.query(`
      SELECT *
      FROM token_liquidity_state
    `);

    for (const row of rows) {
      liquidityCache.set(row.token_address, {
        base_liquidity: Number(row.base_liquidity || 0),
        liquidity_usd: Number(row.liquidity_usd || 0),

        bonding_base: Number(row.bonding_base || 0),
        bonding_usd: Number(row.current || 0),

        progress: Number(row.progress || 0),
        target: Number(row.target || 0),
        current: Number(row.current || 0),

        mode: row.mode,
        platform: row.platform,
        base_symbol: row.base_symbol,
      });
    }

    console.log(`[WARMUP] liquidity cache loaded: ${rows.length}`);

  } catch (err) {
    console.error("[WARMUP LIQUIDITY ERROR]", err.message);
  }
}

const liquidityCache = new Map();

export function setLiquidityState(tokenAddress, state) {
  liquidityCache.set(tokenAddress, state);
}

export function getLiquidityStateCache(tokenAddress) {
  return liquidityCache.get(tokenAddress) || null;
}