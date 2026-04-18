import { db } from "../infra/database.js";

// UPSERT STATE
export async function upsertLiquidityState(data) {
  await db.query(`
    INSERT INTO token_liquidity_state (
      token_address,
      platform,
      mode,
      base_address,
      base_symbol,
      base_liquidity,
      token_liquidity,
      price_base,
      price_usd,
      liquidity_usd,
      current,
      target,
      progress,
      circulating_supply,
      is_migrated,
      pair_address,
      updated_at
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
      $11,$12,$13,$14,$15,$16,NOW()
    )
    ON CONFLICT (token_address)
    DO UPDATE SET
      platform = EXCLUDED.platform,
      mode = EXCLUDED.mode,
      base_address = EXCLUDED.base_address,
      base_symbol = EXCLUDED.base_symbol,
      base_liquidity = EXCLUDED.base_liquidity,
      token_liquidity = EXCLUDED.token_liquidity,
      price_base = EXCLUDED.price_base,
      price_usd = EXCLUDED.price_usd,
      liquidity_usd = EXCLUDED.liquidity_usd,
      current = EXCLUDED.current,
      target = EXCLUDED.target,
      progress = EXCLUDED.progress,
      circulating_supply = EXCLUDED.circulating_supply,
      is_migrated = EXCLUDED.is_migrated,
      pair_address = EXCLUDED.pair_address,
      updated_at = NOW()
  `, [
    data.tokenAddress,
    data.platform,
    data.mode,
    data.baseAddress,
    data.baseSymbol,
    data.baseLiquidity || 0,
    data.tokenLiquidity || 0,
    data.priceBase || 0,
    data.priceUSD || 0,
    data.liquidityUSD || 0,
    data.current || 0,
    data.target || 0,
    data.progress || 0,
    data.circulatingSupply || 0,
    data.isMigrated || false,
    data.pairAddress || null
  ]);
}

// GET FOR WS
export async function getLiquidityState(tokenAddress) {
  const { rows } = await db.query(`
    SELECT *
    FROM token_liquidity_state
    WHERE token_address = $1
    LIMIT 1
  `, [tokenAddress]);

  return rows[0] || null;
}