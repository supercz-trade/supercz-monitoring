// ===============================================================
// bonding.service.js (FINAL CLEAN - NO BUG)
// ===============================================================

import { db } from "../infra/database.js";
import { setLiquidityState, getLiquidityStateCache } from "../cache/liquidity.cache.js";
import { getBasePrice } from "../price/binancePrice.js";
import { pushAggLog } from "../infra/aggDebugBuffer.js";

// ===============================================================
// GLOBAL TARGET (USD)
// ===============================================================

async function getGlobalTarget() {
  const { rows } = await db.query(`
    SELECT avg_target FROM migration_stats WHERE id = 1
  `);

  return Number(rows[0]?.avg_target || 10000);
}

// ===============================================================
// MAIN FUNCTION
// ===============================================================

export async function updateBondingProgress({
  tokenAddress,
  position,
  baseAmount,
  baseSymbol
}) {

  // =========================
  // DEBUG INPUT
  // =========================
  pushAggLog({
    stage: "BONDING_INPUT",
    tokenAddress,
    position,
    baseAmount,
    baseSymbol
  });

  const amount = Number(baseAmount) || 0;
  if (!amount || !baseSymbol) return;

  // =========================
  // GET PRICE
  // =========================
  let basePrice = 0;

  try {
    basePrice = await getBasePrice(baseSymbol);
  } catch (err) {
    console.error("[BONDING] price error:", err.message);
  }

  if (!basePrice) return;

  // =========================
  // CONVERT TO USD DELTA
  // =========================
  const amountUSD = amount * basePrice;

  const deltaUSD =
    position === "BUY"
      ? amountUSD
      : position === "SELL"
      ? -amountUSD
      : 0;

  if (!deltaUSD) return;

  // =========================
  // GET PREVIOUS STATE
  // =========================
  const { rows } = await db.query(`
    SELECT bonding_base, estimated_target
    FROM token_liquidity_state
    WHERE token_address = $1
  `, [tokenAddress]);

  const prev = rows[0] || {};

  const prevBondingBase = Number(prev.bonding_base || 0);

  // =========================
  // CORE FIX (BASE FIRST 🔥)
  // =========================
  const deltaBase = deltaUSD / basePrice;

  const bondingBase = Math.max(prevBondingBase + deltaBase, 0);

  const bondingUSD = bondingBase * basePrice;

  // =========================
  // TARGET
  // =========================
  let target = Number(prev.estimated_target);

  if (!target || target < 1000) {
    target = await getGlobalTarget();
  }

  // =========================
  // PROGRESS
  // =========================
  const progress = target > 0 ? bondingUSD / target : 0;

  // =========================
  // DEBUG
  // =========================
  pushAggLog({
    stage: "BONDING_CALC",
    tokenAddress,
    prevBondingBase,
    deltaUSD,
    bondingBase,
    bondingUSD,
    target,
    progress
  });

  // =========================
  // UPSERT (FIXED 🔥)
  // =========================
  await db.query(`
    INSERT INTO token_liquidity_state (
      token_address,
      bonding_base,
      current,
      target,
      progress,
      platform,
      mode,
      updated_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
    ON CONFLICT (token_address)
    DO UPDATE SET
      bonding_base = EXCLUDED.bonding_base,
      current = EXCLUDED.current,
      target = EXCLUDED.target,
      progress = EXCLUDED.progress,
      platform = COALESCE(EXCLUDED.platform, token_liquidity_state.platform),
      mode = COALESCE(EXCLUDED.mode, token_liquidity_state.mode),
      updated_at = NOW()
  `, [
    tokenAddress,
    bondingBase, // BASE
    bondingUSD,  // USD
    target,
    progress,
    "bonding",
    "bonding"
  ]);

  // =========================
  // CACHE UPDATE
  // =========================
  const prevCache = getLiquidityStateCache(tokenAddress) || {};

  const nextState = {
    ...prevCache,

    current: bondingUSD,
    target,
    progress,

    bonding_usd: bondingUSD,
    bonding_base: bondingBase,
    base_symbol: baseSymbol
  };

  pushAggLog({
    stage: "BONDING_CACHE",
    tokenAddress,
    nextState
  });

  setLiquidityState(tokenAddress, nextState);
}