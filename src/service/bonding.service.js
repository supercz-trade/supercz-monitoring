// ===============================================================
// bonding.service.js (FINAL - USD NORMALIZED + BONDING LIQUIDITY)
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

  // =========================
  // NORMALIZE INPUT
  // =========================
  const amount = Number(baseAmount) || 0;
  if (!amount || !baseSymbol) return;

  // =========================
  // CONVERT TO USD
  // =========================
  let basePrice = 0;

  try {
    basePrice = await getBasePrice(baseSymbol);
  } catch (err) {
    console.error("[BONDING] price error:", err.message);
  }

  const amountUSD = amount * basePrice;

  const delta = position === "BUY"
    ? amountUSD
    : position === "SELL"
    ? -amountUSD
    : 0;

  if (!delta) return;

  // =========================
  // GET PREVIOUS STATE
  // =========================
  const { rows } = await db.query(`
    SELECT bonding_base, estimated_target
    FROM token_liquidity_state
    WHERE token_address = $1
  `, [tokenAddress]);

  const prev = rows[0] || {};
  const prevBonding = Number(prev.bonding_base || 0);

  // =========================
  // DEBUG BEFORE
  // =========================
  pushAggLog({
    stage: "BONDING_BEFORE",
    tokenAddress,
    prevBonding,
    delta,
    position,
    baseAmount,
    baseSymbol
  });

  // =========================
  // BONDING IN USD
  // =========================
  const bondingUSD = Math.max(prevBonding + delta, 0);

  // =========================
  // TARGET FIX (IMPORTANT 🔥)
  // =========================
  let target = Number(prev.estimated_target);

  if (!target || target < 1000) {
    target = await getGlobalTarget();
  }

  // =========================
  // PROGRESS
  // =========================
  const progress = target > 0
    ? bondingUSD / target
    : 0;

  // =========================
  // RESET DETECTOR
  // =========================
  if (prevBonding > bondingUSD) {
    pushAggLog({
      stage: "BONDING_RESET_DETECTED",
      tokenAddress,
      prev: prevBonding,
      next: bondingUSD
    });
  }

  // =========================
  // DEBUG BEFORE DB
  // =========================
  pushAggLog({
    stage: "BONDING_BEFORE_DB",
    tokenAddress,
    prevBonding,
    bondingUSD,
    delta,
    target
  });

  // =========================
  // UPSERT (FIX CORE BUG 🔥)
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
    VALUES ($1, $2, $2, $3, $4, $5, $6, NOW())
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
    bondingUSD,
    target,
    progress,
    "bonding", // platform
    "bonding"  // mode
  ]);

  // =========================
  // DEBUG AFTER DB
  // =========================
  pushAggLog({
    stage: "BONDING_AFTER_DB",
    tokenAddress,
    bondingUSD,
    target,
    progress
  });

  // =========================
  // CALCULATE BASE VALUE
  // =========================
  const bondingBase = basePrice > 0
    ? bondingUSD / basePrice
    : 0;

  // =========================
  // CACHE MERGE
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

  // =========================
  // DEBUG AFTER CACHE
  // =========================
  pushAggLog({
    stage: "BONDING_AFTER_CACHE",
    tokenAddress,
    nextState
  });

  setLiquidityState(tokenAddress, nextState);
}