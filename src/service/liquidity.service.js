import { getBasePrice } from "../price/binancePrice.js";
import { upsertLiquidityState } from "../repository/liquidity.repository.js";
import { setLiquidityState, getLiquidityStateCache } from "../cache/liquidity.cache.js";
import { pushAggLog } from "../infra/aggDebugBuffer.js"; // [ADDED]

// ===============================================================
// UPDATE LIQUIDITY STATE
// ===============================================================

export async function updateLiquidityState(payload) {

  // =========================
  // DEBUG INPUT
  // =========================
  pushAggLog({
    stage: "LIQ_INPUT",
    tokenAddress: payload.tokenAddress,
    baseLiquidity: payload.baseLiquidity,
    baseSymbol: payload.baseSymbol,
    mode: payload.mode,
    platform: payload.platform
  }); // [ADDED]

  let basePrice = 0;

  try {
    basePrice = payload.baseSymbol
      ? await getBasePrice(payload.baseSymbol)
      : 0;
  } catch (err) {
    pushAggLog({
      stage: "LIQ_PRICE_ERROR",
      tokenAddress: payload.tokenAddress,
      error: err.message
    }); // [ADDED]
  }

  const liquidityUSD = (payload.baseLiquidity || 0) * basePrice;

  // =========================
  // DB UPDATE
  // =========================
  await upsertLiquidityState({
    ...payload,
    priceUSD: (payload.priceBase || 0) * basePrice,
    liquidityUSD
  });

  // =========================
  // CACHE MERGE
  // =========================
  const prev = getLiquidityStateCache(payload.tokenAddress) || {};

  // =========================
  // DEBUG BEFORE
  // =========================
  pushAggLog({
    stage: "LIQ_BEFORE",
    tokenAddress: payload.tokenAddress,
    prev
  }); // [ADDED]

  const nextState = {
    // preserve old state
    ...prev,

    // =========================
    // UPDATE SELECTIVE FIELDS
    // =========================
    base_liquidity: payload.baseLiquidity ?? prev.base_liquidity ?? 0,
    liquidity_usd: liquidityUSD ?? prev.liquidity_usd ?? 0,

    progress: payload.progress ?? prev.progress ?? 0,
    current: payload.current ?? prev.current ?? 0,
    target: payload.target ?? prev.target ?? 0,

    mode: payload.mode ?? prev.mode ?? null,
    platform: payload.platform ?? prev.platform ?? null,
    is_migrated: payload.isMigrated ?? prev.is_migrated ?? false,

    base_symbol: payload.baseSymbol ?? prev.base_symbol ?? null,
  };

  // =========================
  // 🚨 RESET DETECTOR
  // =========================
  if ((prev.base_liquidity || 0) > (nextState.base_liquidity || 0)) {
    pushAggLog({
      stage: "LIQ_RESET_DETECTED",
      tokenAddress: payload.tokenAddress,
      prev: prev.base_liquidity,
      next: nextState.base_liquidity
    }); // [ADDED]
  }

  // =========================
  // DEBUG AFTER
  // =========================
  pushAggLog({
    stage: "LIQ_AFTER",
    tokenAddress: payload.tokenAddress,
    nextState
  }); // [ADDED]

  // =========================
  // SAVE TO CACHE
  // =========================
  setLiquidityState(payload.tokenAddress, nextState);

}