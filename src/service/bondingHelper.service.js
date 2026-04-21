import { getTokenInfoSafe } from "../infra/helper3.js";
import { setLiquidityState, getLiquidityStateCache } from "../cache/liquidity.cache.js";

const cache = new Map();

// TTL 2 detik
const TTL = 2000;

export async function syncBondingFromHelper(tokenAddress) {

  const now = Date.now();
  const cached = cache.get(tokenAddress);

  // =========================
  // 🔥 CACHE HIT
  // =========================
  if (cached && (now - cached.ts < TTL)) {
    return cached.data;
  }

  try {
    const info = await getTokenInfoSafe(tokenAddress);

    const funds = info.funds;
    const maxFunds = info.maxFunds;

    const progress = maxFunds > 0 ? funds / maxFunds : 0;

    const prev = getLiquidityStateCache(tokenAddress) || {};

    // ❌ STOP kalau sudah DEX
    if (prev?.mode === "dex") return prev;

    const nextState = {
      ...prev,
      bonding_base: funds,
      progress,
      target: maxFunds,
      is_migrated: info.liquidityAdded
    };

    setLiquidityState(tokenAddress, nextState);

    // =========================
    // SAVE CACHE
    // =========================
    cache.set(tokenAddress, {
      ts: now,
      data: nextState
    });

    return nextState;

  } catch (err) {
    console.error("[HELPER ERROR]", err.message);
    return null;
  }
}