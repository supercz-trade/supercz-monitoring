// ===============================================================
// bonding.service.js
// FIX: atomic UPDATE — eliminasi race condition saat TX concurrent
//      Sebelumnya: read bonding_base → hitung → write (non-atomic)
//      Sekarang:   delta dikirim ke DB, DB sendiri yang akumulasi
//                  dengan GREATEST(..., 0) untuk prevent negatif
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
  // DELTA BASE (bukan USD)
  // Simpan dalam BASE unit supaya tidak terpengaruh
  // fluktuasi harga BNB antar TX concurrent
  // =========================
  const deltaBase =
    position === "BUY"
      ? amount
      : position === "SELL"
      ? -amount
      : 0;

  if (!deltaBase) return;

  // =========================
  // TARGET
  // Ambil dari DB sekali — tidak perlu read bonding_base dulu
  // =========================
  const { rows: targetRows } = await db.query(`
    SELECT estimated_target, target
    FROM token_liquidity_state
    WHERE token_address = $1
  `, [tokenAddress]);

  const prevRow = targetRows[0] || {};
  let target = Number(prevRow.estimated_target || prevRow.target || 0);

  if (!target || target < 1000) {
    target = await getGlobalTarget();
  }

  // =========================
  // ATOMIC UPDATE 🔥
  // DB akumulasi bonding_base sendiri pakai GREATEST untuk
  // prevent nilai negatif. progress & current dihitung langsung
  // di SQL dari bonding_base hasil akumulasi — tidak ada window
  // race antara read dan write.
  // =========================
  const { rows: updated } = await db.query(`
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
    VALUES (
      $1,
      GREATEST($2::numeric, 0),
      GREATEST($2::numeric, 0) * $3::numeric,
      $4::numeric,
      CASE WHEN $4::numeric > 0
        THEN GREATEST($2::numeric, 0) * $3::numeric / $4::numeric
        ELSE 0
      END,
      'bonding',
      'bonding',
      NOW()
    )
    ON CONFLICT (token_address)
    DO UPDATE SET
      bonding_base = GREATEST(token_liquidity_state.bonding_base + $2::numeric, 0),
      current      = GREATEST(token_liquidity_state.bonding_base + $2::numeric, 0) * $3::numeric,
      target       = $4::numeric,
      progress     = CASE WHEN $4::numeric > 0
                       THEN GREATEST(token_liquidity_state.bonding_base + $2::numeric, 0) * $3::numeric / $4::numeric
                       ELSE 0
                     END,
      platform     = COALESCE(token_liquidity_state.platform, 'bonding'),
      mode         = COALESCE(token_liquidity_state.mode, 'bonding'),
      updated_at   = NOW()
    RETURNING bonding_base, current, target, progress
  `, [
    tokenAddress,
    deltaBase,  // delta dalam BASE (BNB/USDT/dll)
    basePrice,  // harga base saat ini
    target,
  ]);

  // =========================
  // CACHE UPDATE — dari hasil RETURNING, bukan dari kalkulasi lokal
  // =========================
  const row = updated[0];
  if (!row) return;

  const bondingBase = Number(row.bonding_base);
  const bondingUSD  = Number(row.current);
  const progress    = Number(row.progress);

  pushAggLog({
    stage: "BONDING_CALC",
    tokenAddress,
    deltaBase,
    bondingBase,
    bondingUSD,
    target,
    progress
  });

  const prevCache = getLiquidityStateCache(tokenAddress) || {};

  const nextState = {
    ...prevCache,
    current:      bondingUSD,
    target,
    progress,
    bonding_usd:  bondingUSD,
    bonding_base: bondingBase,
    base_symbol:  baseSymbol
  };

  pushAggLog({
    stage: "BONDING_CACHE",
    tokenAddress,
    nextState
  });

  setLiquidityState(tokenAddress, nextState);
}