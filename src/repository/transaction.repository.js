// ===============================================================
// transaction.repository.js
// FIX: pakai withClient — satu TX = satu koneksi dari pool
//      Sebelumnya 7+ query per TX masing-masing ambil koneksi
//      sendiri → pool habis saat TX ramai → ETIMEDOUT
// ===============================================================

import { db, withClient } from "../infra/database.js";
import { publish } from "../infra/wsbroker.js";
// [ADDED]
import { getLiquidityState } from "./liquidity.repository.js";
// [ADDED]
import { updateBondingProgress } from "../service/bonding.service.js";
import { getLiquidityStateCache } from "../cache/liquidity.cache.js";
import { pushAggLog } from "../infra/aggDebugBuffer.js"; // [ADDED]

import {
  updateStatsCache,
  calcTop10Sum,
  calcHolderCount,
  setPaperHandPct,
  getOrCreateStats,
} from "./statsCache.js";

import {
  handleBuyHolder,
  handleSellHolder,
  updateHolderCount,
  getPaperHandPct,
} from "./holderStats.js";

import { updateCandle } from "./candleBuilder.js";
import { pushTxToBuffer } from "./txBuffer.js";

const TOTAL_SUPPLY = 1_000_000_000;

// ===============================================================
// INSERT TRANSACTION
// ===============================================================

export async function insertTransaction(data) {

  const wallet = data.addressMessageSender.toLowerCase();

  let amount = data.amountReceive || 0;
  if (data.position === "SELL") amount = -amount;

  // ── Semua query DB dalam satu koneksi ─────────────────────
  // FIX: withClient checkout 1 koneksi, semua query pakai koneksi
  // yang sama, release setelah selesai. Total pool usage: 1 per TX.
  await withClient(async (client) => {

    // 1. Insert transaksi
    await client.query(`
      INSERT INTO token_transactions (
        token_address, time, block_number, tx_hash, position,
        amount_receive, base_payable, amount_base_payable,
        in_usdt_payable, price_base, price_usdt,
        address_message_sender, tag_address, is_dev
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
      ON CONFLICT (tx_hash) DO NOTHING
    `, [
      data.tokenAddress, data.time, data.blockNumber, data.txHash,
      data.position, data.amountReceive, data.basePayable,
      data.amountBasePayable, data.inUSDTPayable,
      data.priceBase || 0, data.priceUSDT || 0,
      wallet, data.tagAddress || null, data.isDev || false,
    ]);

    // 2. Update token_stats price/volume/tx — paralel dengan holder logic
    // [MODIFIED]
    const statsPromise = client.query(`
  UPDATE token_stats
  SET
    price_usdt  = $2::numeric,
    marketcap   = $2::numeric * ${TOTAL_SUPPLY},
    volume_usdt = volume_usdt + $3::numeric,
    volume_24h  = volume_24h + $3::numeric, -- [ADDED]
    tx_count    = tx_count + 1,
    updated_at  = NOW()
  WHERE token_address = $1
`, [data.tokenAddress, data.priceUSDT || 0, data.inUSDTPayable || 0]);

    // 3+4. Holder logic — pakai client yang sama
    // FIX: handleBuy/SellHolder sekarang return delta (+1 / -1 / 0)
    // updateHolderCount pakai delta → tidak perlu COUNT(*) subquery lagi
    const holderPromise = (async () => {
      let delta = 0;
      if (data.position === "BUY") {
        delta = await handleBuyHolder({
          tokenAddress: data.tokenAddress,
          wallet, amount, time: data.time, client,
        });
      } else if (data.position === "SELL") {
        delta = await handleSellHolder({
          tokenAddress: data.tokenAddress,
          wallet, amount, time: data.time, client,
        });
      }
      await updateHolderCount(data.tokenAddress, client, delta);
    })();

    await statsPromise;
    await holderPromise;

    // 5. Top holder supply + holder count — fire-and-forget
    const top10Sum = calcTop10Sum(data.tokenAddress);
    const holderCount = calcHolderCount(data.tokenAddress);

    client.query(`
      UPDATE token_stats
      SET top_holder_supply = $2, holder_count = $3
      WHERE token_address = $1
    `, [data.tokenAddress, top10Sum, holderCount])
      .catch(err => console.error("[TX] top_holder update error:", err.message));

  }); // ← koneksi dikembalikan ke pool di sini

   if (data.position === "BUY" || data.position === "SELL") {
    setImmediate(() => {
pushAggLog({
  stage: "TX_BONDING_CALL",
  tokenAddress: data.tokenAddress,
  position: data.position,
  baseAmount: data.amountBasePayable,
  baseSymbol: data.basePayable,
  txHash: data.txHash
}); // [ADDED]

      updateBondingProgress({
  tokenAddress: data.tokenAddress,
  position: data.position,
  baseAmount: data.amountBasePayable,
  baseSymbol: data.basePayable
}).catch(err => {
        console.error("[BONDING] error:", err.message);
      });
    });
  }

  // ── Memory cache (sync, tidak butuh DB) ───────────────────
  const stats = updateStatsCache({
    tokenAddress: data.tokenAddress,
    wallet,
    amount,
    priceUSDT: data.priceUSDT,
    inUSDTPayable: data.inUSDTPayable,
    isDev: data.isDev,
  });

  // PaperHandPct dari cache, refresh DB setiap 20 TX
  const paperHandPct = stats.paperHandPct ?? 0;
  if (stats.txCount % 20 === 0) {
    getPaperHandPct(data.tokenAddress)
      .then(pct => setPaperHandPct(data.tokenAddress, pct))
      .catch(() => { });
  }

  const top10Sum = calcTop10Sum(data.tokenAddress);
  const holderCount = calcHolderCount(data.tokenAddress);

  // ── Candle update ─────────────────────────────────────────
  // Always use data.time (block timestamp) NOT Date.now()
  // Block timestamp ensures TX from the same block land in
  // the same 1s candle window regardless of processing order.
  // updateCandle() handles late/out-of-order TX gracefully.
  if (data.position === "BUY" || data.position === "SELL" || data.position === "ADD_LIQUIDITY") {
    pushTxToBuffer({
      tokenAddress: data.tokenAddress,
      priceUSDT: data.priceUSDT,
      inUSDTPayable: data.inUSDTPayable,
      time: data.time,
      blockNumber: data.blockNumber,
      logIndex: data.logIndex || 0,
    });
  }

  // ── Publish WS ─────────────────────────────────────────────
const liquidityState = getLiquidityStateCache(data.tokenAddress);

 let ts;

if (data.time instanceof Date) {
  ts = Math.floor(data.time.getTime() / 1000);
} else if (typeof data.time === "string") {
  ts = Math.floor(new Date(data.time).getTime() / 1000);
} else if (typeof data.time === "number") {
  ts = data.time > 1e12 ? Math.floor(data.time / 1000) : data.time;
} else {
  ts = Math.floor(Date.now() / 1000);
}

// [ADDED] devMark — hanya hitung kalau transaksi dari dev
let devMark = null;

const DEV_DUST_THRESHOLD = 1;

if (data.isDev) {
  if (data.position === "BUY") {
    devMark = "DB";
  } else if (data.position === "SELL") {
    const { rows: balRows } = await db.query(`
      SELECT COALESCE(balance, 0) AS balance
      FROM token_holders
      WHERE LOWER(token_address)  = LOWER($1)
        AND LOWER(holder_address) = LOWER($2)
      LIMIT 1
    `, [data.tokenAddress, wallet]);

    const devBalance = Number(balRows[0]?.balance || 0);
    devMark = devBalance < DEV_DUST_THRESHOLD ? "DS" : "DP";
  }
}

publish("token_update", {
  tokenAddress: data.tokenAddress,

  price: stats.price,
  marketcap: stats.marketcap,
  volume24h: stats.volume24h,
  txCount: stats.txCount,
  holderCount,

  devSupply: stats.devSupply,
  devMark,
  topHolderSupply: top10Sum,
  paperHandPct,

  // =========================
  // 🔥 STATUS
  // =========================
  mode: liquidityState?.mode || null,
  platform: liquidityState?.platform || null,
  baseSymbol: liquidityState?.base_symbol || null,

  // =========================
  // 🟢 BONDING MODE
  // =========================
  ...(liquidityState?.mode !== "dex" && {
    progress: liquidityState?.progress
      ? Number((liquidityState.progress * 100).toFixed(2))
      : 0,

    targetUSD: liquidityState?.target || 0,

    bondingLiquidity: {
      base: liquidityState?.bonding_base || 0,
      usd: liquidityState?.bonding_usd || 0
    }
  }),

  // =========================
  // 🔵 DEX MODE
  // =========================
  ...(liquidityState?.mode === "dex" && {
    liquidity: {
      base: liquidityState?.base_liquidity || 0,
      usd: liquidityState?.liquidity_usd || 0
    }
  }),

  timestamp: ts,
});

  if (data.position === "BUY" || data.position === "SELL") {
    const txPayload = {
      txHash: data.txHash,
      time: ts,
      position: data.position,
      tokenAmount: data.amountReceive,
      basePair: data.basePayable,
      baseAmount: data.amountBasePayable,
      volumeUSDT: data.inUSDTPayable || 0,
      priceBase: data.priceBase || 0,
      priceUSDT: data.priceUSDT || 0,
      wallet,
      tag: data.tagAddress || null,
      isDev: data.isDev || false,
      timestamp: ts,
    };
    publish("transaction:all", txPayload);
    // [MODIFIED]
    publish(`transaction:${data.tokenAddress}`, txPayload);
  }

}

// ===============================================================
// WARMUP CACHE
// ===============================================================

export async function warmupStatsCache() {
  try {
    const { rows: tokenRows } = await db.query(`
      SELECT 
  token_address, 
  price_usdt, 
  marketcap, 
  tx_count, 
  paperhand_pct,
  volume_24h
FROM token_stats
    `);

    // ✅ 1 query untuk semua dev supply sekaligus
    const { rows: devRows } = await db.query(`
      SELECT token_address,
             COALESCE(SUM(
               CASE
                 WHEN position = 'BUY'  THEN  amount_receive
                 WHEN position = 'SELL' THEN -amount_receive
                 ELSE 0
               END
             ), 0) AS dev_supply
      FROM token_transactions
      WHERE token_address = ANY($1) AND is_dev = true
      GROUP BY token_address
    `, [tokenRows.map(r => r.token_address)]);

    // index supaya O(1) lookup
    const devMap = Object.fromEntries(devRows.map(r => [r.token_address, r.dev_supply]));

    for (const row of tokenRows) {
      const stats = getOrCreateStats(row.token_address);

      stats.price = Number(row.price_usdt || 0);
      stats.marketcap = Number(row.marketcap || 0);
      stats.txCount = Number(row.tx_count || 0);
      stats.paperHandPct = Number(row.paperhand_pct || 0);

      // [CORRECT]
      stats.volume24h = Number(row.volume_24h || 0);

      stats.devSupply = Math.max(Number(devMap[row.token_address] || 0), 0);
    }

    // FIX: hapus LIMIT 5000 — load semua active holders ke memory
    // Sebelumnya LIMIT 5000 dibagi ke semua token → token dengan banyak
    // holder tidak ke-load semua → calcHolderCount() selalu undercount
    const { rows: holderRows } = await db.query(`
      SELECT token_address, holder_address, balance
      FROM token_holders
      WHERE balance > 0
    `);

    for (const h of holderRows) {
      const stats = getOrCreateStats(h.token_address);
      stats.holders.set(h.holder_address, Number(h.balance));
    }

    console.log(`[WARMUP] loaded ${holderRows.length} active holders into memory`);

    // FIX: repair holder_count di token_stats supaya sync dengan actual
    // Jalankan 1x saat startup — fix semua selisih yang ada di DB
    console.log(`[WARMUP] repairing holder_count in token_stats...`);
    await db.query(`
      UPDATE token_stats ts
      SET holder_count = sub.actual_count
      FROM (
        SELECT token_address, COUNT(*) AS actual_count
        FROM token_holders
        WHERE balance > 0
        GROUP BY token_address
      ) sub
      WHERE ts.token_address = sub.token_address
        AND ts.holder_count != sub.actual_count
    `);
    console.log(`[WARMUP] holder_count repaired`);

    console.log(`[WARMUP] done`);

  } catch (err) {
    console.error("[WARMUP] error:", err.message);
  }
}