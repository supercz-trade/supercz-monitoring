// ===============================================================
// fourmemeHandler.js
// Handler untuk Four.meme — Create Token, Buy/Sell, Add Liquidity
// ===============================================================

import { ethers } from "ethers";
import { TOPICS } from "../infra/topics.js";
import { publish } from "../infra/wsbroker.js";
import { getTransaction, getTransactionReceipt, getContractFields } from "../infra/rpcQueue.js";
import { rpcTxProvider } from "../infra/provider.js";

import { processLaunch } from "../service/fourmeme.service.js";
import { getLaunchByToken, setTokenMigrated } from "../repository/launch.repository.js";
import { insertTransaction } from "../repository/transaction.repository.js";
import { insertPairLiquidity } from "../repository/pairLiquidity.repository.js";
import { insertTokenMigrate } from "../repository/tokenMigrate.repository.js";
import { addPairToMemory } from "./pancakeHandler.js";

import { getBasePrice } from "../price/binancePrice.js";
import { getHelper3TokenInfo } from "../infra/helper3.js";
import { logTrade, logCreate, logAddLiquidity, log } from "../infra/logger.js";
// [ADDED]
import { updateLiquidityState } from "../service/liquidity.service.js";
import { updateMigrationStats } from "../service/migrationStats.service.js";

// FIX: launchCache dengan TTL 10 menit
// Map biasa tanpa eviction → bisa stale + memory leak kalau token banyak
const LAUNCH_CACHE_TTL = 10 * 60 * 1000; // 10 menit
const launchCache = new Map(); // tokenAddress → { data, expiresAt }

function getLaunchCache(tokenAddress) {
  const entry = launchCache.get(tokenAddress);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    launchCache.delete(tokenAddress);
    return null;
  }
  return entry.data;
}

function setLaunchCache(tokenAddress, data) {
  launchCache.set(tokenAddress, {
    data,
    expiresAt: Date.now() + LAUNCH_CACHE_TTL,
  });
}

// ================= CONSTANTS =================

const TOTAL_SUPPLY = 1_000_000_000;
const FOUR_MANAGER = process.env.FOUR_MEME_MANAGER?.toLowerCase();
const CREATE_SELECTOR = "0x519ebb10";
const ADD_LIQ_SELECTOR = "0xe3412e3d";

const TOPIC_BUY = "0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942";
const TOPIC_SELL = "0x0a5575b3648bae2210cee56bf33254cc1ddfbc7bf637c0af2ac18b14fb1bae19";

const BASE_TOKEN_WHITELIST = {
  BNB: "0x0000000000000000000000000000000000000000",
  WBNB: "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
  USDT: "0x55d398326f99059ff775485246999027b3197955",
  USD1: "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d",
  USDC: "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
  CAKE: "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82",
  ASTER: "0x000Ae314E2A2172a039B26378814C252734f556A",
  U: "0xcE24439F2D9C6a2289F741120FE202248B666666",
  币安人生: "0x924fa68a0FC644485b8df8AbfA0A41C2e7744444",
  FORM: "0x5b73A93b4E5e4f1FD27D8b3F8C97D69908b5E284",
  UUSD: "0x61a10e8556bed032ea176330e7f17d6a12a10000",
};

const BASE_ADDRESS_MAP = Object.fromEntries(
  Object.entries(BASE_TOKEN_WHITELIST).map(
    ([symbol, addr]) => [addr.toLowerCase(), symbol]
  )
);

// ================= UTILS =================

const safeLower = v => (typeof v === "string" ? v.toLowerCase() : null);

function getBaseSymbol(quoteAddress) {
  if (!quoteAddress) return "UNKNOWN";
  return BASE_ADDRESS_MAP[quoteAddress.toLowerCase()] || "UNKNOWN";
}

// ===============================================================
// MAIN HANDLER
// ===============================================================

export async function handleFourmemeBlock({ txMap, block, blockNumber }) {

  // FIX: sequential — bukan parallel. Kalau parallel setiap TX ambil
  // koneksi pool sendiri → pool habis saat block ramai → ETIMEDOUT
  for (const [txHash, _logs] of txMap.entries()) {

    try {

      const [tx, receipt] = await Promise.all([
        getTransaction(txHash),
        getTransactionReceipt(txHash)
      ]);

      if (!tx || !receipt) continue;

      const method = tx.data?.slice(0, 10);

      if (safeLower(tx.to) === FOUR_MANAGER && method === CREATE_SELECTOR) {
        await _handleCreate({ tx, receipt, block, blockNumber });
        continue;
      }

      // [MODIFIED] ALWAYS TRY DETECT MIGRATION
      const handled = await _handleAddLiquidity({ tx, receipt, block, blockNumber });
      if (handled) continue;

      await _handleBuySell({ tx, receipt, block, blockNumber });

    } catch (err) {
      log.error("[FOURMEME] TX error:", txHash, err.message);
      log.error(err.stack);
    }
  }
}

// ===============================================================
// CREATE TOKEN
// ===============================================================

async function _handleCreate({ tx, receipt, block, blockNumber }) {

  let tokenAddress = null;
  let tokenAmount = 0;
  let baseAmount = 0;
  let devAddress = null;

  // ===============================
  // ambil data dari BUY event
  // ===============================

  for (const evLog of receipt.logs) {

    if (evLog.topics?.[0] !== TOPIC_BUY) continue;

    try {

      const decoded = ethers.AbiCoder.defaultAbiCoder().decode(
        ["address", "address", "uint256", "uint256", "uint256", "uint256", "uint256", "uint256"],
        evLog.data
      );

      tokenAddress = safeLower(decoded[0]);     // [MODIFIED] langsung dari event
      devAddress = safeLower(decoded[1]);     // [MODIFIED] wallet dev

      const launchInfo = await processLaunch(tokenAddress, {
        onchainLaunchTime: new Date(block.timestamp * 1000).toISOString()
      });

      if (!launchInfo?.basePair) return;

      tokenAmount = Number(
        ethers.formatUnits(decoded[3], launchInfo.decimals ?? 18)
      );

      const basePaid = Number(ethers.formatUnits(decoded[4], 18));
      const fee = Number(ethers.formatUnits(decoded[5], 18));

      // basePaid dari event sudah termasuk fee — jangan ditambah lagi
      baseAmount = basePaid;

      const baseSymbol = launchInfo.basePair;
      const basePriceUSD = getBasePrice(baseSymbol);

      const priceBase = tokenAmount > 0 ? baseAmount / tokenAmount : 0;
      const priceUSDT = priceBase * basePriceUSD;
      const volumeUSDT = baseAmount * basePriceUSD;

      publish("new_token", {
        tokenAddress,
        symbol: launchInfo.symbol,
        name: launchInfo.name,
        basePair: launchInfo.basePair,
        baseAddress: launchInfo.baseAddress,
        imageUrl: launchInfo.imageUrl || null,
        description: launchInfo.description || null,
        website: launchInfo.websiteUrl || null,
        telegram: launchInfo.telegramUrl || null,
        twitter: launchInfo.twitterUrl || null,
        devAddress,
        price: priceUSDT,
        marketcap: priceUSDT * TOTAL_SUPPLY,
        volume24h: volumeUSDT,
        txCount: 1,
        holderCount: 1,
        launchTime: block.timestamp * 1000,
        txHash: tx.hash,
        taxBuy: launchInfo.taxBuy ?? 0,   // ← tambah
        taxSell: launchInfo.taxSell ?? 0,   // ← tambah
        source: "four_meme"
      });

      logCreate({
        platform: "fourmeme",
        tokenAddress,
        tokenSymbol: launchInfo.symbol,
        tokenName: launchInfo.name,
        creator: devAddress,
        basePair: baseSymbol,
        baseAddress: launchInfo.baseAddress,
        txHash: tx.hash,
        blockNumber,
        timestamp: block.timestamp * 1000
      });

      await insertTransaction({
        tokenAddress,
        time: new Date(block.timestamp * 1000).toISOString(),
        blockNumber,
        txHash: tx.hash,
        position: "BUY",
        amountReceive: tokenAmount,
        basePayable: baseSymbol,
        amountBasePayable: baseAmount,
        inUSDTPayable: volumeUSDT,
        priceBase,
        priceUSDT,
        addressMessageSender: devAddress,
        tagAddress: "Developer",
        isDev: true
      });

      break;

    } catch (err) {
      log.warn("[FOURMEME][CREATE] decode failed:", err.message);
    }
  }

}

// ===============================================================
// BUY / SELL
// ===============================================================

async function _handleBuySell({ tx, receipt, block, blockNumber }) {

  for (const evLog of receipt.logs) {

    const topic = evLog.topics?.[0];
    if (topic !== TOPIC_BUY && topic !== TOPIC_SELL) continue;

    let decoded;
    try {
      decoded = ethers.AbiCoder.defaultAbiCoder().decode(
        ["address", "address", "uint256", "uint256", "uint256", "uint256", "uint256", "uint256"],
        evLog.data
      );
    } catch (err) {
      log.warn("[FOURMEME] decode error:", tx.hash, err.message);
      continue;
    }

    const tokenAddress = safeLower(decoded[0]);
    const wallet = safeLower(decoded[1]);

    // pakai TTL cache — cek expired sebelum pakai
    let launchInfo = getLaunchCache(tokenAddress);

    if (!launchInfo) {
      launchInfo = await getLaunchByToken(tokenAddress);
      if (!launchInfo) continue;
      setLaunchCache(tokenAddress, launchInfo);
    }

    const tokenAmount = Number(
      ethers.formatUnits(decoded[3], launchInfo.decimals ?? 18)
    );

    const basePaid = Number(ethers.formatUnits(decoded[4], 18));
    const fee = Number(ethers.formatUnits(decoded[5], 18));
    const baseNet = basePaid - fee;

    const position = topic === TOPIC_BUY ? "BUY" : "SELL";
    // basePaid sudah termasuk fee, jadi untuk BUY pakai basePaid langsung
    const baseAmountPayable = topic === TOPIC_BUY ? basePaid : baseNet;

    if (!tokenAmount || !basePaid) continue;

    // [OPTIMIZED] gunakan data dari DB
    const baseAddress = safeLower(launchInfo.baseAddress);

    if (!baseAddress) {
      log.warn(`[FOURMEME] baseAddress missing for ${tokenAddress}`);
      continue;
    }

    if (!baseAddress) continue;

    const baseSymbol = getBaseSymbol(baseAddress);
    const basePriceUSD = getBasePrice(baseSymbol);

    const priceBase = tokenAmount > 0 ? baseAmountPayable / tokenAmount : 0;
    const priceUSDT = priceBase * basePriceUSD;
    const volumeUSDT = basePaid * basePriceUSD;

    const devAddress = safeLower(
      launchInfo?.devAddress ?? launchInfo?.developer ?? null
    );

    const isDev = devAddress ? wallet === devAddress : false;

    logTrade({
      platform: "fourmeme",
      position,
      tokenAddress,
      tokenAmount,
      baseSymbol,
      baseAmount: baseAmountPayable,
      priceBase,
      priceUSDT,
      volumeUSDT,
      txHash: tx.hash,
      blockNumber,
      timestamp: block.timestamp * 1000,
      wallet,
      isDev
    });

    await insertTransaction({
      tokenAddress,
      time: new Date(block.timestamp * 1000).toISOString(),
      blockNumber,
      txHash: tx.hash,
      position,
      amountReceive: tokenAmount,
      basePayable: baseSymbol,
      amountBasePayable: baseAmountPayable,
      inUSDTPayable: volumeUSDT,
      priceBase,
      priceUSDT,
      addressMessageSender: wallet,
      tagAddress: isDev ? "Developer" : null,
      isDev
    });

  }

}


// [MODIFIED] FULL FIX: fallback + auto recover + no duplicate
async function _handleAddLiquidity({ tx, receipt, block, blockNumber }) {

  let pairCreatedLog = receipt.logs.find(
    l => l.topics?.[0] === TOPICS.PAIR_CREATED
  );

  let pairAddress = null;
  let token0 = null;
  let token1 = null;

  // ===============================================================
  // 🔥 PRIMARY: PAIR_CREATED
  // ===============================================================
  if (pairCreatedLog) {

    token0 = safeLower("0x" + pairCreatedLog.topics[1].slice(26));
    token1 = safeLower("0x" + pairCreatedLog.topics[2].slice(26));

    pairAddress = safeLower(
      "0x" + pairCreatedLog.data.slice(26, 66)
    );

  } else {

    console.warn("[MIGRATE FALLBACK] PAIR_CREATED not found, trying SYNC");

    // ===============================================================
    // 🔥 FALLBACK: SYNC
    // ===============================================================
    const syncFallback = receipt.logs.find(
      l => l.topics?.[0] === TOPICS.SYNC
    );

    if (!syncFallback) {
      console.warn("[MIGRATE FAIL] No PAIR_CREATED & NO SYNC");
      return false;
    }

    pairAddress = safeLower(syncFallback.address);

    try {
      const fields = await getContractFields({
        token0: (provider) => new ethers.Contract(pairAddress, [
          "function token0() view returns(address)",
          "function token1() view returns(address)"
        ], provider).token0(),
        token1: (provider) => new ethers.Contract(pairAddress, [
          "function token0() view returns(address)",
          "function token1() view returns(address)"
        ], provider).token1(),
      });

      token0 = safeLower(fields.token0);
      token1 = safeLower(fields.token1);

    } catch (err) {
      console.error("[MIGRATE FALLBACK ERROR]", err.message);
      return false;
    }
  }

  // ===============================================================
  // 🔥 DETERMINE TOKEN / BASE
  // ===============================================================

  let tokenAddress;
  let baseAddress;

  if (BASE_ADDRESS_MAP[token0]) {
    baseAddress = token0;
    tokenAddress = token1;
  } else if (BASE_ADDRESS_MAP[token1]) {
    baseAddress = token1;
    tokenAddress = token0;
  } else {
    console.warn("[MIGRATE FAIL] Unknown base token");
    return false;
  }

  // ===============================================================
  // 🔥 AUTO RECOVER LAUNCH
  // ===============================================================

  let launchInfo = await getLaunchByToken(tokenAddress);

  if (!launchInfo) {
    console.warn("[MIGRATE SKIP] token not in DB:", tokenAddress);
    return false;
  }

  const baseSymbol = launchInfo.basePair;

  // ===============================================================
  // 🔥 GET SYNC DATA (REAL LIQUIDITY)
  // ===============================================================

  const syncLog = receipt.logs.find(
    l =>
      l.topics?.[0] === TOPICS.SYNC &&
      safeLower(l.address) === pairAddress
  );

  if (!syncLog) {
    console.warn("[MIGRATE FAIL] SYNC not found for pair");
    return false;
  }

  const syncData = syncLog.data.slice(2);

  const reserve0 = BigInt("0x" + syncData.slice(0, 64));
  const reserve1 = BigInt("0x" + syncData.slice(64, 128));

  let tokenAmount;
  let baseAmount;

  if (token0 === tokenAddress) {
    tokenAmount = Number(
      ethers.formatUnits(reserve0, launchInfo.decimals ?? 18)
    );

    baseAmount = Number(
      ethers.formatUnits(reserve1, 18)
    );
  } else {
    tokenAmount = Number(
      ethers.formatUnits(reserve1, launchInfo.decimals ?? 18)
    );

    baseAmount = Number(
      ethers.formatUnits(reserve0, 18)
    );
  }

  if (!tokenAmount || !baseAmount) {
    console.warn("[MIGRATE FAIL] zero liquidity");
    return false;
  }

  // ===============================================================
  // 🔥 PRICE CALC
  // ===============================================================

  const priceBase = baseAmount / tokenAmount;

  let basePriceUSD = getBasePrice(baseSymbol);

  if (!basePriceUSD) {
    for (let i = 0; i < 3 && !basePriceUSD; i++) {
      await new Promise(r => setTimeout(r, 500));
      basePriceUSD = getBasePrice(baseSymbol);
    }
  }

  const priceUSDT = priceBase * basePriceUSD;
  const volumeUSDT = baseAmount * basePriceUSD;

  console.log(`[ANTI-MISS MIGRATE] token=${tokenAddress} pair=${pairAddress} tx=${tx.hash}`);

  // ===============================================================
  // 🔥 SAVE EVERYTHING
  // ===============================================================

  logAddLiquidity({
    platform: "fourmeme",
    tokenAddress,
    pairAddress,
    baseSymbol,
    baseAddress,
    tokenAmount,
    baseAmount,
    priceBase,
    priceUSDT,
    volumeUSDT,
    txHash: tx.hash,
    blockNumber,
    timestamp: block.timestamp * 1000,
    sender: tx.from
  });

  await setTokenMigrated(
    tokenAddress,
    new Date(block.timestamp * 1000).toISOString()
  );

  await insertTokenMigrate({
    tokenAddress,
    pairAddress,
    baseAddress,
    baseSymbol,
    blockNumber,
    txHash: tx.hash
  });

  addPairToMemory({
    pairAddress,
    tokenAddress,
    baseAddress,
    baseSymbol
  });

  await insertTransaction({
    tokenAddress,
    time: new Date(block.timestamp * 1000).toISOString(),
    blockNumber,
    txHash: tx.hash,
    position: "ADD_LIQUIDITY",
    amountReceive: tokenAmount,
    basePayable: baseSymbol,
    amountBasePayable: baseAmount,
    inUSDTPayable: volumeUSDT,
    priceBase,
    priceUSDT,
    addressMessageSender: tx.from,
    tagAddress: "Four.meme",
    isDev: false
  });

  await insertPairLiquidity({
    tokenAddress,
    baseAddress,
    pairAddress,
    baseSymbol,
    liquidityToken: tokenAmount,
    liquidityBase: baseAmount,
    blockNumber,
    txHash: tx.hash
  });

  await updateMigrationStats(volumeUSDT);

  await updateLiquidityState({
    tokenAddress,
    platform: "dex",
    mode: "dex",
    baseAddress,
    baseSymbol,
    baseLiquidity: baseAmount,
    tokenLiquidity: tokenAmount,
    priceBase,
    isMigrated: true,
    pairAddress
  });

  publish("migrate", {
    tokenAddress,
    pairAddress,
    baseSymbol,
    priceUSDT,
    timestamp: block.timestamp * 1000
  });

  return true;
}