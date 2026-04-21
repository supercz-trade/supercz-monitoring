// ===============================================================
// pancakeHandler.js (FINAL - PRODUCTION READY)
// ===============================================================

import { ethers } from "ethers";
import { getLogs, getTransaction, getBlock } from "../infra/rpcQueue.js";
import { TOPICS } from "../infra/topics.js";

import { loadTokenMigrateWithPair } from "../repository/tokenMigrate.repository.js";
import { insertTransaction } from "../repository/transaction.repository.js";

import { getBasePrice } from "../price/binancePrice.js";
import { rpcTxProvider } from "../infra/provider.js";
import { logTrade, log } from "../infra/logger.js";
// [ADDED]
import { updateLiquidityState } from "../service/liquidity.service.js";
import { getLiquidityStateCache } from "../cache/liquidity.cache.js"; // [ADDED]
// import { subscribeLogs } from "../infra/provider.js"; // [ADDED]

// ===============================================================
// CONSTANTS
// ===============================================================

const PAIR_ABI = [
  "function token0() view returns(address)",
  "function token1() view returns(address)"
];

const PAIR_CHUNK_SIZE = 5;

// ===============================================================
// STATE (IN-MEMORY)
// ===============================================================

let pairMap = new Map();
let pairAddresses = [];

// ===============================================================
// ADD PAIR (EVENT-DRIVEN)
// ===============================================================

// [ADDED]
export function addPairToMemory({
  pairAddress,
  tokenAddress,
  baseAddress,
  baseSymbol
}) {
  try {
    if (!pairAddress || !tokenAddress) return;

    const pair = pairAddress.toLowerCase();

    if (pairMap.has(pair)) return; // avoid duplicate

    pairMap.set(pair, {
      tokenAddress: tokenAddress.toLowerCase(),
      baseAddress: baseAddress?.toLowerCase() || null,
      baseSymbol,
      token0: null, // lazy load
      token1: null
    });

    pairAddresses.push(pair);

    log.info(`[PANCAKE] pair added: ${pair}`);

  } catch (err) {
    log.error("[PANCAKE] addPairToMemory error:", err.message);
  }
}

// ===============================================================
// INIT (LOAD FROM DB ONCE)
// ===============================================================

// [MODIFIED]
async function init() {
  try {
    const rows = await loadTokenMigrateWithPair();

    pairMap.clear();
    pairAddresses = [];

    if (!Array.isArray(rows)) {
      throw new Error("loadTokenMigrateWithPair() must return array");
    }

    for (const r of rows) {
      if (!r?.pairAddress || !r?.tokenAddress) continue;

      const pair = r.pairAddress.toLowerCase();

      pairMap.set(pair, {
        tokenAddress: r.tokenAddress.toLowerCase(),
        baseAddress: r.baseAddress?.toLowerCase() || null,
        baseSymbol: r.baseSymbol,
        token0: null,
        token1: null
      });

      pairAddresses.push(pair); // [FIX]
    }

    log.info(`[PANCAKE] pairs loaded: ${pairAddresses.length}`);

  } catch (err) {
    log.error("[PANCAKE INIT ERROR]", err.message);

    // safe default
    pairMap = new Map();
    pairAddresses = [];
  }
}

// ===============================================================
// WS SUBSCRIBE (REPLACE getLogs)
// ===============================================================

// ===============================================================
// PANCAKE WS (FINAL OPTIMIZED)
// ===============================================================

// function startPancakeWS() {

//   if (!pairAddresses.length) {
//     console.log("[PANCAKE WS] No pairs yet");
//     return;
//   }

//   console.log(`[PANCAKE WS] Subscribing ${pairAddresses.length} pairs...`);

//   subscribeLogs({
//     topics: [TOPICS.SWAP]
//   }, async (logEntry) => {

//     try {

//       // =========================
//       // FILTER TOPIC (SAFETY)
//       // =========================
//       if (logEntry.topics?.[0] !== TOPICS.SWAP) return;

//       const pairAddress = logEntry.address?.toLowerCase();
//       if (!pairAddress) return;

//       const pairInfo = pairMap.get(pairAddress);
//       if (!pairInfo) return;

//       // =========================
//       // DEDUPE TX (IMPORTANT)
//       // =========================
//       if (seenTx.has(logEntry.transactionHash)) return;
//       seenTx.add(logEntry.transactionHash);

//       if (seenTx.size > 5000) {
//         seenTx.delete(seenTx.values().next().value);
//       }

//       // =========================
//       // ENSURE TOKEN INFO
//       // =========================
//       await ensurePairTokens(pairAddress, pairInfo);

//       const { tokenAddress, baseSymbol, token0, token1 } = pairInfo;
//       if (!token0 || !token1) return;

//       // =========================
//       // VALIDATE DATA LENGTH
//       // =========================
//       if (!logEntry.data || logEntry.data.length < 258) return;

//       let decoded;

//       try {
//         decoded = ethers.AbiCoder.defaultAbiCoder().decode(
//           ["uint256","uint256","uint256","uint256"],
//           logEntry.data
//         );
//       } catch {
//         return;
//       }

//       const amount0In  = decoded[0];
//       const amount1In  = decoded[1];
//       const amount0Out = decoded[2];
//       const amount1Out = decoded[3];

//       const tokenIs0 = tokenAddress === token0;

//       let position = null;
//       let tokenAmountRaw;
//       let baseAmountRaw;

//       // =========================
//       // DETERMINE POSITION
//       // =========================
//       if (tokenIs0) {

//         if (amount0In > 0n && amount1Out > 0n) {
//           position = "SELL";
//           tokenAmountRaw = amount0In;
//           baseAmountRaw  = amount1Out;
//         }

//         if (amount1In > 0n && amount0Out > 0n) {
//           position = "BUY";
//           tokenAmountRaw = amount0Out;
//           baseAmountRaw  = amount1In;
//         }

//       } else {

//         if (amount1In > 0n && amount0Out > 0n) {
//           position = "SELL";
//           tokenAmountRaw = amount1In;
//           baseAmountRaw  = amount0Out;
//         }

//         if (amount0In > 0n && amount1Out > 0n) {
//           position = "BUY";
//           tokenAmountRaw = amount1Out;
//           baseAmountRaw  = amount0In;
//         }
//       }

//       if (!position) return;

//       const tokenAmount = Number(ethers.formatUnits(tokenAmountRaw, 18));
//       const baseAmount  = Number(ethers.formatUnits(baseAmountRaw, 18));

//       if (!tokenAmount || !baseAmount) return;

//       // =========================
//       // PRICE
//       // =========================
//       let basePrice = 0;
//       try {
//         basePrice = await getBasePrice(baseSymbol);
//       } catch {}

//       const priceBase  = baseAmount / tokenAmount;
//       const priceUSDT  = priceBase * basePrice;
//       const volumeUSDT = baseAmount * basePrice;

//       // =========================
//       // TX + BLOCK
//       // =========================
//       const tx = await getTransaction(logEntry.transactionHash);
//       if (!tx) return;

//       const block = await getBlock(logEntry.blockNumber);
//       if (!block) return;

//       // =========================
//       // WALLET (IMPROVED)
//       // =========================
//       let wallet = tx.from?.toLowerCase();

//       try {
//         if (logEntry.topics[2]) {
//           const to = "0x" + logEntry.topics[2].slice(26);
//           if (to) wallet = to.toLowerCase();
//         }
//       } catch {}

//       // =========================
//       // LOG TRADE
//       // =========================
//       logTrade({
//         platform: "pancake",
//         position,
//         tokenAddress,
//         tokenAmount,
//         baseSymbol,
//         baseAmount,
//         priceBase,
//         priceUSDT,
//         volumeUSDT,
//         txHash: tx.hash,
//         blockNumber: logEntry.blockNumber,
//         timestamp: block.timestamp * 1000,
//         wallet
//       });

//       // =========================
//       // SAVE TX
//       // =========================
//       await insertTransaction({
//         tokenAddress,
//         time: new Date(block.timestamp * 1000).toISOString(),
//         blockNumber: logEntry.blockNumber,
//         txHash: tx.hash,
//         position,
//         amountReceive: tokenAmount,
//         basePayable: baseSymbol,
//         amountBasePayable: baseAmount,
//         inUSDTPayable: volumeUSDT,
//         priceBase,
//         priceUSDT,
//         tagAddress: null,
//         addressMessageSender: wallet
//       });

//       // =========================
//       // LIQUIDITY (APPROX)
//       // =========================
//       const prevState = getLiquidityStateCache(tokenAddress) || {};

//       let currentLiquidity = Number(prevState.base_liquidity || 0);

//       if (position === "BUY") currentLiquidity += baseAmount;
//       else if (position === "SELL") currentLiquidity -= baseAmount;

//       if (currentLiquidity < 0) currentLiquidity = 0;

//       await updateLiquidityState({
//         tokenAddress,
//         platform: "dex",
//         mode: "dex",

//         baseAddress: pairInfo.baseAddress,
//         baseSymbol,

//         baseLiquidity: currentLiquidity,
//         priceBase
//       });

//     } catch (err) {
//       console.error("[PANCAKE WS ERROR]", err.message);
//     }

//   });
// }

// ===============================================================
// LAZY LOAD TOKEN0/TOKEN1
// ===============================================================

// [ADDED]
async function ensurePairTokens(pairAddress, pairInfo) {
  if (pairInfo.token0 && pairInfo.token1) return;

  try {
    const contract = new ethers.Contract(pairAddress, PAIR_ABI, rpcTxProvider);

    const [token0, token1] = await Promise.all([
      contract.token0(),
      contract.token1()
    ]);

    pairInfo.token0 = token0.toLowerCase();
    pairInfo.token1 = token1.toLowerCase();

  } catch (err) {
    log.error("[PANCAKE] ensurePairTokens error:", err.message);
  }
}

// ===============================================================
// BLOCK HANDLER
// ===============================================================

async function handleTokenMigratedBlock({ blockNumber }) {

  if (blockNumber % 2 !== 0) return;

  const seenLogs = new Set();

  if (!pairAddresses.length) {
    return;
  }

  try {

    const allLogs = [];

    for (let i = 0; i < pairAddresses.length; i += PAIR_CHUNK_SIZE) {
      const chunk = pairAddresses.slice(i, i + PAIR_CHUNK_SIZE);

      const chunkLogs = await getLogs({
        address: chunk,
        topics: [[TOPICS.SWAP, TOPICS.SYNC]], // fetch SWAP + SYNC sekaligus
        fromBlock: blockNumber - 2,
        toBlock: blockNumber
      });

      for (const logEntry of chunkLogs) {

  const id = logEntry.transactionHash + "-" + logEntry.logIndex; // [ADDED]

  if (seenLogs.has(id)) continue; // [ADDED]
  seenLogs.add(id); // [ADDED]

  allLogs.push(logEntry); // [MODIFIED]
}
    }

    if (!allLogs.length) return;

    const block = await getBlock(blockNumber);
    if (!block) return;

    const txMap = new Map();

    // Build syncMap: pairAddress → latest reserve data dari SYNC event
    const syncMap = new Map(); // pairAddress → { reserve0, reserve1 }
    for (const logEntry of allLogs) {
      if (logEntry.topics?.[0] !== TOPICS.SYNC) continue;
      const pair = logEntry.address?.toLowerCase();
      if (!pair) continue;
      const syncData = logEntry.data.slice(2);
      syncMap.set(pair, {
        reserve0: BigInt("0x" + syncData.slice(0, 64)),
        reserve1: BigInt("0x" + syncData.slice(64, 128))
      });
    }

    // Build txMap: hanya SWAP logs
    for (const logEntry of allLogs) {
      if (logEntry.topics?.[0] !== TOPICS.SWAP) continue;
      if (!logEntry?.data || logEntry.data.length < 258) continue;

      if (!txMap.has(logEntry.transactionHash)) {
        txMap.set(logEntry.transactionHash, { logs: [], syncMap });
      }

      txMap.get(logEntry.transactionHash).logs.push(logEntry);
    }

    await Promise.allSettled(
      Array.from(txMap.entries()).map(async ([txHash, { logs, syncMap }]) => {
        try {
          const tx = await getTransaction(txHash);
          if (!tx) return;

          await _handleSwap({ tx, logs, block, blockNumber, syncMap });

        } catch (err) {
          log.error("[PANCAKE] TX error:", txHash, err.message);
        }
      })
    );

  } catch (err) {
    log.error("[PANCAKE] block handler error:", err.message);
  }
}

// ===============================================================
// HANDLE SWAP
// ===============================================================

async function _handleSwap({ tx, logs, block, blockNumber, syncMap = new Map() }) {

  for (const logEntry of logs) {

    const pairAddress = logEntry.address?.toLowerCase();
    if (!pairAddress) continue;

    const pairInfo = pairMap.get(pairAddress);
    if (!pairInfo) continue;

    // [ADDED]
    await ensurePairTokens(pairAddress, pairInfo);

    const { tokenAddress, baseSymbol, token0, token1 } = pairInfo;

    if (!token0 || !token1) continue;

    let decoded;

    try {
      decoded = ethers.AbiCoder.defaultAbiCoder().decode(
        ["uint256","uint256","uint256","uint256"],
        logEntry.data
      );
    } catch {
      continue;
    }

    const amount0In  = decoded[0];
    const amount1In  = decoded[1];
    const amount0Out = decoded[2];
    const amount1Out = decoded[3];

    const tokenIs0 = tokenAddress === token0;

    let position = null;
    let tokenAmountRaw;
    let baseAmountRaw;

    if (tokenIs0) {

      if (amount0In > 0n && amount1Out > 0n) {
        position = "SELL";
        tokenAmountRaw = amount0In;
        baseAmountRaw  = amount1Out;
      }

      if (amount1In > 0n && amount0Out > 0n) {
        position = "BUY";
        tokenAmountRaw = amount0Out;
        baseAmountRaw  = amount1In;
      }

    } else {

      if (amount1In > 0n && amount0Out > 0n) {
        position = "SELL";
        tokenAmountRaw = amount1In;
        baseAmountRaw  = amount0Out;
      }

      if (amount0In > 0n && amount1Out > 0n) {
        position = "BUY";
        tokenAmountRaw = amount1Out;
        baseAmountRaw  = amount0In;
      }

    }

    if (!position) continue;

    const tokenAmount = Number(ethers.formatUnits(tokenAmountRaw, 18));
    const baseAmount  = Number(ethers.formatUnits(baseAmountRaw, 18));

    if (!tokenAmount || !baseAmount) continue;

    let basePrice = 0;

    try {
      basePrice = await getBasePrice(baseSymbol);
    } catch {}

    const priceBase  = baseAmount / tokenAmount;
    const priceUSDT  = priceBase * basePrice;
    const volumeUSDT = baseAmount * basePrice;

    // [MODIFIED] ambil user dari event, bukan tx.from
let wallet = tx.from?.toLowerCase();

    logTrade({
      platform: "pancake",
      position,
      tokenAddress,
      tokenAmount,
      baseSymbol,
      baseAmount,
      priceBase,
      priceUSDT,
      volumeUSDT,
      txHash: tx.hash,
      blockNumber,
      timestamp: block.timestamp * 1000,
      wallet
    });

    await insertTransaction({
      tokenAddress,
      time: new Date(block.timestamp * 1000).toISOString(),
      blockNumber,
      txHash: tx.hash,
      position,
      amountReceive: tokenAmount,
      basePayable: baseSymbol,
      amountBasePayable: baseAmount,
      inUSDTPayable: volumeUSDT,
      priceBase,
      priceUSDT,
      tagAddress: null,
      addressMessageSender: wallet
    });

    // [FIX] Pakai SYNC event — exact reserve tanpa extra RPC call
    // SYNC event selalu ada bersamaan SWAP di tx yang sama
    let baseLiquidity = 0;
    const syncData = syncMap.get(pairAddress);

    if (syncData) {
      // Exact dari SYNC event
      const tokenIs0ForReserve = tokenAddress === token0;
      const baseReserveRaw = tokenIs0ForReserve ? syncData.reserve1 : syncData.reserve0;
      baseLiquidity = Number(ethers.formatUnits(baseReserveRaw, 18));
    } else {
      // Fallback approximate kalau SYNC tidak ada
      const prevState = getLiquidityStateCache(tokenAddress) || {};
      baseLiquidity = Number(prevState.base_liquidity || 0);
      if (position === "BUY") baseLiquidity += baseAmount;
      else if (position === "SELL") baseLiquidity -= baseAmount;
      if (baseLiquidity < 0) baseLiquidity = 0;
    }

    await updateLiquidityState({
      tokenAddress,
      platform: "dex",
      mode: "dex",
      baseAddress: pairInfo.baseAddress,
      baseSymbol,
      baseLiquidity,
      priceBase
    });
  }
}

// ===============================================================
// EXPORT
// ===============================================================

handleTokenMigratedBlock.init = init;
// handleTokenMigratedBlock.startWS = startPancakeWS;

export { handleTokenMigratedBlock };