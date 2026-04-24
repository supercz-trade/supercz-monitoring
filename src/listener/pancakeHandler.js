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
import { updateLiquidityState } from "../service/liquidity.service.js";
import { getLiquidityStateCache } from "../cache/liquidity.cache.js";

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
// RECEIPT CACHE — 1 fetch per txHash
// ===============================================================

const _receiptCache = new Map();
const RECEIPT_CACHE_MAX = 2000;

async function _getReceipt(txHash) {
  if (_receiptCache.has(txHash)) return _receiptCache.get(txHash);

  const receipt = await rpcTxProvider.getTransactionReceipt(txHash);

  if (receipt) {
    _receiptCache.set(txHash, receipt);
    if (_receiptCache.size > RECEIPT_CACHE_MAX) {
      _receiptCache.delete(_receiptCache.keys().next().value);
    }
  }

  return receipt;
}

// ===============================================================
// RESOLVE REAL WALLET
// ===============================================================

async function _resolveWallet({ txHash, tokenAddress, pairAddress, position }) {

  try {

    const receipt = await _getReceipt(txHash);
    if (!receipt) return null;

    // ambil semua Transfer event dari token ini di tx ini
    const transfers = receipt.logs.filter(l =>
      l.address.toLowerCase() === tokenAddress &&
      l.topics[0] === TOPICS.ERC20_TRANSFER &&
      l.topics.length >= 3
    );

    if (!transfers.length) return null;

    const pair = pairAddress.toLowerCase();

    if (position === "SELL") {

      // cari Transfer: FROM = user → TO = pair
      // user yang kirim token ke pair = user asli
      for (const t of transfers) {
        const from = "0x" + t.topics[1].slice(26).toLowerCase();
        const to   = "0x" + t.topics[2].slice(26).toLowerCase();
        if (to === pair) return from;
      }

    } else if (position === "BUY") {

      // cari Transfer: FROM = pair → TO = ?
      // siapa yang terima token dari pair
      let recipient = null;
      for (const t of transfers) {
        const from = "0x" + t.topics[1].slice(26).toLowerCase();
        const to   = "0x" + t.topics[2].slice(26).toLowerCase();
        if (from === pair) {
          recipient = to;
          break;
        }
      }

      if (!recipient) return null;

      // follow Transfer chain sampai tidak ada lagi
      // recipient mungkin aggregator yang forward token ke user asli
      const visited = new Set();

      while (recipient && !visited.has(recipient)) {
        visited.add(recipient);

        // cari Transfer berikutnya: FROM = recipient saat ini
        const next = transfers.find(t => {
          const from = "0x" + t.topics[1].slice(26).toLowerCase();
          return from === recipient && !visited.has("0x" + t.topics[2].slice(26).toLowerCase());
        });

        if (!next) break;

        recipient = "0x" + next.topics[2].slice(26).toLowerCase();
      }

      return recipient;

    }

  } catch (err) {
    log.warn("[PANCAKE] resolveWallet error:", err.message);
  }

  return null;

}

// ===============================================================
// ADD PAIR (EVENT-DRIVEN)
// ===============================================================

export function addPairToMemory({
  pairAddress,
  tokenAddress,
  baseAddress,
  baseSymbol
}) {
  try {
    if (!pairAddress || !tokenAddress) return;

    const pair = pairAddress.toLowerCase();

    if (pairMap.has(pair)) return;

    pairMap.set(pair, {
      tokenAddress: tokenAddress.toLowerCase(),
      baseAddress: baseAddress?.toLowerCase() || null,
      baseSymbol,
      token0: null,
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

      pairAddresses.push(pair);
    }

    log.info(`[PANCAKE] pairs loaded: ${pairAddresses.length}`);

  } catch (err) {
    log.error("[PANCAKE INIT ERROR]", err.message);
    pairMap = new Map();
    pairAddresses = [];
  }
}

// ===============================================================
// BLOCK HANDLER
// ===============================================================

async function handleTokenMigratedBlock({ blockNumber }) {

  if (blockNumber % 2 !== 0) return;

  const seenLogs = new Set();

  if (!pairAddresses.length) return;

  try {

    const allLogs = [];

    let syncLogsAll = [];
    try {
      syncLogsAll = await getLogs({
        topics: [TOPICS.SYNC],
        fromBlock: blockNumber - 2,
        toBlock: blockNumber
      });
    } catch (err) {
      log.warn(`[PANCAKE] global SYNC fetch failed: ${err.message}`);
    }

    for (let i = 0; i < pairAddresses.length; i += PAIR_CHUNK_SIZE) {
      const chunk = pairAddresses.slice(i, i + PAIR_CHUNK_SIZE);

      const swapChunkLogs = await getLogs({
        address: chunk,
        topics: [TOPICS.SWAP],
        fromBlock: blockNumber - 2,
        toBlock: blockNumber
      });

      for (const logEntry of swapChunkLogs) {
        const id = logEntry.transactionHash + "-" + logEntry.logIndex;
        if (seenLogs.has(id)) continue;
        seenLogs.add(id);
        allLogs.push(logEntry);
      }
    }

    for (const sl of syncLogsAll) {
      if (pairAddresses.includes(sl.address?.toLowerCase())) {
        allLogs.push(sl);
      }
    }

    if (!allLogs.length) return;

    const block = await getBlock(blockNumber);
    if (!block) return;

    const txMap = new Map();

    const syncMap = new Map();
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

// ── dedupe global (persist antar block) ──────────────────────
const _seenTxGlobal = new Set();
const SEEN_MAX = 10_000;

async function _handleSwap({ tx, logs, block, blockNumber, syncMap = new Map() }) {

  for (const logEntry of logs) {

    const pairAddress = logEntry.address?.toLowerCase();
    if (!pairAddress) continue;

    const pairInfo = pairMap.get(pairAddress);
    if (!pairInfo) continue;

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

    // ── dedupe — skip kalau tx + pair sudah pernah diproses ──
    const dedupeKey = `${tx.hash}-${pairAddress}`;
    if (_seenTxGlobal.has(dedupeKey)) continue;
    _seenTxGlobal.add(dedupeKey);
    if (_seenTxGlobal.size > SEEN_MAX) {
      _seenTxGlobal.delete(_seenTxGlobal.values().next().value);
    }

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

    // [MODIFIED] resolve wallet via Transfer event
    let wallet = tx.from?.toLowerCase();

    try {
      const resolved = await _resolveWallet({
        txHash: tx.hash,
        tokenAddress,
        pairAddress,
        position
      });
      if (resolved) wallet = resolved;
    } catch (err) {
      log.warn("[PANCAKE] wallet resolve failed, fallback tx.from:", err.message);
    }

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

    let baseLiquidity = 0;
    const syncData = syncMap.get(pairAddress);

    if (syncData) {
      const tokenIs0ForReserve = tokenAddress === token0;
      const baseReserveRaw = tokenIs0ForReserve ? syncData.reserve1 : syncData.reserve0;
      baseLiquidity = Number(ethers.formatUnits(baseReserveRaw, 18));
    } else {
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
// ENSURE PAIR TOKENS
// ===============================================================

async function ensurePairTokens(pairAddress, pairInfo) {
  if (pairInfo.token0 && pairInfo.token1) return;

  try {
    const contract = new ethers.Contract(pairAddress, PAIR_ABI, rpcTxProvider);
    pairInfo.token0 = (await contract.token0()).toLowerCase();
    pairInfo.token1 = (await contract.token1()).toLowerCase();
  } catch (err) {
    log.warn("[PANCAKE] ensurePairTokens error:", err.message);
  }
}

// ===============================================================
// EXPORT
// ===============================================================

handleTokenMigratedBlock.init = init;

export { handleTokenMigratedBlock };