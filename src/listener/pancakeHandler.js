// ===============================================================
// pancakeHandler.js
// ===============================================================

import { ethers } from "ethers";
import { getLogs, getTransaction, getTransactionReceipt, getContractFields, getBlock } from "../infra/rpcQueue.js";
import { TOPICS } from "../infra/topics.js";

import { loadTokenMigrateWithPair } from "../repository/tokenMigrate.repository.js";
import { insertTransaction } from "../repository/transaction.repository.js";

import { getBasePrice } from "../price/binancePrice.js";
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

// ===============================================================
// STATE (IN-MEMORY)
// ===============================================================

let pairMap       = new Map();
let pairAddresses = [];

// ===============================================================
// RECEIPT HELPER
// rpcQueue.js sudah punya LRU cache untuk getTransactionReceipt.
// ===============================================================

async function _getReceipt(txHash) {
  return getTransactionReceipt(txHash);
}

// ===============================================================
// RESOLVE REAL WALLET
// FIX: BUY  — prioritas tx.from di antara semua recipient dari pair,
//             fallback recipient dengan amount token terbesar
// FIX: SELL — trace balik dari pair, skip router/intermediary
//             dengan cek apakah `from` juga menerima dari address lain
// ===============================================================

async function _resolveWallet({ txHash, tokenAddress, pairAddress, position, txFrom }) {

  try {

    const receipt = await _getReceipt(txHash);
    if (!receipt) return null;

    const transfers = receipt.logs.filter(l =>
      l.address.toLowerCase() === tokenAddress &&
      l.topics[0] === TOPICS.ERC20_TRANSFER &&
      l.topics.length >= 3
    );

    if (!transfers.length) return null;

    const pair   = pairAddress.toLowerCase();
    const origin = txFrom?.toLowerCase() ?? null;

    // ── SELL ──────────────────────────────────────────────────────
    // Token masuk ke pair. Flow via router: Wallet → Router → Pair
    // Cari Transfer yang `to === pair`, lalu trace balik:
    //   - kalau `from` tersebut juga menerima token dari address lain
    //     berarti itu router/intermediary → ambil pengirim sebelumnya
    //   - kalau tidak ada pengirim sebelumnya → `from` = wallet asli
    // Priority: tx.from kalau cocok dengan chain, fallback trace-back
    if (position === "SELL") {

      for (const t of transfers) {
        const from = "0x" + t.topics[1].slice(26).toLowerCase();
        const to   = "0x" + t.topics[2].slice(26).toLowerCase();

        if (to !== pair) continue;

        // Cek apakah `from` ini juga menerima token dari address lain
        // Kalau iya → itu router, ambil pengirim ke router
        const prev = transfers.find(p => {
          const prevTo = "0x" + p.topics[2].slice(26).toLowerCase();
          return prevTo === from;
        });

        if (!prev) {
          // Tidak ada Transfer masuk ke `from` dalam token ini
          // Kemungkinan aggregator multi-hop pakai token lain dulu
          // tx.from selalu wallet asli — pakai itu sebagai fallback
          return origin || from;
        }

        const realWallet = "0x" + prev.topics[1].slice(26).toLowerCase();

        // Validasi: kalau tx.from cocok dengan hasil trace, pakai itu
        if (origin && realWallet === origin) return realWallet;

        return realWallet;
      }

    }

    // ── BUY ───────────────────────────────────────────────────────
    // Token keluar dari pair ke beberapa address (aggregator bisa split).
    // Priority: tx.from — dia yang sign TX = wallet asli
    // Fallback: recipient dengan amount token terbesar dari pair
    if (position === "BUY") {

      // Kumpulkan semua Transfer keluar dari pair beserta amount-nya
      const recipients = [];

      for (const t of transfers) {
        const from = "0x" + t.topics[1].slice(26).toLowerCase();
        const to   = "0x" + t.topics[2].slice(26).toLowerCase();

        if (from !== pair) continue;

        // Decode amount dari data (uint256 = 32 bytes = 64 hex chars)
        let amount = 0n;
        try {
          amount = BigInt("0x" + t.data.slice(2, 66));
        } catch {}

        recipients.push({ address: to, amount });
      }

      if (!recipients.length) return null;

      // Filter out contract addresses yang bukan wallet asli:
      // - token contract sendiri (bisa jadi liquidity/bonding curve)
      // - pair address
      const filtered = recipients.filter(r =>
        r.address !== tokenAddress &&
        r.address !== pair
      );

      const pool = filtered.length ? filtered : recipients;

      // Priority 1: tx.from ada di antara recipients
      if (origin) {
        const match = pool.find(r => r.address === origin);
        if (match) return match.address;
      }

      // Priority 2: recipient dengan amount terbesar
      pool.sort((a, b) => (b.amount > a.amount ? 1 : -1));
      return pool[0].address;

    }

  } catch (err) {
    log.warn("[PANCAKE] resolveWallet error:", err.message);
  }

  return null;

}

// ===============================================================
// ADD PAIR (EVENT-DRIVEN)
// ===============================================================

export function addPairToMemory({ pairAddress, tokenAddress, baseAddress, baseSymbol }) {
  try {
    if (!pairAddress || !tokenAddress) return;

    const pair = pairAddress.toLowerCase();
    if (pairMap.has(pair)) return;

    pairMap.set(pair, {
      tokenAddress: tokenAddress.toLowerCase(),
      baseAddress:  baseAddress?.toLowerCase() || null,
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
        baseAddress:  r.baseAddress?.toLowerCase() || null,
        baseSymbol:   r.baseSymbol,
        token0: null,
        token1: null
      });

      pairAddresses.push(pair);
    }

    log.info(`[PANCAKE] pairs loaded: ${pairAddresses.length}`);

  } catch (err) {
    log.error("[PANCAKE INIT ERROR]", err.message);
    pairMap       = new Map();
    pairAddresses = [];
  }
}

// ===============================================================
// BLOCK HANDLER
// FIX: gabung semua getLogs jadi 2 call saja (SWAP + SYNC)
//      bukan loop per chunk — drastis kurangi queue pressure.
// FIX: hapus fromBlock - 2, cukup block saat ini saja
//      supaya tidak ada double-processing antar block.
// ===============================================================

async function handleTokenMigratedBlock({ blockNumber }) {

  if (!pairAddresses.length) return;

  try {

    // 1 call untuk semua SWAP dari semua pair sekaligus
    let swapLogs = [];
    try {
      swapLogs = await getLogs({
        address:   pairAddresses,
        topics:    [TOPICS.SWAP],
        fromBlock: blockNumber,
        toBlock:   blockNumber
      });
    } catch (err) {
      log.warn(`[PANCAKE] SWAP fetch failed: ${err.message}`);
      return;
    }

    if (!swapLogs.length) return;

    // 1 call untuk semua SYNC dari semua pair sekaligus
    let syncLogs = [];
    try {
      syncLogs = await getLogs({
        address:   pairAddresses,
        topics:    [TOPICS.SYNC],
        fromBlock: blockNumber,
        toBlock:   blockNumber
      });
    } catch (err) {
      log.warn(`[PANCAKE] SYNC fetch failed: ${err.message}`);
      // sync gagal tidak fatal, lanjut tanpa syncMap
    }

    const block = await getBlock(blockNumber);
    if (!block) return;

    // Build syncMap dari SYNC logs
    const syncMap = new Map();
    for (const logEntry of syncLogs) {
      const pair = logEntry.address?.toLowerCase();
      if (!pair) continue;
      const syncData = logEntry.data.slice(2);
      syncMap.set(pair, {
        reserve0: BigInt("0x" + syncData.slice(0, 64)),
        reserve1: BigInt("0x" + syncData.slice(64, 128))
      });
    }

    // Group SWAP logs per txHash, dedupe per logIndex
    const seenLogs = new Set();
    const txMap    = new Map();

    for (const logEntry of swapLogs) {
      if (!logEntry?.data || logEntry.data.length < 258) continue;

      const id = logEntry.transactionHash + "-" + logEntry.logIndex;
      if (seenLogs.has(id)) continue;
      seenLogs.add(id);

      if (!txMap.has(logEntry.transactionHash)) {
        txMap.set(logEntry.transactionHash, { logs: [], syncMap });
      }
      txMap.get(logEntry.transactionHash).logs.push(logEntry);
    }

    if (!txMap.size) return;

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

const _seenTxGlobal = new Set();
const SEEN_MAX      = 10_000;

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

    let position       = null;
    let tokenAmountRaw;
    let baseAmountRaw;

    if (tokenIs0) {
      if (amount0In > 0n && amount1Out > 0n) { position = "SELL"; tokenAmountRaw = amount0In;  baseAmountRaw = amount1Out; }
      if (amount1In > 0n && amount0Out > 0n) { position = "BUY";  tokenAmountRaw = amount0Out; baseAmountRaw = amount1In;  }
    } else {
      if (amount1In > 0n && amount0Out > 0n) { position = "SELL"; tokenAmountRaw = amount1In;  baseAmountRaw = amount0Out; }
      if (amount0In > 0n && amount1Out > 0n) { position = "BUY";  tokenAmountRaw = amount1Out; baseAmountRaw = amount0In;  }
    }

    if (!position) continue;

    const dedupeKey = `${tx.hash}-${pairAddress}`;
    if (_seenTxGlobal.has(dedupeKey)) continue;
    _seenTxGlobal.add(dedupeKey);
    if (_seenTxGlobal.size > SEEN_MAX) {
      _seenTxGlobal.delete(_seenTxGlobal.values().next().value);
    }

    const tokenAmount = Number(ethers.formatUnits(tokenAmountRaw, 18));
    const baseAmount  = Number(ethers.formatUnits(baseAmountRaw,  18));

    if (!tokenAmount || !baseAmount) continue;

    let basePrice = 0;
    try { basePrice = await getBasePrice(baseSymbol); } catch {}

    const priceBase  = baseAmount / tokenAmount;
    const priceUSDT  = priceBase * basePrice;
    const volumeUSDT = baseAmount * basePrice;

    let wallet = tx.from?.toLowerCase();

    try {
      const resolved = await _resolveWallet({ txHash: tx.hash, tokenAddress, pairAddress, position, txFrom: tx.from });
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
      amountReceive:        tokenAmount,
      basePayable:          baseSymbol,
      amountBasePayable:    baseAmount,
      inUSDTPayable:        volumeUSDT,
      priceBase,
      priceUSDT,
      tagAddress:           null,
      addressMessageSender: wallet
    });

    let baseLiquidity = 0;
    const syncData = syncMap.get(pairAddress);

    if (syncData) {
      const tokenIs0ForReserve = tokenAddress === token0;
      const baseReserveRaw     = tokenIs0ForReserve ? syncData.reserve1 : syncData.reserve0;
      baseLiquidity = Number(ethers.formatUnits(baseReserveRaw, 18));
    } else {
      const prevState = getLiquidityStateCache(tokenAddress) || {};
      baseLiquidity   = Number(prevState.base_liquidity || 0);
      if (position === "BUY")  baseLiquidity += baseAmount;
      if (position === "SELL") baseLiquidity -= baseAmount;
      if (baseLiquidity < 0)   baseLiquidity = 0;
    }

    await updateLiquidityState({
      tokenAddress,
      platform:     "dex",
      mode:         "dex",
      baseAddress:  pairInfo.baseAddress,
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
    const fields = await getContractFields({
      token0: (provider) => new ethers.Contract(pairAddress, PAIR_ABI, provider).token0(),
      token1: (provider) => new ethers.Contract(pairAddress, PAIR_ABI, provider).token1(),
    });

    if (fields.token0) pairInfo.token0 = fields.token0.toLowerCase();
    if (fields.token1) pairInfo.token1 = fields.token1.toLowerCase();

  } catch (err) {
    log.warn("[PANCAKE] ensurePairTokens error:", err.message);
  }
}

// ===============================================================
// EXPORT
// ===============================================================

handleTokenMigratedBlock.init = init;

export { handleTokenMigratedBlock };