// ===============================================================
// flapHandler.js
// Handler untuk Flap.sh — Create Token, Buy/Sell, Add Liquidity
// ===============================================================

import { ethers } from "ethers";
import { TOPICS } from "../infra/topics.js";
import { publish } from "../infra/wsbroker.js";
import { getTransaction, getLogs, getBlock, getContractFields, getTransactionReceipt } from "../infra/rpcQueue.js";

import { getLaunchByToken, insertLaunch, setTokenMigrated } from "../repository/launch.repository.js";
import { insertTransaction } from "../repository/transaction.repository.js";
import { insertTokenFlap, deleteTokenFlap, loadTokenFlap } from "../repository/tokenFlap.repository.js";
import { insertTokenMigrate } from "../repository/tokenMigrate.repository.js";
import { insertPairLiquidity } from "../repository/pairLiquidity.repository.js";
import { fetchIPFSMeta } from "../service/ipfsMeta.service.js";
import { getBasePrice } from "../price/binancePrice.js";
import { getBasePair, normalizeBaseAddress } from "../utils/baseToken.js";
import { logTrade, logCreate, logAddLiquidity, log } from "../infra/logger.js";
import { addPairToMemory } from "./pancakeHandler.js";
import { updateLiquidityState } from "../service/liquidity.service.js";
import { updateMigrationStats } from "../service/migrationStats.service.js";

// ================= CONSTANTS =================

const TOTAL_SUPPLY = 1_000_000_000;

const TOKEN_ABI = [
  "function name() view returns (string)",
  "function symbol() view returns (string)",
  "function decimals() view returns (uint8)",
  "function metaURI() view returns (string)",
  "function QUOTE_TOKEN() view returns (address)",
  "function taxRate() view returns (uint256)"
];

// ================= FLAP TOKEN SET =================

export let flapTokenSet = new Set();

export function addFlapTokenToMemory(token) {
  flapTokenSet.add(token.toLowerCase());
}

// ================= INIT =================

async function init() {
  try {
    const tokens = await loadTokenFlap();

    if (!Array.isArray(tokens)) {
      throw new Error("loadTokenFlap() must return array");
    }

    flapTokenSet = new Set(
      tokens
        .filter(t => typeof t === "string" && t.length > 0)
        .map(t => t.toLowerCase())
    );

    console.log("[FLAP] Tokens loaded:", flapTokenSet.size);

  } catch (err) {
    console.error("[FLAP INIT ERROR]", err.message);
    flapTokenSet = new Set();
  }
}

// ===============================================================
// MAIN HANDLER
// ===============================================================

async function handleFlapBlock({ txMap, block, blockNumber }) {

  for (const [txHash, portalLogs] of txMap.entries()) {

    try {

      const tx = await getTransaction(txHash);
      if (!tx) continue;

      const hasCreate = portalLogs.some(l => l.topics[0] === TOPICS.TOKEN_CREATED);
      const allLogs   = hasCreate
        ? ((await getTransactionReceipt(txHash))?.logs ?? portalLogs)
        : portalLogs;

      await _handleBuySell({ tx, logs: allLogs, block, blockNumber, source: "portal" });

    } catch (err) {
      console.error("[FLAP] TX error:", txHash, err.message);
    }

  }

}

// ===============================================================
// SCAN DIRECT
// ===============================================================

async function scanDirect({ block, blockNumber }) {
  if (!flapTokenSet.size) return;

  try {

    const logs = await getLogs({
      topics: [[TOPICS.TOKEN_BOUGHT, TOPICS.TOKEN_SOLD, TOPICS.LAUNCHED_TO_DEX]],
      fromBlock: blockNumber,
      toBlock:   blockNumber
    });

    if (!logs.length) return;

    const txLogMap = new Map();

    for (const evLog of logs) {

      const topic = evLog.topics[0];
      let tokenAddress = null;

      if (topic === TOPICS.TOKEN_BOUGHT || topic === TOPICS.TOKEN_SOLD) {
        const d = ethers.AbiCoder.defaultAbiCoder().decode(
          ["uint256", "address", "address", "uint256", "uint256", "uint256", "uint256"],
          evLog.data
        );
        tokenAddress = d[1].toLowerCase();
      } else if (topic === TOPICS.LAUNCHED_TO_DEX) {
        const d = ethers.AbiCoder.defaultAbiCoder().decode(
          ["address", "address", "uint256", "uint256"],
          evLog.data
        );
        tokenAddress = d[0].toLowerCase();
      }

      if (!tokenAddress || !flapTokenSet.has(tokenAddress)) continue;

      if (!txLogMap.has(evLog.transactionHash)) txLogMap.set(evLog.transactionHash, []);
      txLogMap.get(evLog.transactionHash).push(evLog);

    }

    const txCache = new Map();

    for (const [txHash, txLogs] of txLogMap.entries()) {

      try {

        let tx = txCache.get(txHash);
        if (!tx) {
          tx = await getTransaction(txHash);
          if (tx) txCache.set(txHash, tx);
        }

        if (!tx) continue;

        await _handleBuySell({ tx, logs: txLogs, block, blockNumber, source: "token" });

      } catch (err) {
        console.error("[FLAP] scanDirect tx error:", txHash, err.message);
      }

    }

  } catch (err) {
    console.error("[FLAP] scanDirect error:", err.message);
  }

}

// ===============================================================
// HANDLE TX — PRIORITY: CREATE > MIGRATE > TRADE
// ===============================================================

async function _handleBuySell({ tx, logs, block, blockNumber, source = "portal" }) {

  const creates            = [];
  const addLiquidityEvents = [];
  const trades             = [];

  for (const evLog of logs) {

    const topic = evLog.topics[0];

    try {

      if (topic === TOPICS.TOKEN_CREATED) {
        const d = _decode(["uint256", "address", "uint256", "address", "string", "string", "string"], evLog.data);
        creates.push({
          tokenAddress: d[3].toLowerCase(),
          creator:      d[1].toLowerCase(),
          name: d[4], symbol: d[5], meta: d[6],
          baseAddress: null, basePair: null, tax: 0
        });
      }

      if (topic === TOPICS.TOKEN_QUOTE_SET) {
        const d = _decode(["address", "address"], evLog.data);
        const tokenAddr = d[0].toLowerCase();
        const quoteAddr = normalizeBaseAddress(d[1]);
        const c = creates.find(x => x.tokenAddress === tokenAddr);
        if (c) { c.baseAddress = quoteAddr; c.basePair = getBasePair(quoteAddr); }
      }

      if (topic === TOPICS.TAX_SET) {
        const d = _decode(["address", "uint256"], evLog.data);
        const tokenAddr = d[0].toLowerCase();
        const c = creates.find(x => x.tokenAddress === tokenAddr);
        if (c) c.tax = Number(d[1]) / 100;
      }

      if (topic === TOPICS.LAUNCHED_TO_DEX) {
        const d = _decode(["address", "address", "uint256", "uint256"], evLog.data);
        addLiquidityEvents.push({
          tokenAddress: d[0].toLowerCase(),
          pairAddress:  d[1].toLowerCase(),
          tokenAmount:  d[2],
          baseAmount:   d[3]
        });
      }

      if (topic === TOPICS.TOKEN_BOUGHT) {
        const d = _decode(["uint256", "address", "address", "uint256", "uint256", "uint256", "uint256"], evLog.data);
        trades.push({ token: d[1].toLowerCase(), wallet: d[2].toLowerCase(), amount: d[3], base: d[4], fee: d[5], position: "BUY" });
      }

      if (topic === TOPICS.TOKEN_SOLD) {
        const d = _decode(["uint256", "address", "address", "uint256", "uint256", "uint256", "uint256"], evLog.data);
        trades.push({ token: d[1].toLowerCase(), wallet: d[2].toLowerCase(), amount: d[3], base: d[4], fee: d[5], position: "SELL" });
      }

    } catch (err) {
      console.warn("[FLAP DECODE SKIP]", err.message);
      continue;
    }
  }

  console.log("[FLAP PARSE]", { tx: tx.hash, creates: creates.length, addLiquidity: addLiquidityEvents.length, trades: trades.length, source });

  // PRIORITY 1: TOKEN_CREATED
  for (const create of creates) {
    try {
      await _handleCreate({ create, tx, block, blockNumber, trades });
    } catch (err) {
      console.error("[FLAP CREATE ERROR]", err.message);
    }
  }

  // PRIORITY 2: LAUNCHED_TO_DEX
  for (const addLiquidity of addLiquidityEvents) {

    try {

      let launchInfo = await getLaunchByToken(addLiquidity.tokenAddress);

      if (!launchInfo) {
        console.warn("[MIGRATE AUTO-REGISTER]", addLiquidity.tokenAddress);
        launchInfo = { tokenAddress: addLiquidity.tokenAddress, basePair: "BNB", baseAddress: null, decimals: 18 };
      }

      const basePrice  = getBasePrice(launchInfo.basePair);
      const tokenAmt   = Number(ethers.formatUnits(addLiquidity.tokenAmount, launchInfo.decimals ?? 18));
      const baseAmt    = Number(ethers.formatUnits(addLiquidity.baseAmount, 18));
      const priceBase  = tokenAmt > 0 ? baseAmt / tokenAmt : 0;
      const priceUSDT  = priceBase * basePrice;
      const volumeUSDT = baseAmt * basePrice;

      const { tokenAddress, pairAddress } = addLiquidity;

      console.log(`[ANTI-MISS MIGRATE] token=${tokenAddress} pair=${pairAddress} tx=${tx.hash}`);

      await setTokenMigrated(tokenAddress, new Date(block.timestamp * 1000).toISOString());
      await insertTokenMigrate({ tokenAddress, pairAddress, baseAddress: launchInfo.baseAddress, baseSymbol: launchInfo.basePair, blockNumber, txHash: tx.hash });
      addPairToMemory({ pairAddress, tokenAddress, baseAddress: launchInfo.baseAddress, baseSymbol: launchInfo.basePair });

      await insertTransaction({
        tokenAddress,
        time: new Date(block.timestamp * 1000).toISOString(),
        blockNumber, txHash: tx.hash,
        position: "ADD_LIQUIDITY",
        amountReceive: tokenAmt, basePayable: launchInfo.basePair, amountBasePayable: baseAmt,
        inUSDTPayable: volumeUSDT, priceBase, priceUSDT,
        addressMessageSender: tx.from, tagAddress: null, isDev: false
      });

      await insertPairLiquidity({ tokenAddress, baseAddress: launchInfo.baseAddress, pairAddress, baseSymbol: launchInfo.basePair, liquidityToken: tokenAmt, liquidityBase: baseAmt, blockNumber, txHash: tx.hash });
      await updateMigrationStats(volumeUSDT);
      await updateLiquidityState({ tokenAddress, platform: "dex", mode: "dex", baseAddress: launchInfo.baseAddress, baseSymbol: launchInfo.basePair, baseLiquidity: baseAmt, tokenLiquidity: tokenAmt, priceBase, isMigrated: true, pairAddress });

      await deleteTokenFlap(tokenAddress);
      flapTokenSet.delete(tokenAddress);

      publish("migrate", { tokenAddress, pairAddress, baseSymbol: launchInfo.basePair, priceUSDT, timestamp: block.timestamp * 1000 });

    } catch (err) {
      console.error("[ANTI-MISS MIGRATE ERROR]", err.message);
    }
  }

  // PRIORITY 3: TOKEN_BOUGHT / TOKEN_SOLD
  for (const trade of trades) {

    try {

      let launchInfo = await getLaunchByToken(trade.token);

      if (!launchInfo) {
        launchInfo = await _autoRegister({ tx, trade, block, blockNumber });
        if (!launchInfo) continue;
      }

      const basePrice  = getBasePrice(launchInfo.basePair);
      const tokenAmt   = Number(ethers.formatUnits(trade.amount, launchInfo.decimals ?? 18));
      const basePaid   = Number(ethers.formatUnits(trade.base - trade.fee, 18));
      const baseAmount = Number(ethers.formatUnits(trade.base, 18));
      const priceBase  = tokenAmt > 0 ? baseAmount / tokenAmt : 0;
      const priceUSDT  = priceBase * basePrice;
      const volumeUSDT = baseAmount * basePrice;

      // Resolve wallet asli via TransferFlapToken log di receipt
      // BUY  → `to`   di TransferFlapToken = wallet asli
      // SELL → `from` di TransferFlapToken = wallet asli
      // Fallback: trade.wallet dari event data
      let wallet = trade.wallet;
      try {
        const receipt = await getTransactionReceipt(tx.hash);
        if (receipt) {
          const flapTransfers = receipt.logs.filter(l =>
            l.address.toLowerCase() === trade.token &&
            l.topics[0] === TOPICS.TRANSFER_FLAP
          );

          if (flapTransfers.length) {
            // TransferFlapToken(address from, address to, uint256 value) — semua di data, tidak indexed
            const decoded = ethers.AbiCoder.defaultAbiCoder().decode(
              ["address", "address", "uint256"],
              flapTransfers[0].data
            );
            const flapFrom = decoded[0].toLowerCase();
            const flapTo   = decoded[1].toLowerCase();

            if (trade.position === "BUY")  wallet = flapTo;
            if (trade.position === "SELL") wallet = flapFrom;
          }
        }
      } catch (err) {
        log.warn("[FLAP] resolveWallet via TransferFlapToken failed:", err.message);
      }

      const isDev = wallet === launchInfo.developerAddress;

      logTrade({ platform: "flap", position: trade.position, tokenAddress: trade.token, tokenSymbol: launchInfo.symbol, tokenAmount: tokenAmt, baseSymbol: launchInfo.basePair, baseAmount: basePaid, priceBase, priceUSDT, volumeUSDT, txHash: tx.hash, blockNumber, timestamp: block.timestamp * 1000, wallet, isDev });

      await insertTransaction({
        tokenAddress: trade.token,
        time: new Date(block.timestamp * 1000).toISOString(),
        blockNumber, txHash: tx.hash,
        position: trade.position,
        amountReceive: tokenAmt, basePayable: launchInfo.basePair, amountBasePayable: basePaid,
        inUSDTPayable: volumeUSDT, priceBase, priceUSDT,
        addressMessageSender: wallet,
        tagAddress: isDev ? "Developer" : null, isDev
      });

    } catch (err) {
      console.error("[FLAP TRADE ERROR]", err.message);
    }
  }

}

// ===============================================================
// HANDLE CREATE
// ===============================================================

async function _handleCreate({ create, tx, block, blockNumber, trades = [] }) {

  const { tokenAddress, creator, name, symbol, meta } = create;

  const existing = await getLaunchByToken(tokenAddress);
  if (existing) return;

  let imageUrl = null, description = null, websiteUrl = null, telegramUrl = null, twitterUrl = null;
  try {
    const metaJSON = await fetchIPFSMeta(meta);
    description = metaJSON?.description || null;
    websiteUrl  = metaJSON?.website     || null;
    telegramUrl = metaJSON?.telegram    || null;
    twitterUrl  = metaJSON?.twitter     || null;
    imageUrl    = _parseImageUrl(metaJSON?.image || null);
  } catch { }

  let { baseAddress, basePair, tax } = create;

  // FIX: signature baru — tidak pass contract instance, pass thunk saja
  if (!baseAddress) {
    try {
      const fields = await getContractFields({
        quoteToken: (provider) => {
          const contract = new ethers.Contract(tokenAddress, TOKEN_ABI, provider);
          return contract.QUOTE_TOKEN();
        }
      });
      if (fields.quoteToken) {
        baseAddress = normalizeBaseAddress(fields.quoteToken);
        basePair    = getBasePair(baseAddress);
      }
    } catch (err) {
      log.warn("[FLAP CREATE] QUOTE_TOKEN fallback failed: " + err.message);
    }
  }

  await insertLaunch({ launchTime: new Date(block.timestamp * 1000).toISOString(), tokenAddress, developer: creator, name, symbol, description, imageUrl, websiteUrl, telegramUrl, twitterUrl, supply: TOTAL_SUPPLY, decimals: 18, taxBuy: tax, taxSell: tax, minBuy: 0, maxBuy: 0, basePair, baseAddress, networkCode: "BSC", sourceFrom: "flap.sh", migrated: false, verifiedCode: true });

  await insertTokenFlap({ tokenAddress, creator, blockNumber });
  addFlapTokenToMemory(tokenAddress);

  logCreate({ platform: "flap", tokenAddress, tokenSymbol: symbol, tokenName: name, creator, basePair, baseAddress, txHash: tx.hash, blockNumber, timestamp: block.timestamp * 1000 });

  let price = 0, marketcap = 0, volume24h = 0, txCount = 0;

  const initialBuy = trades.find(t => t.token === tokenAddress && t.position === "BUY");

  if (initialBuy && basePair) {
    const basePrice = getBasePrice(basePair);
    const tokenAmt  = Number(ethers.formatUnits(initialBuy.amount, 18));
    const baseAmt   = Number(ethers.formatUnits(initialBuy.base, 18));
    const priceBase = tokenAmt > 0 ? baseAmt / tokenAmt : 0;
    price     = priceBase * basePrice;
    marketcap = price * TOTAL_SUPPLY;
    volume24h = baseAmt * basePrice;
    txCount   = 1;
  }

  publish("new_token", { tokenAddress, symbol, name, basePair, baseAddress, imageUrl, description, website: websiteUrl, telegram: telegramUrl, twitter: twitterUrl, devAddress: creator, taxBuy: tax ?? 0, taxSell: tax ?? 0, price, marketcap, volume24h, txCount, holderCount: 1, txHash: tx.hash, launchTime: block.timestamp * 1000, source: "flap.sh" });

}

// ===============================================================
// AUTO REGISTER
// FIX: signature baru — tidak pass contract instance
// ===============================================================

async function _autoRegister({ tx, trade, block, blockNumber }) {

  const fields = await getContractFields({
    name:       (provider) => new ethers.Contract(trade.token, TOKEN_ABI, provider).name(),
    symbol:     (provider) => new ethers.Contract(trade.token, TOKEN_ABI, provider).symbol(),
    decimals:   (provider) => new ethers.Contract(trade.token, TOKEN_ABI, provider).decimals(),
    metaURI:    (provider) => new ethers.Contract(trade.token, TOKEN_ABI, provider).metaURI(),
    quoteToken: (provider) => new ethers.Contract(trade.token, TOKEN_ABI, provider).QUOTE_TOKEN(),
    taxRate:    (provider) => new ethers.Contract(trade.token, TOKEN_ABI, provider).taxRate(),
  });

  const baseAddress = normalizeBaseAddress(fields.quoteToken);
  const basePair    = getBasePair(baseAddress);

  if (!basePair) {
    log.warn("[FLAP SKIP] Unknown base: " + fields.quoteToken + " TX: " + tx.hash);
    return null;
  }

  const metaJSON = await fetchIPFSMeta(fields.metaURI);
  const imageUrl = _parseImageUrl(metaJSON?.image);

  console.log(`[FLAP REGISTRY] token registered via TRADE: ${trade.token}`);

  await insertTokenFlap({ tokenAddress: trade.token, creator: tx.from, blockNumber });
  addFlapTokenToMemory(trade.token);

  await insertLaunch({ launchTime: new Date(block.timestamp * 1000).toISOString(), tokenAddress: trade.token, developer: tx.from, name: fields.name, symbol: fields.symbol, description: metaJSON?.description || null, imageUrl, websiteUrl: metaJSON?.website || null, telegramUrl: metaJSON?.telegram || null, twitterUrl: metaJSON?.twitter || null, supply: TOTAL_SUPPLY, decimals: fields.decimals ?? 18, taxBuy: Number(fields.taxRate ?? 0) / 100, taxSell: Number(fields.taxRate ?? 0) / 100, minBuy: 0, maxBuy: 0, basePair, baseAddress, networkCode: "BSC", sourceFrom: "flap.sh", migrated: false, verifiedCode: false });

  flapTokenSet.add(trade.token);

  return getLaunchByToken(trade.token);

}

// ===============================================================
// UTILS
// ===============================================================

function _decode(types, data) {
  return ethers.AbiCoder.defaultAbiCoder().decode(types, data);
}

function _parseImageUrl(image) {
  if (!image) return null;
  if (image.startsWith("ipfs://")) return image.replace("ipfs://", "https://ipfs.io/ipfs/");
  if (!image.startsWith("http")) return `https://ipfs.io/ipfs/${image}`;
  return image;
}

// ===============================================================
// EXPORT
// ===============================================================

handleFlapBlock.init       = init;
handleFlapBlock.scanDirect = scanDirect;

export { handleFlapBlock };