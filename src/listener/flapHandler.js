// ===============================================================
// flapHandler.js
// Handler untuk Flap.sh — Create Token, Buy/Sell, Add Liquidity
// ===============================================================

import { ethers } from "ethers";
import { TOPICS } from "../infra/topics.js";
import { publish } from "../infra/wsbroker.js";                          // ✅ FIX: tambah import publish
import { getTransaction, getLogs, getBlock, getContractFields, getTransactionReceipt } from "../infra/rpcQueue.js";
import { rpcTxProvider } from "../infra/provider.js";

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
// [ADDED]
import { updateLiquidityState } from "../service/liquidity.service.js";
import { updateMigrationStats } from "../service/migrationStats.service.js";
// import { subscribeLogs } from "../infra/provider.js";

// ================= CONSTANTS =================

const TOTAL_SUPPLY = 1_000_000_000;

const CREATE_METHODS = new Set([
  "0x0ba6324e",
  "0x2e2fdbd9",
  "0x64fd8b9e",
  "0x9f0bb8a9",
  "0x6f8e27ec"
]);

const TOKEN_ABI = [
  "function name() view returns (string)",
  "function symbol() view returns (string)",
  "function decimals() view returns (uint8)",
  "function metaURI() view returns (string)",
  "function QUOTE_TOKEN() view returns (address)",
  "function taxRate() view returns (uint256)"
];

// ================= FLAP TOKEN SET (dari DB) =================

let flapTokenSet = new Set();


export function addFlapTokenToMemory(token) {
  flapTokenSet.add(token.toLowerCase());
}

// ================= INIT =================

// [MODIFIED]
async function init() {
  try {
    const tokens = await loadTokenFlap();

    if (!Array.isArray(tokens)) {
      throw new Error("loadTokenFlap() must return array");
    }

    flapTokenSet = new Set(
      tokens
        .filter(t => typeof t === "string" && t.length > 0) // [ADDED] validation
        .map(t => t.toLowerCase())
    );

    console.log("[FLAP] Tokens loaded:", flapTokenSet.size);

  } catch (err) {
    console.error("[FLAP INIT ERROR]", err.message);

    // [SAFE DEFAULT]
    flapTokenSet = new Set();
  }
}

// ===============================================================
// WS SUBSCRIBE (FINAL VERSION)
// ===============================================================

// [ADDED] dedupe tx (hindari double process)
const seenWS = new Set();

// function startFlapWS() {

//   console.log("[FLAP WS] Subscribing to token events...");

//   subscribeLogs({
//     topics: [[
//       TOPICS.TOKEN_BOUGHT,
//       TOPICS.TOKEN_SOLD,
//       TOPICS.LAUNCHED_TO_DEX
//     ]]
//   }, async (log) => {

//     try {

//       // ================= DEDUPE =================
//       if (seenWS.has(log.transactionHash)) return;
//       seenWS.add(log.transactionHash);

//       if (seenWS.size > 5000) {
//         seenWS.delete(seenWS.values().next().value);
//       }

//       // ================= DECODE =================
//       const topic = log.topics[0];
//       let tokenAddress = null;

//       if (topic === TOPICS.TOKEN_BOUGHT || topic === TOPICS.TOKEN_SOLD) {
//         const d = ethers.AbiCoder.defaultAbiCoder().decode(
//           ["uint256", "address", "address", "uint256", "uint256", "uint256", "uint256"],
//           log.data
//         );
//         tokenAddress = d[1].toLowerCase();
//       }

//       if (topic === TOPICS.LAUNCHED_TO_DEX) {
//         const d = ethers.AbiCoder.defaultAbiCoder().decode(
//           ["address", "address", "uint256", "uint256"],
//           log.data
//         );
//         tokenAddress = d[0].toLowerCase();
//       }

//       if (!tokenAddress || !flapTokenSet.has(tokenAddress)) return;

//       // ================= FETCH TX =================
//       const tx = await getTransaction(log.transactionHash);
//       if (!tx) return;

//       // ================= FETCH BLOCK (REAL TIMESTAMP) =================
//       const block = await getBlock(log.blockNumber);
//       if (!block) return;

//       // ================= HANDLE =================
//       await _handleBuySell({
//         tx,
//         logs: [log],
//         block,
//         blockNumber: log.blockNumber,
//         source: "ws"
//       });

//     } catch (err) {
//       console.error("[FLAP WS ERROR]", err.message);
//     }

//   });
// }

// ===============================================================
// MAIN HANDLER
// ===============================================================

async function handleFlapBlock({ txMap, block, blockNumber }) {

  for (const [txHash, portalLogs] of txMap.entries()) {

    try {

      const tx = await getTransaction(txHash);
      if (!tx) continue;

      const method = tx.data?.slice(0, 10);

      // ================= CREATE TOKEN =================
      if (CREATE_METHODS.has(method)) {
        const receipt = await getTransactionReceipt(txHash);
        const allLogs = receipt?.logs ?? portalLogs;
        await _handleCreate({ tx, logs: allLogs, block, blockNumber });
        continue;
      }

      // ================= BUY / SELL / ADD LIQ =================
      await _handleBuySell({ tx, logs: portalLogs, block, blockNumber, source: "portal" });

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
      toBlock: blockNumber
    });

    if (!logs.length) return;

    const txCache = new Map();
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
// CREATE TOKEN
// ===============================================================

async function _handleCreate({ tx, logs, block, blockNumber }) {

  let tokenAddress = null;
  let creator = null;
  let name = null;
  let symbol = null;
  let meta = null;
  let tax = 0;
  let baseAddress = null;
  let basePair = null;
  let firstBuy = null;
  const decimals = 18;

  // ================= DECODE LOGS =================

  for (const evLog of logs) {

    const topic = evLog.topics[0];

    if (topic === TOPICS.TOKEN_CREATED) {
      const d = _decode(["uint256", "address", "uint256", "address", "string", "string", "string"], evLog.data);
      creator = d[1].toLowerCase();
      tokenAddress = d[3].toLowerCase();
      name = d[4];
      symbol = d[5];
      meta = d[6];
    }

    if (topic === TOPICS.TOKEN_QUOTE_SET) {
      const d = _decode(["address", "address"], evLog.data);
      baseAddress = d[1].toLowerCase();
      basePair = getBasePair(baseAddress);
    }

    if (topic === TOPICS.TAX_SET) {
      const d = _decode(["address", "uint256"], evLog.data);
      tax = Number(d[1]) / 100;
    }

    if (topic === TOPICS.TOKEN_BOUGHT) {
      const d = _decode(["uint256", "address", "address", "uint256", "uint256", "uint256", "uint256"], evLog.data);
      firstBuy = {
        buyer: d[2].toLowerCase(),
        amount: d[3],
        eth: d[4],
        fee: d[5]
      };
    }

  }

  if (!tokenAddress) return;

  // ================= FALLBACK: baca QUOTE_TOKEN dari kontrak =================

  if (!baseAddress) {
    log.warn(`[FLAP CREATE] TOKEN_QUOTE_SET not found, reading from contract: ${tokenAddress}`);
    try {
      const contract = new ethers.Contract(tokenAddress, TOKEN_ABI, rpcTxProvider);
      const fields = await getContractFields(contract, { quoteToken: () => contract.QUOTE_TOKEN() });
      if (fields.quoteToken) {
        baseAddress = fields.quoteToken.toLowerCase();
        basePair = getBasePair(baseAddress);
      }
    } catch (err) {
      log.warn("[FLAP CREATE] QUOTE_TOKEN fallback failed: " + err.message);
    }
  }

  // ================= FETCH META =================

  let imageUrl = null;
  let description = null;
  let websiteUrl = null;
  let telegramUrl = null;
  let twitterUrl = null;

  try {
    const metaJSON = await fetchIPFSMeta(meta);
    description = metaJSON?.description || null;
    websiteUrl = metaJSON?.website || null;
    telegramUrl = metaJSON?.telegram || null;
    twitterUrl = metaJSON?.twitter || null;
    imageUrl = _parseImageUrl(metaJSON?.image || null);
  } catch { }

  // ================= HITUNG PRICE =================
  // Hitung di sini agar tersedia untuk publish new_token

  const basePrice = basePair ? getBasePrice(basePair) : 0;

  let priceUSDT = 0;
  let volumeUSDT = 0;

  if (firstBuy && basePair) {
    const tokenAmt = Number(ethers.formatUnits(firstBuy.amount, decimals));
    const baseAmt = Number(ethers.formatUnits(firstBuy.eth, 18));
    const priceBase = tokenAmt > 0 ? baseAmt / tokenAmt : 0;
    priceUSDT = priceBase * basePrice;
    volumeUSDT = baseAmt * basePrice;
  }

  // ================= INSERT LAUNCH =================

  try {

    console.log(`[FLAP REGISTRY] token registered via CREATE: ${tokenAddress}`);

    await insertLaunch({
      launchTime: new Date(block.timestamp * 1000).toISOString(),
      tokenAddress,
      developer: creator,
      name, symbol,
      description, imageUrl, websiteUrl, telegramUrl, twitterUrl,
      supply: TOTAL_SUPPLY,
      decimals: 18,
      taxBuy: tax,
      taxSell: tax,
      minBuy: 0,
      maxBuy: 0,
      basePair, baseAddress,
      networkCode: "BSC",
      sourceFrom: "flap.sh",
      migrated: false,
      verifiedCode: true
    });

    await insertTokenFlap({ tokenAddress, creator, blockNumber });
    addFlapTokenToMemory(tokenAddress);

    logCreate({
      platform: "flap",
      tokenAddress,
      tokenSymbol: symbol,
      tokenName: name,
      creator,
      basePair,
      baseAddress,
      taxBuy: tax,
      taxSell: tax,
      txHash: tx.hash,
      blockNumber,
      timestamp: block.timestamp * 1000,
      firstBuyAmount: firstBuy ? Number(ethers.formatUnits(firstBuy.amount, 18)) : undefined,
      firstBuyUSD: firstBuy && basePair
        ? (Number(ethers.formatUnits(firstBuy.eth, 18)) * basePrice)
        : undefined
    });

    // ================= WS: new_token =================
    // ✅ FIX: semua variabel diambil dari scope lokal yang sudah ada

    publish("new_token", {
      tokenAddress,
      symbol,
      name,
      basePair,      // ← tambah
      baseAddress,
      imageUrl,
      description,
      website: websiteUrl,
      telegram: telegramUrl,
      twitter: twitterUrl,
      devAddress: creator,
      price: priceUSDT,
      marketcap: priceUSDT * TOTAL_SUPPLY,
      volume24h: volumeUSDT,
      txCount: firstBuy ? 1 : 0,
      holderCount: 1,
      launchTime: block.timestamp * 1000,
      source: "flap.sh"
    });

  } catch (err) {
    console.error("[FLAP CREATE ERROR]", err.message);
  }

  // ================= FIRST BUY =================

  if (firstBuy && basePair) {

    const bp = getBasePrice(basePair);
    const tokenAmt = Number(ethers.formatUnits(firstBuy.amount, decimals));
    const baseAmt = Number(ethers.formatUnits(firstBuy.eth, 18));
    const priceBase = tokenAmt > 0 ? baseAmt / tokenAmt : 0;
    const pUSDT = priceBase * bp;
    const vUSDT = baseAmt * bp;

    await insertTransaction({
      tokenAddress,
      time: new Date(block.timestamp * 1000).toISOString(),
      blockNumber,
      txHash: tx.hash,
      position: "BUY",
      amountReceive: tokenAmt,
      basePayable: basePair,
      amountBasePayable: baseAmt,
      inUSDTPayable: vUSDT,
      priceBase,
      priceUSDT: pUSDT,
      addressMessageSender: firstBuy.buyer,
      tagAddress: "Developer",
      isDev: firstBuy.buyer === creator
    });

  }

}

// ===============================================================
// BUY / SELL / ADD LIQUIDITY
// ===============================================================

async function _handleBuySell({ tx, logs, block, blockNumber, source = "portal" }) {

  let trade = null;
  let position = null;
  let addLiquidity = null;

  for (const evLog of logs) {

    const topic = evLog.topics[0];

    if (topic === TOPICS.TOKEN_BOUGHT) {
      const d = _decode(["uint256", "address", "address", "uint256", "uint256", "uint256", "uint256"], evLog.data);
      trade = { token: d[1].toLowerCase(), wallet: d[2].toLowerCase(), amount: d[3], base: d[4], fee: d[5] };
      position = "BUY";
    }

    if (topic === TOPICS.TOKEN_SOLD) {
      const d = _decode(["uint256", "address", "address", "uint256", "uint256", "uint256", "uint256"], evLog.data);
      trade = { token: d[1].toLowerCase(), wallet: d[2].toLowerCase(), amount: d[3], base: d[4], fee: d[5] };
      position = "SELL";
    }

    if (topic === TOPICS.LAUNCHED_TO_DEX) {
      const d = _decode(["address", "address", "uint256", "uint256"], evLog.data);
      addLiquidity = {
        tokenAddress: d[0].toLowerCase(),
        pairAddress: d[1].toLowerCase(),
        tokenAmount: d[2],
        baseAmount: d[3]
      };
    }

  }

  if (!trade && !addLiquidity) return;

  let launchInfo = await getLaunchByToken(trade?.token || addLiquidity.tokenAddress);

  // ================= AUTO REGISTER =================

  if (!launchInfo && trade) {
    launchInfo = await _autoRegister({ tx, trade, block, blockNumber });
    if (!launchInfo) return;
  }

  if (!launchInfo) return;

  // ================= TRADE =================

  if (trade) {

    const basePrice = getBasePrice(launchInfo.basePair);
    const tokenAmt = Number(ethers.formatUnits(trade.amount, launchInfo.decimals ?? 18));
    const basePaid = Number(ethers.formatUnits(trade.base - trade.fee, 18));
    const baseAmount = Number(ethers.formatUnits(trade.base, 18));
    const priceBase = tokenAmt > 0 ? baseAmount / tokenAmt : 0;
    const priceUSDT = priceBase * basePrice;
    const volumeUSDT = baseAmount * basePrice;
    const isDev = trade.wallet === launchInfo.developerAddress;

    console.log(`[FLAP TRADE] source=${source.toUpperCase()} token=${trade.token} wallet=${trade.wallet} tx=${tx.hash}`);

    logTrade({
      platform: "flap",
      position,
      tokenAddress: trade.token,
      tokenSymbol: launchInfo.symbol,
      tokenAmount: tokenAmt,
      baseSymbol: launchInfo.basePair,
      baseAmount: basePaid,
      priceBase,
      priceUSDT,
      volumeUSDT,
      txHash: tx.hash,
      blockNumber,
      timestamp: block.timestamp * 1000,
      wallet: trade.wallet,
      isDev
    });

    await insertTransaction({
      tokenAddress: trade.token,
      time: new Date(block.timestamp * 1000).toISOString(),
      blockNumber,
      txHash: tx.hash,
      position,
      amountReceive: tokenAmt,
      basePayable: launchInfo.basePair,
      amountBasePayable: basePaid,
      inUSDTPayable: volumeUSDT,
      priceBase,
      priceUSDT,
      addressMessageSender: trade.wallet,
      tagAddress: isDev ? "Developer" : null,
      isDev
    });

  }

  // ================= ADD LIQUIDITY =================

  if (addLiquidity) {

    const basePrice = getBasePrice(launchInfo.basePair);
    const tokenAmt = Number(ethers.formatUnits(addLiquidity.tokenAmount, launchInfo.decimals ?? 18));
    const baseAmt = Number(ethers.formatUnits(addLiquidity.baseAmount, 18));
    const priceBase = tokenAmt > 0 ? baseAmt / tokenAmt : 0;
    const priceUSDT = priceBase * basePrice;
    const volumeUSDT = baseAmt * basePrice;

    const { tokenAddress, pairAddress } = addLiquidity;
    const baseAddress = launchInfo.baseAddress;
    const baseSymbol = launchInfo.basePair;

    console.log(`[FLAP ADD_LIQ] source=${source.toUpperCase()} token=${tokenAddress} pair=${pairAddress} tx=${tx.hash}`);

    logAddLiquidity({
      platform: "flap",
      tokenAddress,
      pairAddress,
      baseSymbol,
      baseAddress,
      tokenAmount: tokenAmt,
      baseAmount: baseAmt,
      priceBase,
      priceUSDT,
      volumeUSDT,
      txHash: tx.hash,
      blockNumber,
      timestamp: block.timestamp * 1000,
      sender: tx.from
    });

    await setTokenMigrated(tokenAddress, new Date(block.timestamp * 1000).toISOString());

    await insertTokenMigrate({ tokenAddress, pairAddress, baseAddress, baseSymbol, blockNumber, txHash: tx.hash });

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
      amountReceive: tokenAmt,
      basePayable: baseSymbol,
      amountBasePayable: baseAmt,
      inUSDTPayable: volumeUSDT,
      priceBase,
      priceUSDT,
      addressMessageSender: tx.from,
      tagAddress: null,
      isDev: false
    });

    await insertPairLiquidity({
      tokenAddress, baseAddress, pairAddress, baseSymbol,
      liquidityToken: tokenAmt,
      liquidityBase: baseAmt,
      blockNumber,
      txHash: tx.hash
    });

    await updateMigrationStats(volumeUSDT);

    // [ADDED]
    await updateLiquidityState({
      tokenAddress,
      platform: "dex",
      mode: "dex",

      baseAddress,
      baseSymbol,

      baseLiquidity: baseAmt,
      tokenLiquidity: tokenAmt,

      priceBase,
      isMigrated: true,
      pairAddress
    });



    await deleteTokenFlap(tokenAddress);
    flapTokenSet.delete(tokenAddress);


    publish("migrate", {
      tokenAddress,
      pairAddress,
      baseSymbol,
      priceUSDT,
      timestamp: block.timestamp * 1000
    });

  }

}

// ===============================================================
// AUTO REGISTER
// ===============================================================

async function _autoRegister({ tx, trade, block, blockNumber }) {

  const contract = new ethers.Contract(trade.token, TOKEN_ABI, rpcTxProvider);

  const fields = await getContractFields(contract, {
    name: () => contract.name(),
    symbol: () => contract.symbol(),
    decimals: () => contract.decimals(),
    metaURI: () => contract.metaURI(),
    quoteToken: () => contract.QUOTE_TOKEN(),
    taxRate: () => contract.taxRate()
  });

  const baseAddress = normalizeBaseAddress(fields.quoteToken);
  const basePair = getBasePair(baseAddress);

  if (!basePair) {
    log.warn("[FLAP SKIP] Unknown base: " + fields.quoteToken + " TX: " + tx.hash);
    return null;
  }

  const metaJSON = await fetchIPFSMeta(fields.metaURI);
  const imageUrl = _parseImageUrl(metaJSON?.image);

  console.log(`[FLAP REGISTRY] token registered via TRADE: ${trade.token}`);

  await insertTokenFlap({ tokenAddress: trade.token, creator: tx.from, blockNumber });
  addFlapTokenToMemory(trade.token);

  await insertLaunch({
    launchTime: new Date(block.timestamp * 1000).toISOString(),
    tokenAddress: trade.token,
    developer: tx.from,
    name: fields.name,
    symbol: fields.symbol,
    description: metaJSON?.description || null,
    imageUrl,
    websiteUrl: metaJSON?.website || null,
    telegramUrl: metaJSON?.telegram || null,
    twitterUrl: metaJSON?.twitter || null,
    supply: TOTAL_SUPPLY,
    decimals: fields.decimals ?? 18,
    taxBuy: Number(fields.taxRate ?? 0) / 100,
    taxSell: Number(fields.taxRate ?? 0) / 100,
    minBuy: 0,
    maxBuy: 0,
    basePair, baseAddress,
    networkCode: "BSC",
    sourceFrom: "flap.sh",
    migrated: false,
    verifiedCode: false
  });



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

handleFlapBlock.init = init;
handleFlapBlock.scanDirect = scanDirect;
// handleFlapBlock.startWS = startFlapWS;

export { handleFlapBlock };