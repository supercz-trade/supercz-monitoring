import { ethers } from "ethers";
// [MODIFIED]
import { rpcTxProvider } from "../infra/provider.js";

import { loadTokenMigrate } from "../repository/tokenMigrate.repository.js";
import { getLaunchByToken } from "../repository/launch.repository.js";
import { insertTransaction } from "../repository/transaction.repository.js";

import { updateHolderBalance } from "../repository/holder.repository.js";
import { updateHolderStats } from "../repository/holderStats.repository.js";

import { updateCandleFromTrade } from "../service/candle.service.js";
import { getBasePrice } from "../price/binancePrice.js";

const SWAP_TOPIC =
ethers.id("Swap(address,uint256,uint256,uint256,uint256,address)");

let tokenSet = new Set(); // [ADDED]

const launchCache = new Map(); // [ADDED]

export async function initMigrateCache() {

  const tokens = await loadTokenMigrate();

  tokens.forEach(t => tokenSet.add(t.toLowerCase()));

  console.log("[MIGRATE TOKENS LOADED]", tokens.length);

}

export function getTokenAddresses() { // [ADDED]
  return Array.from(tokenSet);
}

function hexToBigInt(hex) {
  return BigInt(hex);
}

export async function handleBuySellMigrate({
  txHash,
  logs,
  block,
  blockNumber

}) {

  const transferLog = logs[0];

  const tokenAddress =
  transferLog.address.toLowerCase();

  if (!tokenSet.has(tokenAddress))
    return false;

  let launchInfo =
  launchCache.get(tokenAddress);

  if (!launchInfo) {

    launchInfo =
    await getLaunchByToken(tokenAddress);

    if (!launchInfo) return false;

    launchCache.set(tokenAddress, launchInfo);

  }

  // ================= GET RECEIPT =================

 // [MODIFIED]
const receipt =
await rpcTxProvider.getTransactionReceipt(txHash);

  if (!receipt) return false;

  let swapLog;

  for (const log of receipt.logs) {

    if (log.topics?.[0] === SWAP_TOPIC) {
      swapLog = log;
      break;
    }

  }

  if (!swapLog) return false;

  // ================= WALLET PARSE =================

  // ================= TX INFO ================= // [MODIFIED]

// [MODIFIED]
const tx =
await rpcTxProvider.getTransaction(txHash);

if (!tx) return false;

const wallet = tx.from.toLowerCase(); // [MODIFIED]

// ================= POSITION ================= // [MODIFIED]

const from =
"0x" + transferLog.topics[1].slice(26);

const to =
"0x" + transferLog.topics[2].slice(26);

const pair =
swapLog.address.toLowerCase();

let position;

if (from.toLowerCase() === pair) {

  position = "BUY";

} else if (to.toLowerCase() === pair) {

  position = "SELL";

} else {

  return false;

}

  // ================= PARSE SWAP =================

  const data = swapLog.data.slice(2);

  const amount0In  = hexToBigInt("0x" + data.slice(0, 64));
  const amount1In  = hexToBigInt("0x" + data.slice(64, 128));
  const amount0Out = hexToBigInt("0x" + data.slice(128, 192));
  const amount1Out = hexToBigInt("0x" + data.slice(192, 256));

  const tokenAmount =
  Number(
    ethers.formatUnits(
      transferLog.data,
      launchInfo.decimals
    )
  );

  let baseAmount;

  if (amount0In > 0n || amount1In > 0n) {

    baseAmount =
    Number(
      ethers.formatUnits(
        amount0In > 0n ? amount0In : amount1In,
        18
      )
    );

  } else {

    baseAmount =
    Number(
      ethers.formatUnits(
        amount0Out > 0n ? amount0Out : amount1Out,
        18
      )
    );

  }

  const basePrice =
  await getBasePrice(launchInfo.basePair);

  const priceBase =
  baseAmount / tokenAmount;

  const priceUSDT =
  priceBase * basePrice;

  const volumeUSDT =
  baseAmount * basePrice;

  console.log("\n================ PANCAKE TRADE DETECTED =================");

console.log("TX:", txHash);
console.log("TOKEN:", tokenAddress);
console.log("PAIR:", pair);
console.log("WALLET:", wallet);
console.log("POSITION:", position);

console.log("TOKEN AMOUNT:", tokenAmount);
console.log("BASE AMOUNT:", baseAmount);

console.log("PRICE BASE:", priceBase);
console.log("PRICE USD:", priceUSDT);

console.log("VOLUME USD:", volumeUSDT);

console.log("=========================================================\n");

  // ================= INSERT TRANSACTION =================

  await insertTransaction({

    tokenAddress,

    time: new Date(block.timestamp * 1000),

    blockNumber,

    txHash,

    position,

    amountReceive: tokenAmount,

    basePayable: launchInfo.basePair,

    amountBasePayable: baseAmount,

    inUSDTPayable: volumeUSDT,

    priceBase,
    priceUSDT,

    addressMessageSender: wallet,

    isDev:
    wallet.toLowerCase() ===
    launchInfo.developer_address

  });

  // ================= UPDATE HOLDERS =================

  await updateHolderBalance(
    tokenAddress,
    from.toLowerCase(),
    -tokenAmount
  );

  await updateHolderBalance(
    tokenAddress,
    to.toLowerCase(),
    tokenAmount
  );

  // ================= HOLDER STATS =================

  await updateHolderStats({

    tokenAddress,

    wallet,

    buyUsd:
    position === "BUY"
    ? volumeUSDT
    : 0,

    sellUsd:
    position === "SELL"
    ? volumeUSDT
    : 0,

    buyBase:
    position === "BUY"
    ? baseAmount
    : 0,

    sellBase:
    position === "SELL"
    ? baseAmount
    : 0,

    buyCount:
    position === "BUY"
    ? 1
    : 0,

    sellCount:
    position === "SELL"
    ? 1
    : 0

  });

  // ================= UPDATE CANDLE =================

  await updateCandleFromTrade({

    tokenAddress,

    time: new Date(block.timestamp * 1000),

    priceUSDT,

    volumeUSDT,

    amountReceive: tokenAmount,

    amountBasePayable: baseAmount

  });

  return true;

}