import { processLaunch } from "../service/fourmeme.service.js";
import { insertTransaction } from "../repository/transaction.repository.js";
import { getBasePrice } from "../price/binancePrice.js";

import { updateHolderBalance } from "../repository/holder.repository.js"; // [ADDED]
import { updateCandleFromTrade } from "../service/candle.service.js"; // [ADDED]
import { updateHolderStats } from "../repository/holderStats.repository.js"; // [ADDED]

const safeLower = (v) => (typeof v === "string" ? v.toLowerCase() : null);

const CREATE_TOKEN_SELECTOR = "0x519ebb10";
const ERC20_TRANSFER_TOPIC =
  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

export async function handleCreateToken({
  tx,
  receipt,
  block,
  blockNumber,
  manager
}) {
  if (
    safeLower(tx.to) !== manager ||
    !tx.data?.startsWith(CREATE_TOKEN_SELECTOR)
  ) {
    return false;
  }


  let tokenAddress = null;

  for (const rlog of receipt.logs) {
    if (rlog.topics?.[0] === ERC20_TRANSFER_TOPIC) {
      const from = "0x" + rlog.topics[1].slice(26);

      if (
        safeLower(from) ===
        "0x0000000000000000000000000000000000000000"
      ) {
        tokenAddress = safeLower(rlog.address);
        break;
      }
    }
  }

  if (!tokenAddress) {
    console.log("❌ Mint log not found");
    return true;
  }

  console.log("✅ Token:", tokenAddress);

  const launchInfo = await processLaunch(tokenAddress, {
  onchainLaunchTime: new Date(block.timestamp * 1000)
});

  if (!launchInfo || !launchInfo.basePair) {
    console.log("Launch info invalid");
    return true;
  }

  // ===== Extract dev token receive =====
  let tokenAmount = 0;

  for (const rlog of receipt.logs) {
    if (
      rlog.topics?.[0] === ERC20_TRANSFER_TOPIC &&
      safeLower(rlog.address) === tokenAddress
    ) {
      const to = "0x" + rlog.topics[2].slice(26);

      if (safeLower(to) === safeLower(tx.from)) {
        tokenAmount = Number(rlog.data) / 1e18;
        break;
      }
    }
  }

const baseSymbol = launchInfo.basePair;

const basePriceUSDT = getBasePrice(baseSymbol);

const priceBase = Number(launchInfo.priceInBasePair);
const baseAmount = tokenAmount * priceBase;

const priceUSDT = priceBase * basePriceUSDT;

const position = "GENESIS"; // [ADDED]
const isDev = true; // [ADDED]

console.log("\n================ CREATE Four.meme DETECTED ================"); // [ADDED]
console.log("Token Address      :", tokenAddress); // [ADDED]
console.log("Time               :", new Date(block.timestamp * 1000)); // [ADDED]
console.log("Block Number       :", blockNumber); // [ADDED]
console.log("TX Hash            :", tx.hash); // [ADDED]
console.log("Position           :", position); // [ADDED]
console.log("Token Amount       :", tokenAmount); // [ADDED]
console.log("Base Symbol        :", baseSymbol); // [ADDED]
console.log("Base Amount        :", baseAmount); // [ADDED]
console.log("USDT Value         :", baseAmount * basePriceUSDT); // [ADDED]
console.log("Price (Base)       :", priceBase); // [ADDED]
console.log("Price (USDT)       :", priceUSDT); // [ADDED]
console.log("Sender             :", tx.from); // [ADDED]
console.log("Is Dev             :", isDev); // [ADDED]
console.log("===============================================\n"); // [ADDED]

await insertTransaction({
  tokenAddress,
  time: new Date(block.timestamp * 1000),
  blockNumber,
  txHash: tx.hash,
  position: "GENESIS",
  amountReceive: tokenAmount,
  basePayable: baseSymbol,
  amountBasePayable: baseAmount,
  inUSDTPayable: baseAmount * basePriceUSDT,
  priceBase,
  priceUSDT,
  addressMessageSender: tx.from,
  isDev: true
});

// ================= UPDATE HOLDERS =================
// ================= UPDATE HOLDERS =================
await updateHolderBalance(tokenAddress, tx.from, tokenAmount);

// ================= UPDATE HOLDER STATS =================
await updateHolderStats({
  tokenAddress,
  wallet: safeLower(tx.from),
  buyUsd: baseAmount * basePriceUSDT,
  sellUsd: 0,
  buyBase: baseAmount,
  sellBase: 0,
  buyCount: 1,
  sellCount: 0
});
// ================= CREATE FIRST CANDLE =================
await updateCandleFromTrade({
  tokenAddress,
  time: new Date(block.timestamp * 1000),
  priceUSDT,                       // [MODIFIED]
  volumeUSDT: baseAmount * basePriceUSDT, // [ADDED]
  amountReceive: tokenAmount,
  amountBasePayable: baseAmount
});

  console.log("✅ Genesis transaction saved");

  return true;
}