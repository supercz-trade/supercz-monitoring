import { ethers } from "ethers";
import { helper } from "../infra/helper3.js";
import { processLaunch } from "../service/fourmeme.service.js";
import { getLaunchByToken } from "../repository/launch.repository.js";
import { insertTransaction } from "../repository/transaction.repository.js";
import { getBasePrice } from "../price/binancePrice.js";

import { updateHolderBalance } from "../repository/holder.repository.js"; // [ADDED]
import { updateCandleFromTrade } from "../service/candle.service.js"; // [ADDED]
import { updateHolderStats } from "../repository/holderStats.repository.js"; // [ADDED]

const safeLower = (v) => (typeof v === "string" ? v.toLowerCase() : null);

const ERC20_TRANSFER_TOPIC =
  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

// ================= BASE WHITELIST =================
const BASE_TOKEN_WHITELIST = {
  BNB: "0x0000000000000000000000000000000000000000",
  USDT: "0x55d398326f99059ff775485246999027b3197955",
  USD1: "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d",
  USDC: "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
  CAKE: "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82",
  ASTER: "0x000Ae314E2A2172a039B26378814C252734f556A"
};

const BASE_ADDRESS_MAP = Object.fromEntries(
  Object.entries(BASE_TOKEN_WHITELIST).map(
    ([symbol, address]) => [address.toLowerCase(), symbol]
  )
);

const SKIP_METHODS = [
  "0x519ebb10", // createToken
  "0xe3412e3d"  // addLiquidity
];

function getBaseSymbolFromQuote(quoteAddress) {
  if (!quoteAddress) return "UNKNOWN";
  return BASE_ADDRESS_MAP[quoteAddress.toLowerCase()] || "UNKNOWN";
}

// ================= MAIN =================
export async function handleBuySell({
  tx,
  receipt,
  block,
  blockNumber,
  manager
}) {

  if (SKIP_METHODS.some(sig => tx.data?.startsWith(sig))) {
    return false;
  }
  const processedTokens = new Set();

  for (const log of receipt.logs) {

    // ================= FILTER ONLY ERC20 TRANSFER =================
    if (log.topics?.[0] !== ERC20_TRANSFER_TOPIC) continue;
    if (!log.topics[1] || !log.topics[2]) continue;

    const tokenAddress = safeLower(log.address);

    // ================= SKIP BASE TOKEN AS TARGET =================
    if (BASE_ADDRESS_MAP[tokenAddress]) {   // [ADDED]
      continue;                             // [ADDED]
    }

    if (processedTokens.has(tokenAddress)) continue;

    const from = safeLower(
      ethers.getAddress("0x" + log.topics[1].slice(26))
    );

    const to = safeLower(
      ethers.getAddress("0x" + log.topics[2].slice(26))
    );

    // ================= STRICT MANAGER INVOLVEMENT =================
    if (from !== manager && to !== manager) continue;

    processedTokens.add(tokenAddress);

    let position;
    if (from === manager) {
      position = "BUY";   // manager kirim token
    } else {
      position = "SELL";  // manager terima token
    }

    // ================= ENSURE TOKEN REGISTERED =================
    let launchInfo = await getLaunchByToken(tokenAddress);

    if (!launchInfo) {
      await processLaunch(tokenAddress);
      launchInfo = await getLaunchByToken(tokenAddress);
      if (!launchInfo) continue;
    }

    // ================= GET ONCHAIN PRICE =================
    let priceBase = 0;
    let baseSymbol = "UNKNOWN";

    try {
      const info = await helper.getTokenInfo(tokenAddress);

      const quoteAddress = info[2];
      const lastPrice = info[3];

      baseSymbol = getBaseSymbolFromQuote(quoteAddress);

      priceBase = Number(
        ethers.formatUnits(lastPrice, 18)
      );

    } catch (err) {
      console.log("Helper3 error:", err.message);
      continue;
    }

    const basePriceUSDT = getBasePrice(baseSymbol);
    const tokenAmount = Number(
      ethers.formatUnits(log.data, 18)
    );

    const baseAmount = tokenAmount * priceBase;
    const priceUSDT = priceBase * basePriceUSDT;

    // ================= DEV CHECK =================
    const isDev =
      safeLower(tx.from) ===
      safeLower(launchInfo.developer_address);

    // ================= DEBUG LOG =================
    console.log("\n================ TRADE Four.meme DETECTED ================");
    console.log("Token Address      :", tokenAddress);
    console.log("Time               :", new Date(block.timestamp * 1000));
    console.log("Block Number       :", blockNumber);
    console.log("TX Hash            :", tx.hash);
    console.log("Position           :", position);
    console.log("Token Amount       :", tokenAmount);
    console.log("Base Symbol        :", baseSymbol);
    console.log("Base Amount        :", baseAmount);
    console.log("USDT Value         :", baseAmount * basePriceUSDT);
    console.log("Price (Base)       :", priceBase);
    console.log("Price (USDT)       :", priceUSDT);
    console.log("Sender             :", tx.from);
    console.log("Is Dev             :", isDev);
    console.log("===============================================\n");

    await insertTransaction({
      tokenAddress,
      time: new Date(block.timestamp * 1000),
      blockNumber,
      txHash: tx.hash,
      position,
      amountReceive: tokenAmount,
      basePayable: baseSymbol,
      amountBasePayable: baseAmount,
      inUSDTPayable: baseAmount * basePriceUSDT,
      priceBase,
      priceUSDT,
      addressMessageSender: tx.from,
      isDev
    });

    // ================= UPDATE HOLDERS =================
    // ================= UPDATE HOLDERS =================
    await updateHolderBalance(tokenAddress, from, -tokenAmount); // [MODIFIED]
    await updateHolderBalance(tokenAddress, to, tokenAmount);    // [MODIFIED]

    // ================= UPDATE HOLDER STATS =================
    const usdValue = baseAmount * basePriceUSDT; // [ADDED]

    await updateHolderStats({                    // [ADDED]
      tokenAddress,
      wallet: tx.from,
      buyUsd: position === "BUY" ? usdValue : 0,
      sellUsd: position === "SELL" ? usdValue : 0,
      buyBase: position === "BUY" ? baseAmount : 0,
      sellBase: position === "SELL" ? baseAmount : 0,
      buyCount: position === "BUY" ? 1 : 0,
      sellCount: position === "SELL" ? 1 : 0
    });
    // ================= UPDATE CANDLE =================
    await updateCandleFromTrade({ // [ADDED]
      tokenAddress,
      time: new Date(block.timestamp * 1000),
      priceUSDT,
      volumeUSDT: baseAmount * basePriceUSDT,
      amountReceive: tokenAmount,
      amountBasePayable: baseAmount
    });
  }

  return true;
}