import { ethers } from "ethers";
import { processLaunch } from "../service/fourmeme.service.js";
import { getLaunchByToken, setTokenMigrated } from "../repository/launch.repository.js";
import { insertTransaction } from "../repository/transaction.repository.js";
import { insertPairLiquidity } from "../repository/pairLiquidity.repository.js";
import { insertTokenMigrate } from "../repository/tokenMigrate.repository.js";
import { getBasePrice } from "../price/binancePrice.js";

import { updateHolderBalance } from "../repository/holder.repository.js";
import { updateCandleFromTrade } from "../service/candle.service.js";

const safeLower = (v) => (typeof v === "string" ? v.toLowerCase() : null);

const ADD_LIQ_SELECTOR = "0xe3412e3d";

const ERC20_TRANSFER_TOPIC =
  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

export async function handleAddLiquidity({
  tx,
  receipt,
  block,
  blockNumber,
  manager
}) {

  // ================= METHOD FILTER =================
  if (!tx.data?.startsWith(ADD_LIQ_SELECTOR)) return false;

  const managerLower = safeLower(manager); // [ADDED]

  for (const log of receipt.logs) {

    // ================= ERC20 FILTER =================
    if (log.topics?.[0] !== ERC20_TRANSFER_TOPIC) continue;
    if (!log.topics[1] || !log.topics[2]) continue;

    const tokenAddress = safeLower(log.address);

    const from = safeLower(
      ethers.getAddress("0x" + log.topics[1].slice(26))
    );

    const to = safeLower(
      ethers.getAddress("0x" + log.topics[2].slice(26))
    );

    // ================= MANAGER SENDING TOKEN =================
    if (from !== managerLower) continue; // [MODIFIED]

    const tokenAmount = Number(
      ethers.formatUnits(log.data, 18)
    );

    // ================= GET LAUNCH INFO =================
    let launchInfo = await getLaunchByToken(tokenAddress);

    if (!launchInfo) {

      console.log("⚠ Token not registered, fetching launch info");

      const result = await processLaunch(tokenAddress);
      if (!result) continue;

      launchInfo = await getLaunchByToken(tokenAddress);
      if (!launchInfo) continue;
    }

    const baseSymbol = launchInfo.basePair;
    const baseAddress = safeLower(launchInfo.baseAddress);

    let pairAddress = null;
    let baseAmount = 0;

    // ================= DETECT PAIR + BASE =================
    for (const l of receipt.logs) {

      if (l.topics?.[0] !== ERC20_TRANSFER_TOPIC) continue;
      if (!l.topics[2]) continue;

      const toAddr = safeLower(
        ethers.getAddress("0x" + l.topics[2].slice(26))
      );

      const logToken = safeLower(l.address);

      // token -> pair
      if (logToken === tokenAddress) {
        pairAddress = toAddr;
      }

      // base -> pair
      if (logToken === baseAddress) { // [MODIFIED]
        baseAmount = Number(
          ethers.formatUnits(l.data, 18)
        );
      }
    }

    if (!pairAddress) {
      console.log("❌ Pair address not detected");
      continue;
    }

    // ================= PRICE CALC =================
    const priceBase = baseAmount / tokenAmount;
    const basePriceUSDT = getBasePrice(baseSymbol);
    const priceUSDT = priceBase * basePriceUSDT;
    const inUSDTPayable = baseAmount * basePriceUSDT;

    const position = "ADD_LIQUIDITY";
    const isDev = false;

    // ================= LOGGING =================
    console.log("\n================ ADD LIQUIDITY FOUR.MEME DETECTED ================");
    console.log("Token Address      :", tokenAddress);
    console.log("Pair Address       :", pairAddress);
    console.log("Time               :", new Date(block.timestamp * 1000));
    console.log("Block Number       :", blockNumber);
    console.log("TX Hash            :", tx.hash);
    console.log("Position           :", position);
    console.log("Token Amount       :", tokenAmount);
    console.log("Base Symbol        :", baseSymbol);
    console.log("Base Amount        :", baseAmount);
    console.log("USDT Value         :", inUSDTPayable);
    console.log("Price (Base)       :", priceBase);
    console.log("Price (USDT)       :", priceUSDT);
    console.log("Sender             :", tx.from);
    console.log("Is Dev             :", isDev);
    console.log("===============================================\n");

    // ================= SET MIGRATED =================
    await setTokenMigrated(
      tokenAddress,
      new Date(block.timestamp * 1000)
    );

    await insertTokenMigrate({
    
                tokenAddress,
                pairAddress,
                baseAddress,
                baseSymbol,
    
                blockNumber,
                txHash: tx.hash
    
            });

    // ================= SAVE TRADE =================
    await insertTransaction({
      tokenAddress,
      time: new Date(block.timestamp * 1000),
      blockNumber,
      txHash: tx.hash,
      position: "Add Liquidity",
      amountReceive: tokenAmount,
      basePayable: baseSymbol,
      amountBasePayable: baseAmount,
      inUSDTPayable,
      priceBase,
      priceUSDT,
      addressMessageSender: tx.from,
      isDev
    });

    // ================= SAVE PAIR =================
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

    // ================= UPDATE HOLDERS =================
    await updateHolderBalance(tokenAddress, tx.from, -tokenAmount);
    await updateHolderBalance(tokenAddress, pairAddress, tokenAmount);

    // ================= UPDATE CANDLE =================
    await updateCandleFromTrade({
      tokenAddress,
      time: new Date(block.timestamp * 1000),
      priceUSDT,
      volumeUSDT: inUSDTPayable,
      amountReceive: tokenAmount,
      amountBasePayable: baseAmount
    });

    console.log("✅ Liquidity recorded");

    break;
  }

  return true;
}