import "dotenv/config";
import { ethers } from "ethers";

import { startPriceStream, waitPricesReady } from "./src/price/binancePrice.js";
import { handleFourmemeBlock } from "./src/listener/fourmemeHandler.js";

const provider = new ethers.JsonRpcProvider(process.env.BSC_RPC_TX);

const txHash = process.argv[2];

if (!txHash) {
  console.log("Usage:");
  console.log("node test.js 0xTX_HASH");
  process.exit(1);
}

// ================= PRICE =================

async function initPrice() {

  console.log("[PRICE] init stream");

  startPriceStream();

  await waitPricesReady(10000);

}

// ================= MAIN =================

async function run() {

  console.log("================================");
  console.log("TEST FOURMEME HANDLER");
  console.log("TX:", txHash);
  console.log("================================");

  await initPrice();

  const tx = await provider.getTransaction(txHash);
  const receipt = await provider.getTransactionReceipt(txHash);

  if (!tx || !receipt) {
    console.log("TX not found");
    return;
  }

  const block = await provider.getBlock(receipt.blockNumber);

  const txMap = new Map();
  txMap.set(txHash, receipt.logs);

  await handleFourmemeBlock({
    txMap,
    block,
    blockNumber: receipt.blockNumber
  });

  console.log("================================");
  console.log("TEST DONE");
  console.log("================================");

}

run().catch(console.error);