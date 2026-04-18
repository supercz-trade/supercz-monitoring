// [MODIFIED]
import { wssProvider, rpcLogsProvider, rpcTxProvider } from "../infra/provider.js";
import { handleCreateToken } from "./fourmeme.createToken.js";
import { handleBuySell } from "./fourmeme.buysell.js";
import { handleAddLiquidity } from "./fourmeme.addLiquidity.js"; // [ADDED]

const safeLower = (v) => (typeof v === "string" ? v.toLowerCase() : null);

const MANAGER = safeLower(process.env.FOUR_MEME_MANAGER);

const seenTx = new Set();

export function startListener() {
  console.log("[MONITOR] Four.meme listener started");
  console.log("Manager:", MANAGER);

  // [MODIFIED]
  wssProvider.on("block", async (blockNumber) => {
    let logs;

    try {
      // [MODIFIED]
      logs = await rpcLogsProvider.getLogs({
        fromBlock: blockNumber,
        toBlock: blockNumber,
        address: MANAGER
      });
    } catch (err) {
      console.log("getLogs error:", err.message);
      return;
    }

    if (!logs.length) return;

    let block;

    try {

      block = await rpcTxProvider.getBlock(blockNumber);

    } catch (err) {

      console.log("getBlock error:", err.message);
      return;

    }

    for (const log of logs) {
      const txHash = log.transactionHash;

      if (seenTx.has(txHash)) continue;
      seenTx.add(txHash);

      let tx, receipt;

      try {
        // [MODIFIED]
        tx = await rpcTxProvider.getTransaction(txHash);
        receipt = await rpcTxProvider.getTransactionReceipt(txHash);
      } catch (err) {
        console.log("tx fetch error:", err.message);
        continue;
      }

      if (!tx || !receipt) continue;

      try {

        // ================= CREATE TOKEN =================
        const handledCreate = await handleCreateToken({
          tx,
          receipt,
          block,
          blockNumber,
          manager: MANAGER
        });

        if (handledCreate) continue;

        // ================= ADD LIQUIDITY =================
        const handledLiquidity = await handleAddLiquidity({
          tx,
          receipt,
          block,
          blockNumber,
          manager: MANAGER
        });

        if (handledLiquidity) continue;



        // ================= BUY / SELL =================
        await handleBuySell({
          tx,
          receipt,
          block,
          blockNumber,
          manager: MANAGER
        });

      } catch (err) {
        console.log("Dispatcher error:", err.message);
      }
    }

    if (seenTx.size > 5000) seenTx.clear();
  });
}