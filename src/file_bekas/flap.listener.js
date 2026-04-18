import { wssProvider, rpcLogsProvider, rpcTxProvider } from "../infra/provider.js";

import { handleCreateToken } from "./flap.createToken.js";
import { handleBuySell } from "./flap.buySell.js";
import { loadFlapTokens, scanFlapTransactions } 
from "./flap.token.transaction.js";

const PORTAL = process.env.FLAP_PORTAL.toLowerCase();

const seenTx = new Set();

export async function startFlapListener() {

  console.log("[MONITOR] Flap listener started");

  // load semua token dari DB
  await loadFlapTokens();

  wssProvider.on("block", async (blockNumber) => {

    let logs;

    try {

      logs = await rpcLogsProvider.getLogs({
        address: PORTAL,
        fromBlock: blockNumber,
        toBlock: blockNumber
      });

    } catch (err) {

      console.log("getLogs error:", err.message);
      return;

    }

    if (!logs.length) {
      await scanFlapTransactions({
        provider: rpcLogsProvider,
        blockNumber
      });
      return;
    }

    let block;

    try {

      block = await rpcTxProvider.getBlock(blockNumber);

    } catch (err) {

      console.log("getBlock error:", err.message);
      return;

    }

    // ================= GROUP TX =================

    const txMap = new Map();

    for (const log of logs) {

      const txHash = log.transactionHash;

      if (!txMap.has(txHash)) {
        txMap.set(txHash, []);
      }

      txMap.get(txHash).push(log);

    }

    for (const [txHash, txLogs] of txMap.entries()) {

      // ================= DUPLICATE PROTECTION =================

      if (seenTx.has(txHash)) continue;
      seenTx.add(txHash);

      try {

        const tx = await rpcTxProvider.getTransaction(txHash);

        if (!tx) continue;

        // ================= CREATE TOKEN =================

        const created = await handleCreateToken({
          tx,
          logs: txLogs,
          block,
          blockNumber
        });

        if (created) continue;

        // ================= BUY / SELL =================

        await handleBuySell({
          provider: rpcTxProvider,
          tx,
          logs: txLogs,
          block,
          blockNumber
        });

      } catch (err) {

        console.log("dispatcher error:", err.message);

      }

    }

    // ================= CLEAN MEMORY CACHE =================

    if (seenTx.size > 5000) {

      console.log("[CACHE] Clearing seenTx cache");

      seenTx.clear();

    }

    // ================= SCAN TOKEN DIRECT TRADE =================

    await scanFlapTransactions({
      provider: rpcLogsProvider,
      blockNumber
    });

  });

}