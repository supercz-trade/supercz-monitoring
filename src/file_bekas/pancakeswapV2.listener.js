// [MODIFIED]
import { wssProvider, rpcLogsProvider, rpcTxProvider } from "../infra/provider.js";

import {
    initMigrateCache,
    handleBuySellMigrate,
    getTokenAddresses // [ADDED]
} from "./pancakeswapV2.transaction.js";

import { ethers } from "ethers";

const TRANSFER_TOPIC =
    ethers.id("Transfer(address,address,uint256)");

const seenTx = new Set();

let tokenAddresses = []; // [ADDED]

export async function startPancakeListener() {

    console.log("[MONITOR] PancakeSwapV2 listener started");

    await initMigrateCache();

    tokenAddresses = await getTokenAddresses(); // [ADDED]

    console.log("[TOKENS LOADED]", tokenAddresses.length);

    // [MODIFIED]
    wssProvider.on("block", async (blockNumber) => {

        let logs;
        // [MODIFIED]
        const block = await rpcTxProvider.getBlock(blockNumber);

        try {

            // [MODIFIED]
            logs = await rpcLogsProvider.getLogs({
                fromBlock: blockNumber,
                toBlock: blockNumber,
                address: tokenAddresses,
                topics: [TRANSFER_TOPIC]
            });

        } catch (err) {

            console.log("getLogs error:", err.message);
            return;

        }

        if (!logs.length) return;

        const txMap = new Map();

        for (const log of logs) {

            const txHash = log.transactionHash;

            if (!txMap.has(txHash)) {
                txMap.set(txHash, []);
            }

            txMap.get(txHash).push(log);

        }

        for (const [txHash, txLogs] of txMap.entries()) {

            if (seenTx.has(txHash)) continue;

            seenTx.add(txHash);

            try {

                await handleBuySellMigrate({

                    txHash,
                    logs: txLogs,
                    block,
                    blockNumber

                });

            } catch (err) {

                console.log("dispatcher error:", err.message);

            }

        }

        if (seenTx.size > 5000) {
            seenTx.clear();
        }

    });

}