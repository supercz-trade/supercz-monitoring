import { ethers } from "ethers";

import { loadTokenFlap } from "../repository/tokenFlap.repository.js";
import { handleBuySell } from "./flap.buySell.js";

const TOKEN_BOUGHT =
    ethers.id("TokenBought(uint256,address,address,uint256,uint256,uint256,uint256)");

const TOKEN_SOLD =
    ethers.id("TokenSold(uint256,address,address,uint256,uint256,uint256,uint256)");
const LAUNCHED_TO_DEX =
    ethers.id("LaunchedToDEX(address,address,uint256,uint256)");
let tokenSet = new Set(); // [ADDED]

// ================= LOAD TOKENS =================

export async function loadFlapTokens() {

    const tokens = await loadTokenFlap();

    tokenSet = new Set(
        tokens.map((t) => t.toLowerCase())
    );

    console.log("[FLAP TOKENS LOADED]", tokenSet.size); // [ADDED]

}


// ================= BLOCK SCANNER =================

export async function scanFlapTransactions({
    provider,
    blockNumber
}) {

    const logs = await provider.getLogs({
        fromBlock: blockNumber,
        toBlock: blockNumber,
        topics: [[
            TOKEN_BOUGHT,
            TOKEN_SOLD,
            LAUNCHED_TO_DEX
        ]]
    });

    if (!logs.length) return;

    const block = await provider.getBlock(blockNumber);

    const txCache = new Map();

    for (const log of logs) {

        try {

            const decoded = ethers.AbiCoder.defaultAbiCoder().decode(
                [
                    "uint256",
                    "address",
                    "address",
                    "uint256",
                    "uint256",
                    "uint256",
                    "uint256"
                ],
                log.data
            );

            const tokenAddress = decoded[1].toLowerCase();

            if (!tokenSet.has(tokenAddress)) continue;

            let tx = txCache.get(log.transactionHash);

            if (!tx) {

                tx = await provider.getTransaction(log.transactionHash);

                txCache.set(log.transactionHash, tx);

            }

            await handleBuySell({

                provider,
                tx,
                logs: [log],   // hanya log terkait
                block,
                blockNumber

            });

        } catch (err) {

            console.error("FLAP PARSE ERROR", err);

        }

    }

}