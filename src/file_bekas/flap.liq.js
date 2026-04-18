import { ethers } from "ethers";

import { handleBuySell } from "./flap.buySell.js";

import { getLaunchByToken, setTokenMigrated }
    from "../repository/launch.repository.js";

import { insertTransaction }
    from "../repository/transaction.repository.js";

import { insertPairLiquidity }
    from "../repository/pairLiquidity.repository.js";

import { getBasePrice }
    from "../price/binancePrice.js";

import { updateHolderBalance }
    from "../repository/holder.repository.js";

import { insertTokenMigrate } from "../repository/tokenMigrate.repository.js";

import { deleteTokenFlap } from "../repository/tokenFlap.repository.js";

import { updateCandleFromTrade }
    from "../service/candle.service.js";

const LAUNCHED_TO_DEX_TOPIC =
    "0x6e4f47630b8745b8cacbd44f42a8a33e7eea7cc08ef22fc7630f4385784ff7d";

const TOKEN_BOUGHT =
    ethers.id("TokenBought(uint256,address,address,uint256,uint256,uint256,uint256)");

const TOKEN_SOLD =
    ethers.id("TokenSold(uint256,address,address,uint256,uint256,uint256,uint256)");

export async function handleFlapAddLiquidity({
    provider,
    tx,
    receipt,
    block,
    blockNumber
}) {

    for (const log of receipt.logs) {

        const topic = log.topics[0];

        // ================= BUY / SELL =================

        if (topic === TOKEN_BOUGHT || topic === TOKEN_SOLD) {

            return await handleBuySell({
                provider,
                tx,
                logs: receipt.logs,
                block,
                blockNumber
            });

        }

        // ================= ADD LIQUIDITY =================

        if (topic !== LAUNCHED_TO_DEX_TOPIC) continue;

        const decoded = ethers.AbiCoder.defaultAbiCoder().decode(
            [
                "address",
                "address",
                "uint256",
                "uint256"
            ],
            log.data
        );

        const tokenAddress = decoded[0].toLowerCase();
        const pairAddress = decoded[1].toLowerCase();

        const tokenAmount =
            Number(ethers.formatUnits(decoded[2], 18));

        const baseAmount =
            Number(ethers.formatUnits(decoded[3], 18));

        const launchInfo = await getLaunchByToken(tokenAddress);

        if (!launchInfo) return false;

        const baseSymbol = launchInfo.basePair;
        const baseAddress = launchInfo.baseAddress;

        const priceBase = baseAmount / tokenAmount;

        const basePriceUSDT = getBasePrice(baseSymbol);

        const priceUSDT = priceBase * basePriceUSDT;

        const volumeUSDT = baseAmount * basePriceUSDT;

        console.log("\n================ FLAP ADD LIQUIDITY =================");

        console.log("Token Address :", tokenAddress);
        console.log("Pair Address  :", pairAddress);
        console.log("Token Amount  :", tokenAmount);
        console.log("Base Amount   :", baseAmount);
        console.log("Price USDT    :", priceUSDT);
        console.log("TX            :", tx.hash);

        console.log("=====================================================\n");

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

        await insertTransaction({

            tokenAddress,

            time: new Date(block.timestamp * 1000),

            blockNumber,

            txHash: tx.hash,

            position: "Add Liquidity",

            amountReceive: tokenAmount,

            basePayable: baseSymbol,

            amountBasePayable: baseAmount,

            inUSDTPayable: volumeUSDT,

            priceBase,

            priceUSDT,

            addressMessageSender: tx.from,

            isDev: false

        });

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

        await updateHolderBalance(tokenAddress, pairAddress, tokenAmount);

        await updateCandleFromTrade({

            tokenAddress,

            time: new Date(block.timestamp * 1000),

            priceUSDT,

            volumeUSDT,

            amountReceive: tokenAmount,

            amountBasePayable: baseAmount

        });

        deleteTokenFlap(tokenAddress);

        console.log("✅ Flap liquidity recorded");

        return true;

    }

    return false;

}