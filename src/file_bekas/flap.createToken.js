import { ethers } from "ethers";

import { insertLaunch } from "../repository/launch.repository.js";
import { fetchIPFSMeta } from "../service/ipfsMeta.service.js";

import { getBasePrice } from "../price/binancePrice.js";

import { updateHolderBalance } from "../repository/holder.repository.js";
import { updateCandleFromTrade } from "../service/candle.service.js";
import { updateHolderStats } from "../repository/holderStats.repository.js";
import { insertTokenFlap } from "../repository/tokenFlap.repository.js";

const CREATE_METHOD = new Set([
    "0x0ba6324e", // V2
    "0x2e2fdbd9", // V3
    "0x64fd8b9e", // V4
    "0x9f0bb8a9",  // V5
    "0x6f8e27ec"
]);


const TOKEN_CREATED =
    ethers.id("TokenCreated(uint256,address,uint256,address,string,string,string)");

const TOKEN_QUOTE_SET =
    ethers.id("TokenQuoteSet(address,address)");

const TAX_SET =
    ethers.id("FlapTokenTaxSet(address,uint256)");

const TRANSFER =
    ethers.id("TransferFlapToken(address,address,uint256)");

const TOKEN_BOUGHT =
    ethers.id("TokenBought(uint256,address,address,uint256,uint256,uint256,uint256)");

const BASE_TOKEN_WHITELIST = {
    BNB: "0x0000000000000000000000000000000000000000",
    USDT: "0x55d398326f99059ff775485246999027b3197955",
    USD1: "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d",
    USDC: "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
    ASTER: "0x000Ae314E2A2172a039B26378814C252734f556A"
};

function getBasePair(address) {
    const lower = address.toLowerCase();

    for (const [symbol, addr] of Object.entries(BASE_TOKEN_WHITELIST)) {
        if (addr.toLowerCase() === lower) {
            return symbol;
        }
    }

    return null;
}

export async function handleCreateToken({ tx, logs, block, blockNumber }) {

    const method = tx.data?.slice(0, 10);

    if (!method || !CREATE_METHOD.has(method)) {
        return false;
    }

    let tokenAddress = null;
    let creator = null;
    let name = null;
    let symbol = null;
    let meta = null;

    let tax = 0;

    let baseAddress = null;
    let basePair = null;

    let firstBuy = null;
    let devTokenAmount = 0;

    for (const log of logs) {

        const topic = log.topics[0];

        if (topic === TOKEN_CREATED) {

            const decoded = ethers.AbiCoder.defaultAbiCoder().decode(
                [
                    "uint256",
                    "address",
                    "uint256",
                    "address",
                    "string",
                    "string",
                    "string"
                ],
                log.data
            );

            creator = decoded[1].toLowerCase();
            tokenAddress = decoded[3].toLowerCase();
            name = decoded[4];
            symbol = decoded[5];
            meta = decoded[6];

        }

        if (topic === TOKEN_QUOTE_SET) {

            const decoded = ethers.AbiCoder.defaultAbiCoder().decode(
                ["address", "address"],
                log.data
            );

            baseAddress = decoded[1].toLowerCase();
            basePair = getBasePair(baseAddress);

        }

        if (topic === TAX_SET) {

            const decoded = ethers.AbiCoder.defaultAbiCoder().decode(
                ["address", "uint256"],
                log.data
            );

            tax = Number(decoded[1]) / 100;

        }

        if (topic === TRANSFER) {

            const decoded = ethers.AbiCoder.defaultAbiCoder().decode(
                ["address", "address", "uint256"],
                log.data
            );

            const to = decoded[1].toLowerCase();

            if (to === creator) {
                devTokenAmount = decoded[2];
            }

        }

        if (topic === TOKEN_BOUGHT) {

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

            firstBuy = {
                buyer: decoded[2].toLowerCase(),
                amount: decoded[3],
                eth: decoded[4],
                fee: decoded[5],
                price: decoded[6]
            };

        }

    }

    if (!tokenAddress) return false;

    let description = null;
    let imageUrl = null;
    let websiteUrl = null;
    let telegramUrl = null;
    let twitterUrl = null;

    try {

        const metaJSON = await fetchIPFSMeta(meta);

        description = metaJSON?.description || null;

        let image = metaJSON?.image || null; // [ADDED]

        websiteUrl = metaJSON?.website || null;
        telegramUrl = metaJSON?.telegram || null;
        twitterUrl = metaJSON?.twitter || null;

        // ================= IMAGE PARSE =================
        if (image) {

            if (image.startsWith("ipfs://")) {

                imageUrl = image.replace(
                    "ipfs://",
                    "https://ipfs.io/ipfs/"
                );

            } else if (!image.startsWith("http")) {

                imageUrl = `https://ipfs.io/ipfs/${image}`;

            } else {

                imageUrl = image;

            }

        } else {

            imageUrl = null;

        }

    } catch (err) { }

    // ================= SAVE TOKEN =================

    try {

    console.log("[CREATE] inserting launch");

    await insertLaunch({

            launchTime: new Date(block.timestamp * 1000),

            tokenAddress,
            developer: creator,

            name,
            symbol,

            description,
            imageUrl,
            websiteUrl,
            telegramUrl,
            twitterUrl,

            supply: 1000000000,
            decimals: 18,

            taxBuy: tax,
            taxSell: tax,

            minBuy: 0,
            maxBuy: 0,

            basePair,
            baseAddress,

            networkCode: "BSC",
            sourceFrom: "flap.sh",

            migrated: false,
            verifiedCode: true

        });

    console.log("[CREATE] launch inserted");

    await insertTokenFlap({
        tokenAddress: tokenAddress.toLowerCase(),
        creator,
        blockNumber
    });

    console.log("[CREATE] token_flap inserted");

} catch (err) {

    console.error("[CREATE ERROR]");
    console.error(err);

}

    if (firstBuy) {

        const basePrice = await getBasePrice(basePair);

        const priceBase =
            Number(firstBuy.eth) / Number(firstBuy.amount - firstBuy.fee);

        const priceUSDT =
            priceBase * basePrice;

        const volumeUSDT =
            (Number(firstBuy.eth) / 1e18) * basePrice;

        const timestamp =
            new Date(block.timestamp * 1000);

        await updateHolderBalance(
            tokenAddress,
            firstBuy.buyer,
            firstBuy.amount
        );

        await updateHolderStats({
            tokenAddress,
            wallet: firstBuy.buyer,
            buyUsd: volumeUSDT,
            sellUsd: 0,
            buyBase: firstBuy.eth - firstBuy.fee,
            sellBase: 0,
            buyCount: 1,
            sellCount: 0
        });

        await updateCandleFromTrade({
            tokenAddress,
            time: new Date(block.timestamp * 1000),
            priceUSDT,                       // [MODIFIED]
            volumeUSDT, // [ADDED]
            amountReceive: firstBuy.amount,
            amountBasePayable: firstBuy.eth - firstBuy.fee
        });

    }

    console.log("\n================ FLAP CREATE DETECTED ================");
    console.log("Block Time          :", new Date(block.timestamp * 1000));
    console.log("TX Hash             :", tx.hash);

    console.log("\nTOKEN INFO");
    console.log("Token Address       :", tokenAddress);
    console.log("Creator             :", creator);
    console.log("Name                :", name);
    console.log("Symbol              :", symbol);

    console.log("\nTOKENOMICS");
    console.log("Tax Buy             :", tax, "%");
    console.log("Tax Sell            :", tax, "%");

    console.log("\nPAIR");
    console.log("Base Pair           :", basePair);
    console.log("Base Address        :", baseAddress);

    if (firstBuy) {

        console.log("\nFIRST TRADE");
        console.log("Buyer               :", firstBuy.buyer);
        console.log("Token Bought        :", firstBuy.amount.toString());

        const basePaid = firstBuy.eth - firstBuy.fee;

        console.log("Base Net Paid       :", basePaid.toString());

        const priceBase =
            Number(firstBuy.eth) / Number(firstBuy.amount - firstBuy.fee);

        if (basePair) {

            const basePrice = await getBasePrice(basePair);
            const priceUSDT = priceBase * basePrice;

            console.log("Base Price USD      :", basePrice);
            console.log("Token Price USD     :", priceUSDT);

            const volumeUSDT =
                (Number(firstBuy.eth) / 1e18) * basePrice;

            console.log("Volume USD          :", volumeUSDT);

        }

    }

    console.log("\nDEV DISTRIBUTION");
    console.log("Dev Token Received  :", devTokenAmount ? devTokenAmount.toString() : "0");

    console.log("\n======================================================\n");

    return true;

}