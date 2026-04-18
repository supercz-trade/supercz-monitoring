import { ethers } from "ethers";

import { getLaunchByToken, insertLaunch } from "../repository/launch.repository.js";
import { insertTransaction } from "../repository/transaction.repository.js";

import { updateHolderBalance } from "../repository/holder.repository.js";
import { updateHolderStats } from "../repository/holderStats.repository.js";
import { insertTokenFlap } from "../repository/tokenFlap.repository.js";
import { deleteTokenFlap } from "../repository/tokenFlap.repository.js";

import { insertTokenMigrate } from "../repository/tokenMigrate.repository.js"; // [ADDED]
import { insertPairLiquidity } from "../repository/pairLiquidity.repository.js"; // [ADDED]
import { setTokenMigrated } from "../repository/launch.repository.js"; // [ADDED]

import { updateCandleFromTrade } from "../service/candle.service.js";
import { fetchIPFSMeta } from "../service/ipfsMeta.service.js";

import { getBasePrice } from "../price/binancePrice.js";

import { getBasePair, normalizeBaseAddress } from "../utils/baseToken.js";


const TRADE_METHOD = new Set([
  "0x0ba6324e",
  "0x2e2fdbd9",
  "0x64fd8b9e",
  "0x9f0bb8a9"
]);


const TOKEN_BOUGHT =
  ethers.id("TokenBought(uint256,address,address,uint256,uint256,uint256,uint256)");

const TOKEN_SOLD =
  ethers.id("TokenSold(uint256,address,address,uint256,uint256,uint256,uint256)");

const LAUNCHED_TO_DEX =
  ethers.id("LaunchedToDEX(address,address,uint256,uint256)");



const TOKEN_ABI = [
  "function name() view returns (string)",
  "function symbol() view returns (string)",
  "function decimals() view returns (uint8)",
  "function metaURI() view returns (string)",
  "function QUOTE_TOKEN() view returns (address)",
  "function taxRate() view returns (uint256)"
];


export async function handleBuySell({
  provider,
  tx,
  logs,
  block,
  blockNumber
}) {

  const method = tx.data?.slice(0, 10);

  if (TRADE_METHOD.has(method)) return false;


  let trade = null;
  let position = null;

  let addLiquidity = null; // [ADDED]


  for (const log of logs) {

    const topic = log.topics[0];

    // ================= BUY =================

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

      trade = {
        token: decoded[1].toLowerCase(),
        wallet: decoded[2].toLowerCase(),
        amount: decoded[3],
        base: decoded[4],
        fee: decoded[5]
      };

      position = "BUY";
    }

    // ================= SELL =================

    if (topic === TOKEN_SOLD) {

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

      trade = {
        token: decoded[1].toLowerCase(),
        wallet: decoded[2].toLowerCase(),
        amount: decoded[3],
        base: decoded[4],
        fee: decoded[5]
      };

      position = "SELL";
    }

    // ================= ADD LIQ =================

    if (topic === LAUNCHED_TO_DEX) {

      const decoded = ethers.AbiCoder.defaultAbiCoder().decode(
        [
          "address",
          "address",
          "uint256",
          "uint256"
        ],
        log.data
      );

      addLiquidity = { // [ADDED]
        tokenAddress: decoded[0].toLowerCase(),
        pairAddress: decoded[1].toLowerCase(),
        tokenAmount: decoded[2],
        baseAmount: decoded[3]
      };

    }

  }


  if (!trade && !addLiquidity) return false;


  let launchInfo = await getLaunchByToken(trade?.token || addLiquidity.tokenAddress);


  // ================= TOKEN AUTO REGISTER =================

  if (!launchInfo && trade) {

    const token = new ethers.Contract(
      trade.token,
      TOKEN_ABI,
      provider
    );

    let name = null;
    let symbol = null;
    let decimals = 18;
    let metaURI = null;
    let quoteToken = null;
    let taxRate = 0;

    try { name = await token.name(); } catch {}
    try { symbol = await token.symbol(); } catch {}
    try { decimals = await token.decimals(); } catch {}
    try { metaURI = await token.metaURI(); } catch {}
    try { quoteToken = await token.QUOTE_TOKEN(); } catch {}
    try { taxRate = await token.taxRate(); } catch {}

    const baseAddress = normalizeBaseAddress(quoteToken);
    const basePair = getBasePair(baseAddress);

    if (!basePair) {

      console.log("\n[SKIP] Unknown base token");
      console.log("Token :", trade.token);
      console.log("Quote :", quoteToken);
      console.log("TX    :", tx.hash);

      return false;

    }

    const metaJSON = await fetchIPFSMeta(metaURI);

    let imageUrl = null;

    if (metaJSON?.image) {

      if (metaJSON.image.startsWith("ipfs://")) {
        imageUrl = metaJSON.image.replace("ipfs://", "https://ipfs.io/ipfs/");
      } else {
        imageUrl = `https://ipfs.io/ipfs/${metaJSON.image}`;
      }

    }

    await insertTokenFlap({
      tokenAddress: trade.token,
      creator: tx.from,
      blockNumber
    });

    await insertLaunch({

      launchTime: new Date(block.timestamp * 1000),

      tokenAddress: trade.token,
      developer: tx.from,

      name,
      symbol,

      description: metaJSON?.description || null,
      imageUrl,

      websiteUrl: metaJSON?.website || null,
      telegramUrl: metaJSON?.telegram || null,
      twitterUrl: metaJSON?.twitter || null,

      supply: 1000000000,
      decimals,

      taxBuy: Number(taxRate) / 100,
      taxSell: Number(taxRate) / 100,

      minBuy: 0,
      maxBuy: 0,

      basePair,
      baseAddress,

      networkCode: "BSC",
      sourceFrom: "flap.sh",

      migrated: false,
      verifiedCode: false

    });

    launchInfo = await getLaunchByToken(trade.token);

  }



  // ================= BUY / SELL =================

  if (trade) {

    const basePrice = await getBasePrice(launchInfo.basePair);

    const basePaid =
      Number(trade.base) - Number(trade.fee);

    const tokenAmount =
      Number(ethers.formatUnits(trade.amount, 18));

    const priceBase =
      Number(trade.base) / Number(trade.amount);

    const priceUSDT =
      priceBase * basePrice;

    const volumeUSDT =
      (Number(trade.base) / 1e18) * basePrice;


    console.log("\n================ FLAP TRADE DETECTED =================");
    console.log("TX:", tx.hash);
    console.log("TOKEN:", trade.token);
    console.log("WALLET:", trade.wallet);
    console.log("POSITION:", position);
    console.log("PRICE USD:", priceUSDT);
    console.log("VOLUME USD:", volumeUSDT);
    console.log("======================================================\n");


    await insertTransaction({

      tokenAddress: trade.token,

      time: new Date(block.timestamp * 1000),

      blockNumber,

      txHash: tx.hash,

      position,

      amountReceive: tokenAmount,

      basePayable: launchInfo.basePair,

      amountBasePayable: basePaid,

      inUSDTPayable: volumeUSDT,

      priceBase,
      priceUSDT,

      addressMessageSender: trade.wallet,

      isDev: trade.wallet === launchInfo.developer_address

    });


    if (position === "BUY") {

      await updateHolderBalance(
        trade.token,
        trade.wallet,
        trade.amount
      );

    } else {

      await updateHolderBalance(
        trade.token,
        trade.wallet,
        -trade.amount
      );

    }


    await updateHolderStats({

      tokenAddress: trade.token,
      wallet: trade.wallet,

      buyUsd: position === "BUY" ? volumeUSDT : 0,
      sellUsd: position === "SELL" ? volumeUSDT : 0,

      buyBase: position === "BUY" ? basePaid : 0,
      sellBase: position === "SELL" ? basePaid : 0,

      buyCount: position === "BUY" ? 1 : 0,
      sellCount: position === "SELL" ? 1 : 0

    });


    await updateCandleFromTrade({

      tokenAddress: trade.token,

      time: new Date(block.timestamp * 1000),

      priceUSDT,
      volumeUSDT,

      amountReceive: tokenAmount,
      amountBasePayable: basePaid

    });

  }



  // ================= ADD LIQUIDITY =================

  if (addLiquidity) {

    const basePrice = await getBasePrice(launchInfo.basePair);

    const tokenAmount =
      Number(ethers.formatUnits(addLiquidity.tokenAmount, 18));

    const baseAmount =
      Number(ethers.formatUnits(addLiquidity.baseAmount, 18));

    const priceBase =
      baseAmount / tokenAmount;

    const priceUSDT =
      priceBase * basePrice;

    const volumeUSDT =
      baseAmount * basePrice;

    const tokenAddress = addLiquidity.tokenAddress;
    const pairAddress = addLiquidity.pairAddress;

    const baseAddress = launchInfo.baseAddress;
    const baseSymbol = launchInfo.basePair;


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

      position: "ADD_LIQUIDITY",

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


    await updateHolderBalance(
      tokenAddress,
      pairAddress,
      addLiquidity.tokenAmount
    );


    await updateCandleFromTrade({

      tokenAddress,

      time: new Date(block.timestamp * 1000),

      priceUSDT,
      volumeUSDT,

      amountReceive: tokenAmount,
      amountBasePayable: baseAmount

    });


    await deleteTokenFlap(tokenAddress);

  }


  return true;

}