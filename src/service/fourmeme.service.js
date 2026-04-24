import { fetchFourMemeData } from "../infra/http.js";
import { insertLaunch } from "../repository/launch.repository.js";

export async function processLaunch(tokenAddress, options = {}) {
  let response = null;

  for (let i = 0; i < 5; i++) {
    response = await fetchFourMemeData(tokenAddress);

    if (response && response.code === 0 && response.data) break;

    console.log(`API not ready for ${tokenAddress}, retry ${i + 1}`);
    await new Promise((res) => setTimeout(res, 1500));
  }

  if (!response || response.code !== 0 || !response.data) {
    console.log("❌ API failed after retry:", tokenAddress);
    return null;
  }

  const data = response.data;

  // ================= FLEXIBLE LAUNCH TIME =================
  const launchTime =
    options.onchainLaunchTime ||
    new Date(Number(data.launchTime)); // fallback API

  // Normalize basePair — BNB_MPC / BNB_GAS / varian lain → BNB
  const _rawPair = (data.symbol || "").toUpperCase();
  const basePair = _rawPair.startsWith("BNB") ? "BNB" : _rawPair;

  // Ambil baseAddress dari whitelist, fallback ke native BNB
  const BASE_ADDRESS = {
    BNB: "0x0000000000000000000000000000000000000000",
    USDT: "0x55d398326f99059ff775485246999027b3197955",
    USD1: "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d",
    USDC: "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
    CAKE: "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82",
    ASTER: "0x000Ae314E2A2172a039B26378814C252734f556A",
    U: "0xcE24439F2D9C6a2289F741120FE202248B666666",
    币安人生: "0x924fa68a0FC644485b8df8AbfA0A41C2e7744444",
    FORM: "0x5b73A93b4E5e4f1FD27D8b3F8C97D69908b5E284",
    UUSD: "0x61a10e8556bed032ea176330e7f17d6a12a10000",
  };
  const baseAddress = BASE_ADDRESS[basePair];

  const priceInBasePair = Number(data.tokenPrice.price);

  await insertLaunch({
    launchTime,
    tokenAddress,
    developer: data.userAddress,
    name: data.name,
    symbol: data.shortName,
    description: data.descr || null,
    imageUrl: data.image,
    websiteUrl: data.webUrl || null,
    telegramUrl: data.telegramUrl || null,
    twitterUrl: data.twitterUrl || null,
    supply: data.totalAmount,
    decimals: 18,
    taxBuy: data.taxInfo?.feeRate || 0,
    taxSell: data.taxInfo?.feeRate || 0,
    minBuy: data.minBuy || 0,
    maxBuy: data.maxBuy || 0,
    basePair,
    baseAddress,
    networkCode: data.networkCode,
    sourceFrom: "four_meme",
    migrated: false,
    verifiedCode: true
  });

  console.log("✅ Inserted:", tokenAddress);

  return {
    tokenAddress,
    developer: data.userAddress,
    name: data.name,
    symbol: data.shortName,
    basePair,
    baseAddress,
    priceInBasePair,
    taxBuy: data.taxInfo?.feeRate || 0,
    taxSell: data.taxInfo?.feeRate || 0,
    imageUrl: data.image || null,
    description: data.descr || null,
    websiteUrl: data.webUrl || null,
    telegramUrl: data.telegramUrl || null,
    twitterUrl: data.twitterUrl || null,
  };
}