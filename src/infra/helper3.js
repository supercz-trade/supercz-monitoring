import { ethers } from "ethers";
import { rpcTxProvider } from "./provider.js";

// ================= HELPER3 ADDRESS =================
const HELPER3_ADDRESS = "0xF251F83e40a78868FcfA3FA4599Dad6494E46034";

// ================= ABI =================
const ABI = [
  "function getTokenInfo(address token) view returns (uint256 version, address tokenManager, address quote, uint256 lastPrice, uint256 tradingFeeRate, uint256 minTradingFee, uint256 launchTime, uint256 offers, uint256 maxOffers, uint256 funds, uint256 maxFunds, bool liquidityAdded)"
];

// ================= CONTRACT INSTANCE =================
export const helper = new ethers.Contract(
  HELPER3_ADDRESS,
  ABI,
  rpcTxProvider
);