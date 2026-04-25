import { ethers } from "ethers";
import { getContractFields } from "./rpcQueue.js";

// ================= HELPER3 ADDRESS =================
const HELPER3_ADDRESS = "0xF251F83e40a78868FcfA3FA4599Dad6494E46034";

// ================= ABI =================
const ABI = [
  "function getTokenInfo(address token) view returns (uint256 version, address tokenManager, address quote, uint256 lastPrice, uint256 tradingFeeRate, uint256 minTradingFee, uint256 launchTime, uint256 offers, uint256 maxOffers, uint256 funds, uint256 maxFunds, bool liquidityAdded)"
];

// ================= FETCH TOKEN INFO =================
// Menggunakan getContractFields agar routing provider + circuit breaker aktif.
// Caller tetap bisa pakai seperti biasa: const info = await getHelper3TokenInfo(addr)

export async function getHelper3TokenInfo(tokenAddress) {
  const result = await getContractFields({
    tokenInfo: (provider) => {
      const contract = new ethers.Contract(HELPER3_ADDRESS, ABI, provider);
      return contract.getTokenInfo(tokenAddress);
    }
  });
  return result.tokenInfo ?? null;
}