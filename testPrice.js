import { ethers } from "ethers";
import "dotenv/config";

// ===== ENV =====
const RPC = process.env.BSC_RPC;
if (!RPC) {
  console.error("BSC_RPC env missing");
  process.exit(1);
}

// ===== HELPER CONTRACT =====
const HELPER3 = "0xF251F83e40a78868FcfA3FA4599Dad6494E46034";

const ABI = [
  "function getTokenInfo(address token) view returns (uint256,address,address,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,bool)"
];

const provider = new ethers.JsonRpcProvider(RPC);
const helper = new ethers.Contract(HELPER3, ABI, provider);

// ===== MAIN =====
async function main() {
  const token = process.argv[2];

  if (!token) {
    console.log("Usage: node testGetTokenInfo.js <TOKEN_ADDRESS>");
    process.exit(0);
  }

  try {
    const result = await helper.getTokenInfo(token);

    console.log("Raw Result:");
    console.log(result);

    console.log("\nDecoded Fields:");
    console.log({
      field0_uint256: result[0].toString(),
      field1_address: result[1],
      field2_address: result[2],
      field3_uint256: result[3].toString(),
      field4_uint256: result[4].toString(),
      field5_uint256: result[5].toString(),
      field6_uint256: result[6].toString(),
      field7_uint256: result[7].toString(),
      field8_uint256: result[8].toString(),
      field9_uint256: result[9].toString(),
      field10_uint256: result[10].toString(),
      field11_bool: result[11]
    });

  } catch (err) {
    console.error("Error calling getTokenInfo:", err.message);
  }

  process.exit(0);
}

main();