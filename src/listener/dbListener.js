import pkg from "pg";
import { ethers } from "ethers";
import { getContractFields } from "../infra/rpcQueue.js";

const { Client } = pkg;

// shared state (dipakai semua handler)
export const flapTokenSet   = new Set();
export const pairMap        = new Map();
export const pairAddresses  = [];

const PAIR_ABI = [
  "function token0() view returns(address)",
  "function token1() view returns(address)"
];

export async function startDBListener() {

  const client = new Client({
    connectionString: process.env.DATABASE_URL
  });

  await client.connect();

  await client.query("LISTEN token_flap_insert");
  await client.query("LISTEN token_migrate_insert");

  console.log("[DB LISTENER] ready");

  client.on("notification", async (msg) => {

    try {
      const payload = JSON.parse(msg.payload);

      // ================= FLAP TOKEN =================
      if (msg.channel === "token_flap_insert") {

        const token = payload.tokenAddress.toLowerCase();

        if (!flapTokenSet.has(token)) {
          flapTokenSet.add(token);
          console.log("[EVENT] FLAP token:", token);
        }

      }

      // ================= PAIR =================
      if (msg.channel === "token_migrate_insert") {

        const pair = payload.pairAddress.toLowerCase();

        if (pairMap.has(pair)) return;

        // Pakai getContractFields — routing EWMA + circuit breaker aktif
        const fields = await getContractFields({
          token0: (provider) => new ethers.Contract(pair, PAIR_ABI, provider).token0(),
          token1: (provider) => new ethers.Contract(pair, PAIR_ABI, provider).token1(),
        });

        if (!fields.token0 || !fields.token1) {
          console.warn("[DB LISTENER] failed to resolve pair tokens:", pair);
          return;
        }

        pairMap.set(pair, {
          tokenAddress: payload.tokenAddress.toLowerCase(),
          baseAddress:  payload.baseAddress.toLowerCase(),
          baseSymbol:   payload.baseSymbol,
          token0:       fields.token0.toLowerCase(),
          token1:       fields.token1.toLowerCase(),
        });

        pairAddresses.push(pair);

        console.log("[EVENT] PAIR:", pair);
      }

    } catch (err) {
      console.error("[DB LISTENER ERROR]", err.message);
    }

  });

}