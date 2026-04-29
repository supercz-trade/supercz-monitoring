// ===============================================================
// holderSync.service.js
// ===============================================================

// [ADDED] import dependencies
import { db } from "../infra/database.js";
import { Interface, JsonRpcProvider, Contract } from "ethers";

// [ADDED] CONFIG
const RPC_URL = process.env.RPC_MULTICALL;
const MULTICALL_ADDRESS = "0xca11bde05977b3631167028862be2a173976ca11".toLowerCase();
const CHUNK_SIZE = 100; // [ADDED] max address per multicall
const TOP_HOLDER_LIMIT = 200; // [ADDED] max holder per token
const STALE_MINUTES = 2; // [ADDED] skip recently updated

// [ADDED] provider init
const provider = new JsonRpcProvider(RPC_URL);

// [ADDED] ERC20 ABI
const erc20Iface = new Interface([
    "function balanceOf(address) view returns (uint256)"
]);

// [ADDED] Multicall ABI
const multicall = new Contract(
    MULTICALL_ADDRESS,
    [
        "function aggregate(tuple(address target, bytes callData)[] calls) public view returns (uint256 blockNumber, bytes[] returnData)"
    ],
    provider
);

// ===============================================================
// HELPERS
// ===============================================================

// [ADDED] chunk array
function chunkArray(arr, size) {
    const chunks = [];
    for (let i = 0; i < arr.length; i += size) {
        chunks.push(arr.slice(i, i + size));
    }
    return chunks;
}

// ===============================================================
// FETCH BALANCES VIA MULTICALL
// ===============================================================

// [ADDED] multicall balance fetch
async function fetchBalances(tokenAddress, holders) {

    const calls = holders.map(addr => ({
        target: tokenAddress,
        callData: erc20Iface.encodeFunctionData("balanceOf", [addr])
    }));

    const [, returnData] = await multicall.aggregate(calls);

    return returnData.map((data, i) => ({
        holder: holders[i],
        balance: erc20Iface.decodeFunctionResult("balanceOf", data)[0].toString()
    }));
}

// ===============================================================
// GET TOP HOLDERS (WITH STALENESS)
// ===============================================================

// [ADDED] get top holders from DB
async function getTopHolders(tokenAddress) {

    const { rows } = await db.query(`
    SELECT holder_address, balance, last_updated
    FROM token_holders
    WHERE LOWER(token_address) = LOWER($1)
      AND balance > 0
      AND (
        last_updated IS NULL
        OR last_updated < NOW() - INTERVAL '${STALE_MINUTES} minutes'
      )
    ORDER BY balance DESC
    LIMIT $2
  `, [tokenAddress, TOP_HOLDER_LIMIT]);

    return rows;
}

// ===============================================================
// UPDATE DB (DIRTY ONLY)
// ===============================================================

// [ADDED] batch update only changed balances
async function updateBalances(tokenAddress, updates) {

    if (!updates.length) return;

    const values = updates
        .map((u, i) => `($${i * 2 + 2}, $${i * 2 + 3})`)
        .join(",");

    const params = [tokenAddress];

    updates.forEach(u => {
        params.push(u.holder);
        params.push(u.balance);
    });

    await db.query(`
    UPDATE token_holders th
    SET balance = v.balance::numeric,
        last_updated = NOW()
    FROM (VALUES ${values}) AS v(address, balance)
    WHERE LOWER(th.token_address) = LOWER($1)
      AND LOWER(th.holder_address) = LOWER(v.address)
  `, params);
}

// ===============================================================
// SYNC ONE TOKEN
// ===============================================================

// [MODIFIED] optimized version
export async function syncTokenHolders(tokenAddress) {

    try {

        const holders = await getTopHolders(tokenAddress);
        if (!holders.length) return;

        // [ADDED] map for O(1) lookup
        const holderMap = new Map(
            holders.map(h => [h.holder_address.toLowerCase(), h])
        );

        const addresses = holders.map(h => h.holder_address);
        const chunks = chunkArray(addresses, CHUNK_SIZE);

        const updates = [];

        const DECIMALS = 18; // [ADDED] TODO: ambil dari DB kalau mau dynamic

        for (const chunk of chunks) {

            const results = await fetchBalances(tokenAddress, chunk);

            for (const r of results) {

                const dbHolder = holderMap.get(r.holder.toLowerCase()); // [MODIFIED]

                if (!dbHolder) continue;

                const oldBalance = Number(dbHolder.balance || 0);
                const newBalance = Number(r.balance) / (10 ** DECIMALS); // [MODIFIED]

                if (Math.abs(oldBalance - newBalance) > 1e-12) {
                    updates.push({
                        holder: r.holder,
                        balance: newBalance.toString()
                    });
                }

            }
        }

        await updateBalances(tokenAddress, updates);

        console.log(`[HOLDER SYNC] ${tokenAddress} updated: ${updates.length}`);

    } catch (err) {
        console.error("[HOLDER SYNC ERROR]", tokenAddress, err.message);
    }

}

// ===============================================================
// SYNC ALL TOKENS (ENTRY POINT)
// ===============================================================

// [ADDED] main sync loop
// [MODIFIED] add concurrency limit
import pLimit from "p-limit";

export async function syncAllTokens() {

    try {

        const { rows } = await db.query(`
          SELECT lt.token_address
          FROM launch_tokens lt
          JOIN token_stats ts
            ON ts.token_address = lt.token_address
          WHERE ts.marketcap > 10000
          ORDER BY ts.marketcap DESC
        `);

        const limit = pLimit(3); // [ADDED] max 3 token parallel

        await Promise.all(
            rows.map(r =>
                limit(() => syncTokenHolders(r.token_address))
            )
        );

    } catch (err) {
        console.error("[HOLDER SYNC ALL ERROR]", err.message);
    }

}