import { db } from "../infra/database.js";

export async function insertPairLiquidity(data) {
  const query = `
    INSERT INTO pair_liquidity (
      token_address,
      base_address,
      pair_address,
      base_symbol,
      liquidity_token,
      liquidity_base,
      block_number,
      tx_hash
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
  `;

  await db.query(query, [
    data.tokenAddress,
    data.baseAddress,
    data.pairAddress,
    data.baseSymbol,
    data.liquidityToken,
    data.liquidityBase,
    data.blockNumber,
    data.txHash
  ]);
}