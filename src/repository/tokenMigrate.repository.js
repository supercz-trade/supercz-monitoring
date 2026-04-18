import { db } from "../infra/database.js";


// ================= INSERT TOKEN MIGRATE =================

export async function insertTokenMigrate({
  tokenAddress,
  pairAddress,
  baseAddress,
  baseSymbol,
  blockNumber,
  txHash
}) {

  const query = `
    INSERT INTO token_migrate (
      token_address,
      pair_address,
      base_address,
      base_symbol,
      block_number,
      tx_hash
    )
    VALUES ($1,$2,$3,$4,$5,$6)
    ON CONFLICT (token_address) DO NOTHING
  `;

  try {

    await db.query(query, [
      tokenAddress,
      pairAddress,
      baseAddress,
      baseSymbol,
      blockNumber,
      txHash
    ]);

    console.log("[TOKEN_MIGRATE][INSERT]", tokenAddress);

  } catch (err) {

    console.error("[TOKEN_MIGRATE][INSERT ERROR]");
    console.error(err);

    throw err;

  }

}


// ================= LOAD TOKENS =================

export async function loadTokenMigrate() {

  const query = `
    SELECT token_address
    FROM token_migrate
  `;

  const { rows } = await db.query(query);

  return rows.map((r) => r.token_address.toLowerCase());

}

// ================= LOAD TOKEN + PAIR =================

export async function loadTokenMigrateWithPair() {

  const query = `
    SELECT
      token_address,
      pair_address,
      base_address,
      base_symbol
    FROM token_migrate
  `;

  const { rows } = await db.query(query);

  return rows.map((r) => ({
    tokenAddress : r.token_address.toLowerCase(),
    pairAddress  : r.pair_address.toLowerCase(),
    baseAddress  : r.base_address.toLowerCase(),
    baseSymbol   : r.base_symbol
  }));

}