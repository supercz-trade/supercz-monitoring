import { db } from "../infra/database.js";

export async function insertTokenFlap({
  tokenAddress,
  creator,
  blockNumber
}) {

  const query = `
    INSERT INTO token_flap (
      token_address,
      creator,
      block_number
    )
    VALUES ($1,$2,$3)
    ON CONFLICT (token_address) DO NOTHING
  `;

  try {

    await db.query(query, [
      tokenAddress.toLowerCase(),
      creator?.toLowerCase() || null,
      blockNumber
    ]);

    console.log("[TOKEN_FLAP][INSERT]", tokenAddress);

  } catch (err) {

    console.error("[TOKEN_FLAP][INSERT ERROR]");
    console.error("token:", tokenAddress);
    console.error(err);

    throw err;

  }

}


// ================= LOAD TOKENS =================

export async function loadTokenFlap() {

  const query = `
    SELECT token_address
    FROM token_flap
  `;

  try {

    const { rows } = await db.query(query);

    const tokens = rows.map(r => r.token_address.toLowerCase());

    console.log("[TOKEN_FLAP][LOAD]", tokens.length, "tokens");

    return tokens;

  } catch (err) {

    console.error("[TOKEN_FLAP][LOAD ERROR]");
    console.error(err);

    throw err;

  }

}


// ================= CHECK TOKEN =================

export async function getTokenFlap(tokenAddress) {

  const query = `
    SELECT token_address
    FROM token_flap
    WHERE token_address = $1
    LIMIT 1
  `;

  try {

    const { rows } = await db.query(query, [
      tokenAddress.toLowerCase()
    ]);

    return rows.length ? rows[0] : null;

  } catch (err) {

    console.error("[TOKEN_FLAP][GET ERROR]");
    console.error("token:", tokenAddress);
    console.error(err);

    throw err;

  }

}

// ================= DELETE TOKEN =================

export async function deleteTokenFlap(tokenAddress) {

  const query = `
    DELETE FROM token_flap
    WHERE token_address = $1
  `;

  try {

    const { rowCount } = await db.query(query, [
      tokenAddress.toLowerCase()
    ]);

    if (rowCount > 0) {

      console.log("[TOKEN_FLAP][DELETE]", tokenAddress);

      return true;

    }

    console.log("[TOKEN_FLAP][DELETE MISS]", tokenAddress);

    return false;

  } catch (err) {

    console.error("[TOKEN_FLAP][DELETE ERROR]");
    console.error("token:", tokenAddress);
    console.error(err);

    throw err;

  }

}

export async function deleteManyTokenFlap(tokens) {

  const query = `
    DELETE FROM token_flap
    WHERE token_address = ANY($1)
  `;

  await db.query(query, [tokens]);

}