import { db } from "../infra/database.js";

export async function insertLaunch(data) {
  const query = `
    INSERT INTO launch_tokens (
      launch_time,
      token_address,
      developer_address,
      name,
      symbol,
      description,
      image_url,
      website_url,
      telegram_url,
      twitter_url,
      supply,
      decimals,
      tax_buy,
      tax_sell,
      min_buy,
      max_buy,
      base_pair,
      base_address,
      network_code,
      source_from,
      migrated,
      verified_code
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
      $11,$12,$13,$14,$15,$16,$17,$18,
      $19,$20,$21,$22
    )
    ON CONFLICT (token_address) DO NOTHING
  `;

  await db.query(query, [
    data.launchTime,
    data.tokenAddress,
    data.developer,
    data.name,
    data.symbol,
    data.description,
    data.imageUrl,
    data.websiteUrl,
    data.telegramUrl,
    data.twitterUrl,
    data.supply,
    data.decimals,
    data.taxBuy,
    data.taxSell,
    data.minBuy,
    data.maxBuy,
    data.basePair,
    data.baseAddress,
    data.networkCode,
    data.sourceFrom,
    data.migrated,
    data.verifiedCode
  ]);

  await db.query(`
INSERT INTO token_stats (
token_address
)
VALUES ($1)
ON CONFLICT (token_address) DO NOTHING
`, [data.tokenAddress]);
}

export async function getLaunchByToken(tokenAddress) {
  const query = `
    SELECT
      token_address,
      developer_address,
      base_pair,
      base_address,
      launch_time,
      migrated,
      migrated_time
    FROM launch_tokens
    WHERE token_address = $1
    LIMIT 1
  `;

  const { rows } = await db.query(query, [tokenAddress]);

  if (!rows.length) return null;

  return {
    tokenAddress: rows[0].token_address,
    developer:     rows[0].developer_address,
    basePair: rows[0].base_pair,
    baseAddress: rows[0].base_address,
    launchTime: rows[0].launch_time,
    migrated: rows[0].migrated,
    migratedTime: rows[0].migrated_time
  };
}

// ================= SET MIGRATED =================

export async function setTokenMigrated(tokenAddress, migratedTime) {
  const query = `
    UPDATE launch_tokens
    SET
      migrated = TRUE,
      migrated_time = $2
    WHERE token_address = $1
  `;

  await db.query(query, [
    tokenAddress,
    migratedTime || new Date()
  ]);
}