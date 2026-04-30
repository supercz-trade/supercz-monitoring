import { db } from "../infra/database.js";
import { getLiquidityStateCache } from "../cache/liquidity.cache.js";

const _cache = new Map();

function cacheGet(key) {
  const entry = _cache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    _cache.delete(key);
    return null;
  }
  return entry.value;
}

function cacheSet(key, value, ttlMs = 10_000) {
  _cache.set(key, { value, expiresAt: Date.now() + ttlMs });
}

async function calcTokenStats(addresses) {

  const [statsResult, holderResult, devMarkResult] = await Promise.all([
    db.query(`
      SELECT token_address, price_usdt, marketcap, volume_usdt, tx_count,
             holder_count, dev_supply, top_holder_supply, paperhand_pct
      FROM token_stats
      WHERE token_address = ANY($1)
    `, [addresses]),

    db.query(`
      SELECT token_address, holder_address, balance
      FROM (
        SELECT token_address, holder_address, balance,
               ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY balance DESC) as rn
        FROM token_holders th
        WHERE token_address = ANY($1)
      ) ranked
      WHERE rn <= 10
    `, [addresses]),

    db.query(`
      SELECT tt.token_address,
             COALESCE(th.balance, 0) AS dev_balance,
             COUNT(*) FILTER (WHERE tt.position = 'BUY') AS buy_count,
             COUNT(*) FILTER (WHERE tt.position = 'SELL') AS sell_count
      FROM token_transactions tt
      JOIN launch_tokens lt
        ON LOWER(lt.token_address) = LOWER(tt.token_address)
       AND LOWER(lt.developer_address) = LOWER(tt.address_message_sender)
      LEFT JOIN token_holders th
        ON LOWER(th.token_address) = LOWER(tt.token_address)
       AND LOWER(th.holder_address) = LOWER(lt.developer_address)
      WHERE tt.token_address = ANY($1)
        AND tt.is_dev = true
      GROUP BY tt.token_address, th.balance
    `, [addresses]),
  ]);

  const statsMap = {};
  for (const r of statsResult.rows) {
    statsMap[r.token_address] = {
      priceUsdt: Number(r.price_usdt || 0),
      marketCap: Number(r.marketcap || 0),
      volumeUsdt: Number(r.volume_usdt || 0),
      txCount: Number(r.tx_count || 0),
      holderCount: Number(r.holder_count || 0),
      devSupply: Number(r.dev_supply || 0),
      top10Supply: Number(r.top_holder_supply || 0),
      paperPct: Number(r.paperhand_pct || 0),
    };
  }

  const holderMap = {};
  for (const h of holderResult.rows) {
    if (!holderMap[h.token_address]) holderMap[h.token_address] = [];
    holderMap[h.token_address].push(h);
  }

  const holderCountMap = {};
  const paperMap = {};
  for (const addr of addresses) {
    holderCountMap[addr] = statsMap[addr]?.holderCount || 0;
    paperMap[addr] = statsMap[addr]?.paperPct || 0;
  }

  const devMarkMap = {};
  for (const r of devMarkResult.rows) {
    const buy = Number(r.buy_count || 0);
    const sell = Number(r.sell_count || 0);
    const bal = Number(r.dev_balance || 0);

    let mark = "DH";
    if (sell > 0 && bal < 1) mark = "DS";
    else if (sell > 0 && buy > sell) mark = "DB";
    else if (sell > 0) mark = "DP";
    else if (buy > 1) mark = "DB";

    devMarkMap[r.token_address] = mark;
  }

  return { statsMap, holderMap, holderCountMap, paperMap, devMarkMap };
}

//
// ✅ KEEP (tidak dihapus)
async function get24hChange(tokenAddress) {
  const { rows } = await db.query(`
    SELECT close
    FROM token_candles
    WHERE LOWER(token_address) = LOWER($1)
      AND timeframe = '1h'
    ORDER BY start_time DESC
    LIMIT 24
  `, [tokenAddress]);

  if (rows.length < 2) return 0;

  const current = Number(rows[0].close || 0);
  const past = Number(rows[rows.length - 1].close || 0);

  if (!past) return 0;

  return ((current - past) / past) * 100;
}

//
// 🔥 [ADDED] batch version (dipakai sekarang)
async function get24hChangeBatch(addresses) {

  const { rows } = await db.query(`
    SELECT token_address, close
    FROM token_candles
    WHERE token_address = ANY($1)
      AND timeframe = '1h'
    ORDER BY token_address, start_time DESC
  `, [addresses]);

  const map = {};

  for (const r of rows) {
    if (!map[r.token_address]) map[r.token_address] = [];
    if (map[r.token_address].length < 24) {
      map[r.token_address].push(Number(r.close));
    }
  }

  const result = {};

  for (const addr of addresses) {
    const arr = map[addr] || [];

    if (arr.length < 2) {
      result[addr] = 0;
      continue;
    }

    const current = arr[0];
    const past = arr[arr.length - 1];

    result[addr] = past ? ((current - past) / past) * 100 : 0;
  }

  return result;
}

//
// 🔧 MODIFIED (tidak dihapus)
async function buildTokenResponse(t, maps, changeMap) {

  const { statsMap, holderMap, holderCountMap, paperMap, devMarkMap } = maps;

  const liq = getLiquidityStateCache(t.token_address);
  const supply = Number(t.supply || 0);
  const stats = statsMap[t.token_address] || {};

  const change24h = changeMap
    ? (changeMap[t.token_address] || 0)
    : await get24hChange(t.token_address); // fallback

  return {
    tokenAddress: t.token_address,
    name: t.name,
    symbol: t.symbol,

    priceUsdt: stats.priceUsdt || 0,
    marketCap: stats.marketCap || 0,
    priceChange24h: change24h,

    volumeUsdt: stats.volumeUsdt || 0,
    txCount: stats.txCount || 0,
    holderCount: holderCountMap[t.token_address] || 0,

    devMark: devMarkMap[t.token_address] || "DH",
  };
}

//
// 🚀 endpoint (pakai batch, tapi function lama tetap ada)
//
export async function getNewTokens(req, reply) {
  try {

    const { rows } = await db.query(`
      SELECT *
      FROM launch_tokens
      WHERE migrated = false
      ORDER BY launch_time DESC
      LIMIT 50
    `);

    const addresses = rows.map(r => r.token_address);

    const [maps, changeMap] = await Promise.all([
      calcTokenStats(addresses),
      get24hChangeBatch(addresses)
    ]);

    const tokens = await Promise.all(
      rows.map(t => buildTokenResponse(t, maps, changeMap))
    );

    return tokens;

  } catch (err) {
    console.error("[NEW TOKENS ERROR]", err);
    return reply.code(500).send({ error: "failed" });
  }
}

export async function getTokenInfo(req, reply) {

  try {

    const { address } = req.params;

    const tokenQuery = await db.query(`
      SELECT * FROM launch_tokens
      WHERE LOWER(token_address) = LOWER($1)
      LIMIT 1
    `, [address]);

    const token = tokenQuery.rows[0];
    if (!token) return reply.code(404).send({ error: "Token not found" });

    const addr = token.token_address;
    const maps = await calcTokenStats([addr]);

    const stats = maps.statsMap[addr] || { priceUsdt: 0, marketCap: 0, volumeUsdt: 0, txCount: 0 };
    const supply = Number(token.supply || 0);
    const liq = getLiquidityStateCache(addr);
    const change24h = await get24hChange(addr);
    const topHolders = (maps.holderMap[addr] || []).map((h, i) => {
      const balance = Number(h.balance || 0);
      const pct = supply > 0
        ? parseFloat(((balance / supply) * 100).toFixed(2))
        : 0;
      return {
        rank: i + 1,
        address: h.holder_address,
        balance,
        pct,
        isDev: h.holder_address.toLowerCase() === (token.developer_address || "").toLowerCase()
      };
    });

    const devHolder = topHolders.find(h => h.isDev);
    const devHoldPct = devHolder ? devHolder.pct : 0;
    const mode = liq?.mode || (token.migrated ? "dex" : "bonding");

    return {
      launchTime: token.launch_time,
      tokenAddress: token.token_address,
      developerAddress: token.developer_address || null,
      name: token.name,
      symbol: token.symbol,

      basePair: liq?.base_symbol || token.base_pair || null,
      baseAddress: token.base_address || null,

      description: token.description,
      imageUrl: token.image_url,
      sourceFrom: token.source_from,

      website: token.website_url,
      telegram: token.telegram_url,
      twitter: token.twitter_url,

      totalSupply: supply,
      decimals: Number(token.decimals || 18),

      priceUsdt: stats.priceUsdt,
      marketCap: stats.marketCap,
      priceChange24h: change24h, // [ADDED] 
      volumeUsdt: stats.volumeUsdt,
      txCount: stats.txCount,
      holderCount: maps.holderCountMap[addr] || 0,

      tax: {
        buy: token.tax_buy,
        sell: token.tax_sell
      },

      mode,
      platform: liq?.platform || token.source_from || null,

      ...(mode !== "dex" && {
        progress: liq?.progress
          ? Number((liq.progress * 100).toFixed(2))
          : 0,
        targetUSD: liq?.target || 0,
        bondingLiquidity: {
          base: liq?.bonding_base || 0,
          usd: liq?.bonding_usd || 0
        }
      }),

      ...(mode === "dex" && {
        liquidity: {
          base: liq?.base_liquidity || 0,
          usd: liq?.liquidity_usd || 0
        }
      }),

      migrated: token.migrated,
      migratedTime: token.migrated_time,

      devMark: maps.devMarkMap[addr] || "DH",

      holderStats: {
        devHoldPct,
        paperHandPct: maps.paperMap[addr] || 0,
        top10: topHolders
      }
    };

  } catch (err) {
    console.error("[TOKEN INFO ERROR]", err.message);
    return reply.code(500).send({ error: "failed_to_fetch_token_info" });
  }

}

export async function getTokensMigrating(req, reply) {

  try {

    const limit = Math.min(Math.max(parseInt(req.query?.limit) || 50, 1), 500);
    const cacheKey = `tokens_migrating_${limit}`;

    const cached = cacheGet(cacheKey);
    if (cached) return cached;

    const { rows } = await db.query(`
      SELECT lt.*
      FROM launch_tokens lt
      JOIN token_stats ts ON ts.token_address = lt.token_address
      WHERE lt.migrated = false
        AND ts.marketcap > 11000
      ORDER BY ts.marketcap DESC
      LIMIT $1
    `, [limit]);

    if (!rows.length) return [];

    const addresses = rows.map(r => r.token_address);
    const maps = await calcTokenStats(addresses);
    const tokens = await Promise.all(
  rows.map(t => buildTokenResponse(t, maps))
);

    cacheSet(cacheKey, tokens, 10_000);

    return tokens;

  } catch (err) {
    console.error("[TOKEN MIGRATING ERROR]", err);
    return reply.code(500).send({ error: "failed_to_fetch_migrating_tokens" });
  }

}

export async function getTokensMigrated(req, reply) {

  try {

    const limit = Math.min(Math.max(parseInt(req.query?.limit) || 50, 1), 500);
    const cacheKey = `tokens_migrated_${limit}`;

    const cached = cacheGet(cacheKey);
    if (cached) return cached;

    const { rows } = await db.query(`
      SELECT *
      FROM launch_tokens
      WHERE migrated = true
      ORDER BY migrated_time DESC
      LIMIT $1
    `, [limit]);

    if (!rows.length) return [];

    const addresses = rows.map(r => r.token_address);
    const maps = await calcTokenStats(addresses);
    const tokens = await Promise.all(
  rows.map(t => buildTokenResponse(t, maps))
);

    cacheSet(cacheKey, tokens, 10_000);

    return tokens;

  } catch (err) {
    console.error("[TOKEN MIGRATED ERROR]", err);
    return reply.code(500).send({ error: "failed_to_fetch_migrated_tokens" });
  }

}