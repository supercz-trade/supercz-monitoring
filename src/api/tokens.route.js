// ===============================================================
// tokens.route.js
// ===============================================================

import { db } from "../infra/database.js";
import { getLiquidityStateCache } from "../cache/liquidity.cache.js";

// ===============================================================
// SIMPLE MEMORY CACHE
// ===============================================================

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

// ===============================================================
// HELPER — hitung stats dari transaksi & holder
// ===============================================================

async function calcTokenStats(addresses) {

  const [statsResult, holderResult, devMarkResult] = await Promise.all([

    db.query(`
      SELECT
        token_address,
        price_usdt,
        marketcap,
        volume_usdt,
        tx_count,
        holder_count,
        dev_supply,
        top_holder_supply,
        paperhand_pct
      FROM token_stats
      WHERE token_address = ANY($1)
    `, [addresses]),

    // [MODIFIED] tambah EXISTS filter — exclude relay/aggregator/LP contract
    // yang tidak punya transaksi BUY/SELL di token_transactions
    db.query(`
      SELECT token_address, holder_address, balance
      FROM (
        SELECT
          token_address,
          holder_address,
          balance,
          ROW_NUMBER() OVER (
            PARTITION BY token_address
            ORDER BY balance DESC
          ) as rn
        FROM token_holders th
        WHERE token_address = ANY($1)
          AND EXISTS (
            SELECT 1 FROM token_transactions tt
            WHERE LOWER(tt.token_address)          = LOWER(th.token_address)
              AND LOWER(tt.address_message_sender) = LOWER(th.holder_address)
              AND tt.position IN ('BUY', 'SELL')
          )
      ) ranked
      WHERE rn <= 10
    `, [addresses]),

    // dev mark per token — konsisten dengan WS
    // DH = first buy dev, DB = buy berikutnya, DP = sell sebagian, DS = sell semua
    db.query(`
      SELECT
        tt.token_address,
        COALESCE(th.balance, 0)                               AS dev_balance,
        COUNT(*) FILTER (WHERE tt.position = 'BUY')           AS buy_count,
        COUNT(*) FILTER (WHERE tt.position = 'SELL')          AS sell_count
      FROM token_transactions tt
      JOIN launch_tokens lt
        ON LOWER(lt.token_address)     = LOWER(tt.token_address)
       AND LOWER(lt.developer_address) = LOWER(tt.address_message_sender)
      LEFT JOIN token_holders th
        ON LOWER(th.token_address)  = LOWER(tt.token_address)
       AND LOWER(th.holder_address) = LOWER(lt.developer_address)
      WHERE tt.token_address = ANY($1)
        AND tt.is_dev = true
      GROUP BY tt.token_address, th.balance
    `, [addresses]),

  ]);

  // ── statsMap ────────────────────────────────────────────────
  const statsMap = {};
  for (const r of statsResult.rows) {
    statsMap[r.token_address] = {
      priceUsdt:   Number(r.price_usdt        || 0),
      marketCap:   Number(r.marketcap         || 0),
      volumeUsdt:  Number(r.volume_usdt       || 0),
      txCount:     Number(r.tx_count          || 0),
      holderCount: Number(r.holder_count      || 0),
      devSupply:   Number(r.dev_supply        || 0),
      top10Supply: Number(r.top_holder_supply || 0),
      paperPct:    Number(r.paperhand_pct     || 0),
    };
  }

  // ── holderMap ────────────────────────────────────────────────
  const holderMap = {};
  for (const h of holderResult.rows) {
    if (!holderMap[h.token_address]) holderMap[h.token_address] = [];
    if (holderMap[h.token_address].length < 10) {
      holderMap[h.token_address].push(h);
    }
  }

  // ── holderCountMap ───────────────────────────────────────────
  const holderCountMap = {};
  for (const addr of addresses) {
    holderCountMap[addr] = statsMap[addr]?.holderCount || 0;
  }

  // ── paperMap ─────────────────────────────────────────────────
  const paperMap = {};
  for (const addr of addresses) {
    paperMap[addr] = statsMap[addr]?.paperPct || 0;
  }

  // ── devMarkMap ───────────────────────────────────────────────
  const DEV_DUST_THRESHOLD = 1;

  const devMarkMap = {};
  for (const r of devMarkResult.rows) {
    const buyCount   = Number(r.buy_count   || 0);
    const sellCount  = Number(r.sell_count  || 0);
    const devBalance = Number(r.dev_balance || 0);

    let mark = "DH";
    if (sellCount > 0 && devBalance < DEV_DUST_THRESHOLD) mark = "DS";      // jual semua
    else if (sellCount > 0 && buyCount > sellCount) mark = "DB";             // pernah sell tapi beli lagi
    else if (sellCount > 0) mark = "DP";                                     // jual sebagian, belum beli lagi
    else if (buyCount > 1) mark = "DB";                                      // belum pernah sell, buy > 1x

    devMarkMap[r.token_address] = mark;
  }

  return { statsMap, holderMap, holderCountMap, paperMap, devMarkMap };
}

// ===============================================================
// HELPER — build token response shape (reuse di semua endpoint)
// ===============================================================

function buildTokenResponse(t, { statsMap, holderMap, holderCountMap, paperMap, devMarkMap }) {

  const liq    = getLiquidityStateCache(t.token_address);
  const supply = Number(t.supply || 0);
  const stats  = statsMap[t.token_address] || {
    priceUsdt: 0, marketCap: 0, volumeUsdt: 0, txCount: 0
  };

  if (t.migrated && liq && liq.mode !== "dex") liq.mode = "dex";

  const top10 = (holderMap[t.token_address] || []).map((h, i) => {
    const balance = Number(h.balance || 0);
    const pct = supply > 0
      ? parseFloat(((balance / supply) * 100).toFixed(2))
      : 0;
    return {
      rank: i + 1,
      address: h.holder_address,
      balance,
      pct,
      isDev: h.holder_address.toLowerCase() === (t.developer_address || "").toLowerCase()
    };
  });

  const devHolder  = top10.find(h => h.isDev);
  const devHoldPct = devHolder ? devHolder.pct : 0;
  const mode       = t.migrated ? "dex" : (liq?.mode || "bonding");

  return {
    launchTime:   t.launch_time,
    tokenAddress: t.token_address,
    name:         t.name,
    symbol:       t.symbol,

    basePair:    liq?.base_symbol || t.base_pair    || null,
    baseAddress: t.base_address   || null,

    description: t.description,
    imageUrl:    t.image_url,
    sourceFrom:  t.source_from,

    website:  t.website_url,
    telegram: t.telegram_url,
    twitter:  t.twitter_url,

    totalSupply: supply,
    decimals:    Number(t.decimals || 18),

    priceUsdt:   stats.priceUsdt,
    marketCap:   stats.marketCap,
    volumeUsdt:  stats.volumeUsdt,
    txCount:     stats.txCount,
    holderCount: holderCountMap[t.token_address] || 0,

    tax: {
      buy:  t.tax_buy,
      sell: t.tax_sell
    },

    mode,
    platform: t.migrated
      ? "dex"
      : (liq?.platform || t.source_from || null),

    ...(mode !== "dex" && {
      progress: liq?.progress
        ? Number((liq.progress * 100).toFixed(2))
        : 0,
      targetUSD: liq?.target || 0,
      bondingLiquidity: {
        base: liq?.bonding_base || 0,
        usd:  liq?.bonding_usd  || 0
      }
    }),

    ...(mode === "dex" && {
      liquidity: {
        base: liq?.base_liquidity || 0,
        usd:  liq?.liquidity_usd  || 0
      }
    }),

    migrated:     t.migrated,
    migratedTime: t.migrated_time,

    devMark: devMarkMap[t.token_address] || "DH",

    holderStats: {
      devHoldPct,
      paperHandPct: paperMap[t.token_address] || 0,
      top10
    }
  };
}

// ===============================================================
// NEW TOKENS
// ===============================================================

export async function getNewTokens(req, reply) {

  try {

    const limit    = Math.min(Math.max(parseInt(req.query?.limit) || 50, 1), 500);
    const cacheKey = `new_tokens_${limit}`;

    const cached = cacheGet(cacheKey);
    if (cached) return cached;

    const { rows } = await db.query(`
      SELECT *
      FROM launch_tokens
      WHERE migrated = false
      ORDER BY launch_time DESC
      LIMIT $1
    `, [limit]);

    if (!rows.length) return [];

    const addresses = rows.map(r => r.token_address);
    const maps      = await calcTokenStats(addresses);
    const tokens    = rows.map(t => buildTokenResponse(t, maps));

    cacheSet(cacheKey, tokens, 10_000);

    return tokens;

  } catch (err) {
    console.error("[NEW TOKENS ERROR]", err);
    return reply.code(500).send({ error: "failed_to_fetch_new_tokens" });
  }

}

// ===============================================================
// TOKEN DETAIL
// ===============================================================

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

    const stats      = maps.statsMap[addr] || { priceUsdt: 0, marketCap: 0, volumeUsdt: 0, txCount: 0 };
    const supply     = Number(token.supply || 0);
    const liq        = getLiquidityStateCache(addr);
    const topHolders = (maps.holderMap[addr] || []).map((h, i) => {
      const balance = Number(h.balance || 0);
      const pct     = supply > 0
        ? parseFloat(((balance / supply) * 100).toFixed(2))
        : 0;
      return {
        rank:    i + 1,
        address: h.holder_address,
        balance,
        pct,
        isDev: h.holder_address.toLowerCase() === (token.developer_address || "").toLowerCase()
      };
    });

    const devHolder  = topHolders.find(h => h.isDev);
    const devHoldPct = devHolder ? devHolder.pct : 0;
    const mode       = liq?.mode || (token.migrated ? "dex" : "bonding");

    return {
      launchTime:   token.launch_time,
      tokenAddress: token.token_address,
      name:         token.name,
      symbol:       token.symbol,

      basePair:    liq?.base_symbol || token.base_pair || null,
      baseAddress: token.base_address || null,

      description: token.description,
      imageUrl:    token.image_url,
      sourceFrom:  token.source_from,

      website:  token.website_url,
      telegram: token.telegram_url,
      twitter:  token.twitter_url,

      totalSupply: supply,
      decimals:    Number(token.decimals || 18),

      priceUsdt:   stats.priceUsdt,
      marketCap:   stats.marketCap,
      volumeUsdt:  stats.volumeUsdt,
      txCount:     stats.txCount,
      holderCount: maps.holderCountMap[addr] || 0,

      tax: {
        buy:  token.tax_buy,
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
          usd:  liq?.bonding_usd  || 0
        }
      }),

      ...(mode === "dex" && {
        liquidity: {
          base: liq?.base_liquidity || 0,
          usd:  liq?.liquidity_usd  || 0
        }
      }),

      migrated:     token.migrated,
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

// ===============================================================
// TOKENS MIGRATING
// ===============================================================

export async function getTokensMigrating(req, reply) {

  try {

    const limit    = Math.min(Math.max(parseInt(req.query?.limit) || 50, 1), 500);
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
    const maps      = await calcTokenStats(addresses);
    const tokens    = rows.map(t => buildTokenResponse(t, maps));

    cacheSet(cacheKey, tokens, 10_000);

    return tokens;

  } catch (err) {
    console.error("[TOKEN MIGRATING ERROR]", err);
    return reply.code(500).send({ error: "failed_to_fetch_migrating_tokens" });
  }

}

// ===============================================================
// TOKENS MIGRATED
// ===============================================================

export async function getTokensMigrated(req, reply) {

  try {

    const limit    = Math.min(Math.max(parseInt(req.query?.limit) || 50, 1), 500);
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
    const maps      = await calcTokenStats(addresses);
    const tokens    = rows.map(t => buildTokenResponse(t, maps));

    cacheSet(cacheKey, tokens, 10_000);

    return tokens;

  } catch (err) {
    console.error("[TOKEN MIGRATED ERROR]", err);
    return reply.code(500).send({ error: "failed_to_fetch_migrated_tokens" });
  }

}