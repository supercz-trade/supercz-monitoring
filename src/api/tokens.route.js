// ===============================================================
// tokens.route.js
// FIX: Tidak pakai token_stats — hitung langsung dari
//      token_transactions dan token_holders
// ===============================================================

import { db } from "../infra/database.js";
// [ADDED]
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

  // Query stats
  const { rows: statsRows } = await db.query(`
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
  `, [addresses]);

  // Query top holders untuk setiap token
  // [MODIFIED] ambil max 10 holder per token (anti overload)
  const { rows: holderRows } = await db.query(`
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
    FROM token_holders
    WHERE token_address = ANY($1)
  ) ranked
  WHERE rn <= 10
`, [addresses]);

  // Build statsMap
  const statsMap = {};
  for (const r of statsRows) {
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

  // Build holderMap — top 10 per token
  const holderMap = {};
  for (const h of holderRows) {
    if (!holderMap[h.token_address]) holderMap[h.token_address] = [];
    if (holderMap[h.token_address].length < 10) {
      holderMap[h.token_address].push(h);
    }
  }

  // Build holderCountMap dari statsMap (sudah ada di token_stats)
  const holderCountMap = {};
  for (const addr of addresses) {
    holderCountMap[addr] = statsMap[addr]?.holderCount || 0;
  }

  // Build paperMap dari statsMap
  const paperMap = {};
  for (const addr of addresses) {
    paperMap[addr] = statsMap[addr]?.paperPct || 0;
  }

  return { statsMap, holderMap, holderCountMap, paperMap };
}

// ===============================================================
// NEW TOKENS
// ===============================================================

export async function getNewTokens(req, reply) {

  try {

    const limit = Math.min(Math.max(parseInt(req.query?.limit) || 50, 1), 500);
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

    const { statsMap, holderMap, holderCountMap, paperMap } =
      await calcTokenStats(addresses);

    const tokens = rows.map(t => {

      const liq = getLiquidityStateCache(t.token_address);

      // [ADDED] force sync kalau sudah migrated
      if (t.migrated && liq && liq.mode !== "dex") {
        liq.mode = "dex";
      }
      const supply = Number(t.supply || 0);
      const stats = statsMap[t.token_address] || {
        priceUsdt: 0, marketCap: 0, volumeUsdt: 0, txCount: 0
      };

      const top10 = (holderMap[t.token_address] || []).map(h => {
        const balance = Number(h.balance || 0);
        const pct = supply > 0
          ? parseFloat(((balance / supply) * 100).toFixed(2))
          : 0;
        return {
          address: h.holder_address,
          balance,
          pct,
          isDev: h.holder_address.toLowerCase() === (t.developer_address || "").toLowerCase()
        };
      });

      const devHolder = top10.find(h => h.isDev);
      const devHoldPct = devHolder ? devHolder.pct : 0;

      return {
        launchTime: t.launch_time,
        tokenAddress: t.token_address,
        name: t.name,
        symbol: t.symbol,

        basePair: t.base_pair || null,
        baseAddress: t.base_address || null,

        description: t.description,
        imageUrl: t.image_url,
        sourceFrom: t.source_from,

        website: t.website_url,
        telegram: t.telegram_url,
        twitter: t.twitter_url,

        totalSupply: supply,
        decimals: Number(t.decimals || 18),

        priceUsdt: stats.priceUsdt,
        marketCap: stats.marketCap,
        volumeUsdt: stats.volumeUsdt,
        txCount: stats.txCount,
        holderCount: holderCountMap[t.token_address] || 0,

        tax: {
          buy: t.tax_buy,
          sell: t.tax_sell
        },

        // =========================
        // 🔥 MODE-DRIVEN STATE
        // =========================
        mode: t.migrated ? "dex" : (liq?.mode || "bonding"), // [MODIFIED] DB jadi source of truth,
        platform: t.migrated
          ? "dex"
          : (liq?.platform || t.source_from || null), // [MODIFIED]

        // =========================
        // 🟢 BONDING
        // =========================
        ...((liq?.mode || (t.migrated ? "dex" : "bonding")) !== "dex" && {
          progress: liq?.progress
            ? Number((liq.progress * 100).toFixed(2))
            : 0,

          targetUSD: liq?.target || 0,

          // =========================
          // 🔥 NEW: BONDING LIQUIDITY
          // =========================
          bondingLiquidity: {
            base: liq?.bonding_base || 0,
            usd: liq?.bonding_usd || 0
          }
        }),

        // =========================
        // 🔵 DEX (MIGRATED)
        // =========================
        ...(liq?.mode === "dex" && {
          liquidity: {
            base: liq?.base_liquidity || 0,
            usd: liq?.liquidity_usd || 0
          }
        }),

        // =========================
        // META
        // =========================
        migrated: t.migrated,
        migratedTime: t.migrated_time,

        holderStats: {
          devHoldPct,
          paperHandPct: paperMap[t.token_address] || 0,
          top10
        }
      };

    });

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

    const { statsMap, holderMap, holderCountMap, paperMap } =
      await calcTokenStats([addr]);

    const stats = statsMap[addr] || { priceUsdt: 0, marketCap: 0, volumeUsdt: 0, txCount: 0 };
    const supply = Number(token.supply || 0);

    const topHolders = (holderMap[addr] || []).map((h, i) => {
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

    const liq = getLiquidityStateCache(token.token_address);

    const devHolder = topHolders.find(h => h.isDev);
    const devHoldPct = devHolder ? devHolder.pct : 0;

    return {
      launchTime: token.launch_time,
      tokenAddress: token.token_address,
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
      volumeUsdt: stats.volumeUsdt,
      txCount: stats.txCount,
      holderCount: holderCountMap[addr] || 0,

      tax: {
        buy: token.tax_buy,
        sell: token.tax_sell
      },

      // =========================
      // 🔥 MODE-DRIVEN STATE
      // =========================
      mode: liq?.mode || (token.migrated ? "dex" : "bonding"),

      platform: liq?.platform || token.source_from || null,

      // =========================
      // 🟢 BONDING
      // =========================
      ...((liq?.mode || (token.migrated ? "dex" : "bonding")) !== "dex" && {
        progress: liq?.progress
          ? Number((liq.progress * 100).toFixed(2))
          : 0,
        targetUSD: liq?.target || 0,
        bondingLiquidity: {
          base: liq?.bonding_base || 0,
          usd: liq?.bonding_usd || 0,
        },
      }),

      // =========================
      // 🔵 DEX (MIGRATED)
      // =========================
      ...((liq?.mode || (token.migrated ? "dex" : "bonding")) === "dex" && {
        liquidity: {
          base: liq?.base_liquidity || 0,
          usd: liq?.liquidity_usd || 0
        }
      }),

      // =========================
      // META
      // =========================
      migrated: token.migrated,
      migratedTime: token.migrated_time,

      holderStats: {
        devHoldPct,
        paperHandPct: paperMap[addr] || 0,
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

    const limit = Math.min(Math.max(parseInt(req.query?.limit) || 50, 1), 500);
    const cacheKey = `tokens_migrating_${limit}`;

    const cached = cacheGet(cacheKey);
    if (cached) return cached;

    // JOIN ke token_stats supaya bisa filter marketcap > 11000
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

    const { statsMap, holderMap, holderCountMap, paperMap } =
      await calcTokenStats(addresses);

    const tokens = rows.map(t => {

      const liq = getLiquidityStateCache(t.token_address);

      // [ADDED] force sync kalau sudah migrated
      if (t.migrated && liq && liq.mode !== "dex") {
        liq.mode = "dex";
      }
      const supply = Number(t.supply || 0);
      const stats = statsMap[t.token_address] || {
        priceUsdt: 0, marketCap: 0, volumeUsdt: 0, txCount: 0
      };

      const top10 = (holderMap[t.token_address] || []).map(h => {
        const balance = Number(h.balance || 0);
        const pct = supply > 0
          ? parseFloat(((balance / supply) * 100).toFixed(2))
          : 0;
        return {
          address: h.holder_address,
          balance,
          pct,
          isDev: h.holder_address.toLowerCase() === (t.developer_address || "").toLowerCase()
        };
      });

      const devHolder = top10.find(h => h.isDev);
      const devHoldPct = devHolder ? devHolder.pct : 0;

      return {
        launchTime: t.launch_time,
        tokenAddress: t.token_address,
        name: t.name,
        symbol: t.symbol,

        basePair: t.base_pair || null,
        baseAddress: t.base_address || null,

        description: t.description,
        imageUrl: t.image_url,
        sourceFrom: t.source_from,

        website: t.website_url,
        telegram: t.telegram_url,
        twitter: t.twitter_url,

        totalSupply: supply,
        decimals: Number(t.decimals || 18),

        priceUsdt: stats.priceUsdt,
        marketCap: stats.marketCap,
        volumeUsdt: stats.volumeUsdt,
        txCount: stats.txCount,
        holderCount: holderCountMap[t.token_address] || 0,

        tax: {
          buy: t.tax_buy,
          sell: t.tax_sell
        },

        // =========================
        // 🔥 MODE-DRIVEN STATE
        // =========================
        mode: t.migrated ? "dex" : (liq?.mode || "bonding"), // [MODIFIED] DB jadi source of truth,
        platform: t.migrated
          ? "dex"
          : (liq?.platform || t.source_from || null), // [MODIFIED]

        // =========================
        // 🟢 BONDING
        // =========================
        ...((liq?.mode || (t.migrated ? "dex" : "bonding")) !== "dex" && {
          progress: liq?.progress
            ? Number((liq.progress * 100).toFixed(2))
            : 0,

          targetUSD: liq?.target || 0,

          // =========================
          // 🔥 NEW: BONDING LIQUIDITY
          // =========================
          bondingLiquidity: {
            base: liq?.bonding_base || 0,
            usd: liq?.bonding_usd || 0
          }
        }),

        // =========================
        // 🔵 DEX (MIGRATED)
        // =========================
        ...(liq?.mode === "dex" && {
          liquidity: {
            base: liq?.base_liquidity || 0,
            usd: liq?.liquidity_usd || 0
          }
        }),

        // =========================
        // META
        // =========================
        migrated: t.migrated,
        migratedTime: t.migrated_time,

        holderStats: {
          devHoldPct,
          paperHandPct: paperMap[t.token_address] || 0,
          top10
        }
      };

    });

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

    const { statsMap, holderMap, holderCountMap, paperMap } =
      await calcTokenStats(addresses);

    const tokens = rows.map(t => {

      const liq = getLiquidityStateCache(t.token_address);

      // [ADDED] force sync kalau sudah migrated
      if (t.migrated && liq && liq.mode !== "dex") {
        liq.mode = "dex";
      }
      const supply = Number(t.supply || 0);
      const stats = statsMap[t.token_address] || {
        priceUsdt: 0, marketCap: 0, volumeUsdt: 0, txCount: 0
      };

      const top10 = (holderMap[t.token_address] || []).map(h => {
        const balance = Number(h.balance || 0);
        const pct = supply > 0
          ? parseFloat(((balance / supply) * 100).toFixed(2))
          : 0;
        return {
          address: h.holder_address,
          balance,
          pct,
          isDev: h.holder_address.toLowerCase() === (t.developer_address || "").toLowerCase()
        };
      });

      const devHolder = top10.find(h => h.isDev);
      const devHoldPct = devHolder ? devHolder.pct : 0;

      return {
        launchTime: t.launch_time,
        tokenAddress: t.token_address,
        name: t.name,
        symbol: t.symbol,

        basePair: t.base_pair || null,
        baseAddress: t.base_address || null,

        description: t.description,
        imageUrl: t.image_url,
        sourceFrom: t.source_from,

        website: t.website_url,
        telegram: t.telegram_url,
        twitter: t.twitter_url,

        totalSupply: supply,
        decimals: Number(t.decimals || 18),

        priceUsdt: stats.priceUsdt,
        marketCap: stats.marketCap,
        volumeUsdt: stats.volumeUsdt,
        txCount: stats.txCount,
        holderCount: holderCountMap[t.token_address] || 0,

        tax: {
          buy: t.tax_buy,
          sell: t.tax_sell
        },

        // =========================
        // 🔥 MODE-DRIVEN STATE
        // =========================
        mode: t.migrated ? "dex" : (liq?.mode || "bonding"), // [MODIFIED] DB jadi source of truth,
        platform: t.migrated
          ? "dex"
          : (liq?.platform || t.source_from || null), // [MODIFIED]

        // =========================
        // 🟢 BONDING
        // =========================
        ...((liq?.mode || (t.migrated ? "dex" : "bonding")) !== "dex" && {
          progress: liq?.progress
            ? Number((liq.progress * 100).toFixed(2))
            : 0,

          targetUSD: liq?.target || 0,

          // =========================
          // 🔥 NEW: BONDING LIQUIDITY
          // =========================
          bondingLiquidity: {
            base: liq?.bonding_base || 0,
            usd: liq?.bonding_usd || 0
          }
        }),

        // =========================
        // 🔵 DEX (MIGRATED)
        // =========================
        ...(liq?.mode === "dex" && {
          liquidity: {
            base: liq?.base_liquidity || 0,
            usd: liq?.liquidity_usd || 0
          }
        }),

        // =========================
        // META
        // =========================
        migrated: t.migrated,
        migratedTime: t.migrated_time,

        holderStats: {
          devHoldPct,
          paperHandPct: paperMap[t.token_address] || 0,
          top10
        }
      };

    });

    cacheSet(cacheKey, tokens, 10_000);

    return tokens;

  } catch (err) {
    console.error("[TOKEN MIGRATED ERROR]", err);
    return reply.code(500).send({ error: "failed_to_fetch_migrated_tokens" });
  }

}