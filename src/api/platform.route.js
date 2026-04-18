// ===============================================================
// platform.route.js
// GET /platform/stats?period=24h|1h|6h|5m
//
// Mengembalikan data market overview:
//   - Total TXs + % change vs periode sebelumnya
//   - Total Traders (unique wallets) + % change
//   - Volume 24h total + buy/sell breakdown
//   - Token Created + % change
//   - Migrations + % change
//   - Top Launchpads by volume
//   - Top Protocols (DEX) by volume setelah migrate
// ===============================================================

import { db } from "../infra/database.js";

// ── Period config ──────────────────────────────────────────────
const PERIODS = {
  "5m":  5  * 60,
  "1h":  60 * 60,
  "6h":  6  * 60 * 60,
  "24h": 24 * 60 * 60,
};

// ── Source from label map ──────────────────────────────────────
// Sesuaikan dengan nilai source_from di tabel launch_tokens
const SOURCE_LABEL = {
  four_meme : "Four.meme",
  flap      : "Flap.sh",
  pancake   : "PancakeSwap",
  unknown   : "Unknown",
};

// ===============================================================
// HELPER — hitung % change
// ===============================================================

function pctChange(current, previous) {
  if (!previous || previous === 0) return current > 0 ? 100 : 0;
  return Number((((current - previous) / previous) * 100).toFixed(2));
}

// ===============================================================
// GET PLATFORM STATS
// GET /platform/stats?period=24h
// ===============================================================

export async function getPlatformStats(req, reply) {

  try {

    const periodKey = req.query.period || "24h";
    const periodSec = PERIODS[periodKey];

    if (!periodSec) {
      return reply.code(400).send({
        error: "invalid period",
        valid: Object.keys(PERIODS),
      });
    }

    const now      = Math.floor(Date.now() / 1000);
    const fromCur  = new Date((now - periodSec)         * 1000); // periode ini
    const fromPrev = new Date((now - periodSec * 2)     * 1000); // periode sebelumnya
    const toPrev   = fromCur;                                    // batas periode sebelumnya

    // ── Jalankan semua query paralel ──────────────────────────
    const [
      txStatsResult,
      txStatsPrevResult,
      volumeResult,
      volumePrevResult,
      tokenCreatedResult,
      tokenCreatedPrevResult,
      migrationResult,
      migrationPrevResult,
      launchpadResult,
      protocolResult,
    ] = await Promise.all([

      // 1. TX count + unique traders — periode ini
      db.query(`
        SELECT
          COUNT(*)                                          AS tx_count,
          COUNT(DISTINCT address_message_sender)            AS trader_count
        FROM token_transactions
        WHERE time >= $1
          AND position IN ('BUY', 'SELL')
      `, [fromCur]),

      // 2. TX count + unique traders — periode sebelumnya
      db.query(`
        SELECT
          COUNT(*)                                          AS tx_count,
          COUNT(DISTINCT address_message_sender)            AS trader_count
        FROM token_transactions
        WHERE time >= $1 AND time < $2
          AND position IN ('BUY', 'SELL')
      `, [fromPrev, toPrev]),

      // 3. Volume breakdown (buy/sell) — periode ini
      db.query(`
        SELECT
          COALESCE(SUM(in_usdt_payable), 0)                         AS total_volume,
          COALESCE(SUM(in_usdt_payable) FILTER (WHERE position = 'BUY'),  0) AS buy_volume,
          COALESCE(SUM(in_usdt_payable) FILTER (WHERE position = 'SELL'), 0) AS sell_volume,
          COUNT(*)              FILTER (WHERE position = 'BUY')             AS buy_count,
          COUNT(*)              FILTER (WHERE position = 'SELL')            AS sell_count
        FROM token_transactions
        WHERE time >= $1
          AND position IN ('BUY', 'SELL')
      `, [fromCur]),

      // 4. Volume — periode sebelumnya
      db.query(`
        SELECT
          COALESCE(SUM(in_usdt_payable), 0) AS total_volume
        FROM token_transactions
        WHERE time >= $1 AND time < $2
          AND position IN ('BUY', 'SELL')
      `, [fromPrev, toPrev]),

      // 5. Token created — periode ini
      db.query(`
        SELECT COUNT(*) AS count
        FROM launch_tokens
        WHERE launch_time >= $1
      `, [fromCur]),

      // 6. Token created — periode sebelumnya
      db.query(`
        SELECT COUNT(*) AS count
        FROM launch_tokens
        WHERE launch_time >= $1 AND launch_time < $2
      `, [fromPrev, toPrev]),

      // 7. Migrations — periode ini
      db.query(`
        SELECT COUNT(*) AS count
        FROM launch_tokens
        WHERE migrated = true
          AND migrated_time >= $1
      `, [fromCur]),

      // 8. Migrations — periode sebelumnya
      db.query(`
        SELECT COUNT(*) AS count
        FROM launch_tokens
        WHERE migrated = true
          AND migrated_time >= $1 AND migrated_time < $2
      `, [fromPrev, toPrev]),

      // 9. Top Launchpads by volume — dari source_from di launch_tokens
      db.query(`
        SELECT
          lt.source_from                                AS source,
          COALESCE(SUM(tt.in_usdt_payable), 0)         AS volume,
          COUNT(DISTINCT tt.token_address)              AS token_count,
          COUNT(*)                                      AS tx_count
        FROM token_transactions tt
        JOIN launch_tokens lt ON LOWER(lt.token_address) = LOWER(tt.token_address)
        WHERE tt.time >= $1
          AND tt.position IN ('BUY', 'SELL')
        GROUP BY lt.source_from
        ORDER BY volume DESC
        LIMIT 10
      `, [fromCur]),

      // 10. Top Protocols — volume per launchpad setelah token migrate
      // source_from dari launch_tokens = nama platform (four_meme, flap, dll)
      // base_symbol dari token_migrate = base pair (BNB, USDT) — ini bukan nama DEX
      // Setelah migrate ke DEX, volume tetap di-track via token_transactions
      db.query(`
        SELECT
          lt.source_from                                AS protocol,
          tm.base_symbol                                AS base_symbol,
          COALESCE(SUM(tt.in_usdt_payable), 0)         AS volume,
          COUNT(DISTINCT tt.token_address)              AS token_count,
          COUNT(*)                                      AS tx_count
        FROM token_transactions tt
        JOIN token_migrate tm ON LOWER(tm.token_address) = LOWER(tt.token_address)
        JOIN launch_tokens  lt ON LOWER(lt.token_address) = LOWER(tt.token_address)
        WHERE tt.time  >= $1
          AND tt.position IN ('BUY', 'SELL')
          AND lt.migrated = true
        GROUP BY lt.source_from, tm.base_symbol
        ORDER BY volume DESC
        LIMIT 10
      `, [fromCur]),

    ]);

    // ── Parse results ─────────────────────────────────────────

    const txCur  = txStatsResult.rows[0];
    const txPrev = txStatsPrevResult.rows[0];
    const volCur  = volumeResult.rows[0];
    const volPrev = volumePrevResult.rows[0];

    const txCount      = Number(txCur.tx_count      || 0);
    const traderCount  = Number(txCur.trader_count  || 0);
    const txCountPrev  = Number(txPrev.tx_count     || 0);
    const traderPrev   = Number(txPrev.trader_count || 0);

    const totalVolume  = Number(volCur.total_volume  || 0);
    const buyVolume    = Number(volCur.buy_volume    || 0);
    const sellVolume   = Number(volCur.sell_volume   || 0);
    const buyCount     = Number(volCur.buy_count     || 0);
    const sellCount    = Number(volCur.sell_count    || 0);
    const totalVolPrev = Number(volPrev.total_volume || 0);

    const tokenCreated     = Number(tokenCreatedResult.rows[0]?.count     || 0);
    const tokenCreatedPrev = Number(tokenCreatedPrevResult.rows[0]?.count || 0);
    const migrations       = Number(migrationResult.rows[0]?.count        || 0);
    const migrationsPrev   = Number(migrationPrevResult.rows[0]?.count    || 0);

    // ── Top Launchpads ────────────────────────────────────────
    const launchpads = launchpadResult.rows.map(r => ({
      source:     r.source,
      label:      SOURCE_LABEL[r.source] || r.source,
      volume:     Number(r.volume      || 0),
      tokenCount: Number(r.token_count || 0),
      txCount:    Number(r.tx_count    || 0),
    }));

    // ── Top Protocols ─────────────────────────────────────────
    const protocols = protocolResult.rows.map(r => ({
      protocol:   r.protocol,
      label:      SOURCE_LABEL[r.protocol] || r.protocol,
      baseSymbol: r.base_symbol,
      volume:     Number(r.volume      || 0),
      tokenCount: Number(r.token_count || 0),
      txCount:    Number(r.tx_count    || 0),
    }));

    // ── Susun response ────────────────────────────────────────
    return {
      period: periodKey,
      generatedAt: new Date().toISOString(),

      // ── Market Activity ──────────────────────────────────────
      transactions: {
        count:   txCount,
        change:  pctChange(txCount, txCountPrev),
        prev:    txCountPrev,
      },

      traders: {
        count:  traderCount,
        change: pctChange(traderCount, traderPrev),
        prev:   traderPrev,
      },

      volume: {
        total:      totalVolume,
        change:     pctChange(totalVolume, totalVolPrev),
        prev:       totalVolPrev,
        buy: {
          volume: buyVolume,
          count:  buyCount,
        },
        sell: {
          volume: sellVolume,
          count:  sellCount,
        },
      },

      // ── Token Stats ──────────────────────────────────────────
      tokens: {
        created: {
          count:  tokenCreated,
          change: pctChange(tokenCreated, tokenCreatedPrev),
          prev:   tokenCreatedPrev,
        },
        migrations: {
          count:  migrations,
          change: pctChange(migrations, migrationsPrev),
          prev:   migrationsPrev,
        },
      },

      // ── Top Launchpads ───────────────────────────────────────
      launchpads,

      // ── Top Protocols ────────────────────────────────────────
      protocols,
    };

  } catch (err) {
    console.error("[PLATFORM STATS API]", err.message);
    return reply.code(500).send({ error: "failed_to_fetch_platform_stats" });
  }

}

// ===============================================================
// GET PLATFORM VOLUME CHART
// GET /platform/chart?period=24h&interval=5m
//
// Kembalikan time series volume buy/sell untuk chart
// ===============================================================

export async function getPlatformVolumeChart(req, reply) {

  try {

    const periodKey   = req.query.period   || "24h";
    const intervalKey = req.query.interval || "5m";
    const periodSec   = PERIODS[periodKey];
    const intervalSec = PERIODS[intervalKey];

    if (!periodSec) {
      return reply.code(400).send({ error: "invalid period", valid: Object.keys(PERIODS) });
    }
    if (!intervalSec) {
      return reply.code(400).send({ error: "invalid interval", valid: Object.keys(PERIODS) });
    }

    const fromCur = new Date((Math.floor(Date.now() / 1000) - periodSec) * 1000);

    const { rows } = await db.query(`
      SELECT
        (FLOOR(EXTRACT(EPOCH FROM time) / $2) * $2)::bigint   AS bucket,
        COALESCE(SUM(in_usdt_payable) FILTER (WHERE position = 'BUY'),  0) AS buy_volume,
        COALESCE(SUM(in_usdt_payable) FILTER (WHERE position = 'SELL'), 0) AS sell_volume,
        COUNT(*) FILTER (WHERE position = 'BUY')                            AS buy_count,
        COUNT(*) FILTER (WHERE position = 'SELL')                           AS sell_count
      FROM token_transactions
      WHERE time >= $1
        AND position IN ('BUY', 'SELL')
      GROUP BY bucket
      ORDER BY bucket ASC
    `, [fromCur, intervalSec]);

    return rows.map(r => ({
      time:        Number(r.bucket),
      buyVolume:   Number(r.buy_volume  || 0),
      sellVolume:  Number(r.sell_volume || 0),
      buyCount:    Number(r.buy_count   || 0),
      sellCount:   Number(r.sell_count  || 0),
    }));

  } catch (err) {
    console.error("[PLATFORM CHART API]", err.message);
    return reply.code(500).send({ error: "failed_to_fetch_platform_chart" });
  }

}