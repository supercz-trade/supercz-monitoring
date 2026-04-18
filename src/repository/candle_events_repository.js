// ================================================================
// candle_events_repository.js
//
// Insert satu event per TX BUY/SELL ke tabel candle_events.
// Dipanggil dari transaction_repository.js setelah insertTransaction.
//
// wallet_type priority (dari tinggi ke rendah):
//   1. dev          → is_dev = true
//   2. whale        → token_amount >= WHALE_THRESHOLD (1% supply = 10M)
//   3. early_buyer  → wallet ada di top-10 holder (dari statsCache)
//   4. regular      → semua lainnya
//
// Query helpers:
//   getEventsByToken()  → chart overlay
//   getDevEvents()      → alert dev buy/sell
//   getWhalEvents()     → alert whale
// ================================================================

import { db } from "../infra/database.js";

const TOTAL_SUPPLY      = 1_000_000_000;
const WHALE_THRESHOLD   = TOTAL_SUPPLY * 0.01;   // 10_000_000 token = 1%
const TOP_N_HOLDER      = 10;

// ================================================================
// RESOLVE WALLET TYPE
// ================================================================

/**
 * Tentukan wallet_type berdasarkan priority.
 *
 * @param {object} opts
 * @param {boolean}  opts.isDev
 * @param {number}   opts.tokenAmount   - absolute value (selalu positif)
 * @param {string}   opts.wallet
 * @param {Map}      opts.holders       - Map<walletAddress, balance> dari statsCache
 * @returns {'dev'|'whale'|'early_buyer'|'regular'}
 */
function resolveWalletType({ isDev, tokenAmount, wallet, holders }) {

  if (isDev) return "dev";

  if (tokenAmount >= WHALE_THRESHOLD) return "whale";

  // Early buyer = masuk top-N holder saat ini
  if (holders && holders.size > 0) {
    // Sort by balance, ambil top N
    const sorted = [...holders.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, TOP_N_HOLDER)
      .map(([addr]) => addr.toLowerCase());

    if (sorted.includes(wallet.toLowerCase())) return "early_buyer";
  }

  return "regular";
}

// ================================================================
// INSERT EVENT
// ================================================================

/**
 * Insert satu candle_event. Dipanggil dari transaction_repository.js.
 * Menggunakan ON CONFLICT DO NOTHING — aman dipanggil ulang.
 *
 * @param {object} data
 * @param {string}   data.txHash
 * @param {string}   data.tokenAddress
 * @param {Date}     data.time              - block timestamp
 * @param {string}   data.position          - 'BUY' | 'SELL'
 * @param {string}   data.wallet
 * @param {boolean}  data.isDev
 * @param {string}   [data.tagAddress]
 * @param {number}   data.priceUSDT
 * @param {number}   data.amountReceive     - token amount (raw, positif)
 * @param {string}   data.basePayable       - 'SOL' | 'BNB' | dll
 * @param {number}   data.amountBasePayable - amount dalam base currency
 * @param {number}   data.inUSDTPayable     - volume USD
 * @param {Map}      data.holders           - current holder map dari statsCache
 * @param {object}   [client]               - optional pg client untuk reuse koneksi
 */
export async function insertCandleEvent(data, client) {

  const tokenAmount = Math.abs(data.amountReceive || 0);
  const priceUSDT   = data.priceUSDT || 0;
  const mcapUSDT    = priceUSDT * TOTAL_SUPPLY;

  const walletType  = resolveWalletType({
    isDev:       data.isDev || false,
    tokenAmount,
    wallet:      data.wallet,
    holders:     data.holders,
  });

  // candle_time = floor unix seconds → key untuk join token_candles 1s
  const rawTime    = data.time instanceof Date ? data.time.getTime() : data.time;
  const candleTime = rawTime > 1e10
    ? Math.floor(rawTime / 1000)
    : Math.floor(rawTime);

  const query = `
    INSERT INTO candle_events (
      tx_hash,
      token_address,
      time,
      candle_time,
      position,
      wallet,
      wallet_type,
      price_usdt,
      mcap_usdt,
      token_amount,
      base_amount,
      base_payable,
      volume_usdt,
      is_dev,
      tag_address
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
    ON CONFLICT (tx_hash) DO NOTHING
  `;

  const params = [
    data.txHash,
    data.tokenAddress,
    data.time,
    candleTime,
    data.position,
    data.wallet.toLowerCase(),
    walletType,
    priceUSDT,
    mcapUSDT,
    tokenAmount,
    data.amountBasePayable || 0,
    data.basePayable       || "",
    data.inUSDTPayable     || 0,
    data.isDev             || false,
    data.tagAddress        || null,
  ];

  // Pakai client yang sudah ada (withClient dari transaction_repository)
  // atau ambil dari pool kalau tidak ada
  if (client) {
    await client.query(query, params);
  } else {
    await db.query(query, params);
  }
}

// ================================================================
// QUERY HELPERS
// ================================================================

/**
 * Ambil events untuk satu token dalam range waktu.
 * Dipakai oleh chart overlay endpoint.
 *
 * @param {string} tokenAddress
 * @param {object} opts
 * @param {Date}   [opts.from]        - default: 24 jam lalu
 * @param {Date}   [opts.to]          - default: now
 * @param {string} [opts.walletType]  - filter: 'dev' | 'whale' | 'early_buyer' | 'regular'
 * @param {number} [opts.limit]       - default: 500
 * @returns {Promise<object[]>}
 */
export async function getEventsByToken(tokenAddress, opts = {}) {

  const from       = opts.from       ?? new Date(Date.now() - 86_400_000);
  const to         = opts.to         ?? new Date();
  const limit      = opts.limit      ?? 500;
  const walletType = opts.walletType ?? null;

  let query, params;

  if (walletType) {
    query = `
      SELECT
        tx_hash,
        time,
        candle_time,
        position,
        wallet,
        wallet_type,
        price_usdt,
        mcap_usdt,
        token_amount,
        base_amount,
        base_payable,
        volume_usdt,
        is_dev,
        tag_address
      FROM candle_events
      WHERE token_address = $1
        AND time >= $2
        AND time <= $3
        AND wallet_type = $4
      ORDER BY time DESC
      LIMIT $5
    `;
    params = [tokenAddress, from, to, walletType, limit];
  } else {
    query = `
      SELECT
        tx_hash,
        time,
        candle_time,
        position,
        wallet,
        wallet_type,
        price_usdt,
        mcap_usdt,
        token_amount,
        base_amount,
        base_payable,
        volume_usdt,
        is_dev,
        tag_address
      FROM candle_events
      WHERE token_address = $1
        AND time >= $2
        AND time <= $3
      ORDER BY time DESC
      LIMIT $4
    `;
    params = [tokenAddress, from, to, limit];
  }

  const { rows } = await db.query(query, params);
  return rows;
}

/**
 * Ambil events dev untuk satu token.
 * Dipakai alert engine untuk deteksi dev buy/sell.
 *
 * @param {string} tokenAddress
 * @param {number} [sinceSeconds=3600]  - lookback window dalam detik
 * @returns {Promise<object[]>}
 */
export async function getDevEvents(tokenAddress, sinceSeconds = 3_600) {

  const { rows } = await db.query(`
    SELECT
      tx_hash,
      time,
      position,
      wallet,
      price_usdt,
      mcap_usdt,
      token_amount,
      volume_usdt
    FROM candle_events
    WHERE token_address = $1
      AND wallet_type   = 'dev'
      AND time         >= NOW() - ($2 || ' seconds')::interval
    ORDER BY time DESC
  `, [tokenAddress, sinceSeconds]);

  return rows;
}

/**
 * Ambil events whale untuk satu token.
 *
 * @param {string} tokenAddress
 * @param {number} [sinceSeconds=3600]
 * @returns {Promise<object[]>}
 */
export async function getWhaleEvents(tokenAddress, sinceSeconds = 3_600) {

  const { rows } = await db.query(`
    SELECT
      tx_hash,
      time,
      position,
      wallet,
      price_usdt,
      mcap_usdt,
      token_amount,
      volume_usdt
    FROM candle_events
    WHERE token_address = $1
      AND wallet_type   = 'whale'
      AND time         >= NOW() - ($2 || ' seconds')::interval
    ORDER BY time DESC
  `, [tokenAddress, sinceSeconds]);

  return rows;
}

/**
 * Ambil events berdasarkan wallet address.
 * Dipakai profil wallet / social feed.
 *
 * @param {string} wallet
 * @param {object} [opts]
 * @param {number} [opts.limit=100]
 * @param {Date}   [opts.before]   - cursor pagination
 * @returns {Promise<object[]>}
 */
export async function getEventsByWallet(wallet, opts = {}) {

  const limit  = opts.limit  ?? 100;
  const before = opts.before ?? new Date();

  const { rows } = await db.query(`
    SELECT
      tx_hash,
      token_address,
      time,
      position,
      wallet_type,
      price_usdt,
      mcap_usdt,
      token_amount,
      base_amount,
      base_payable,
      volume_usdt
    FROM candle_events
    WHERE wallet = $1
      AND time  < $2
    ORDER BY time DESC
    LIMIT $3
  `, [wallet.toLowerCase(), before, limit]);

  return rows;
}

/**
 * Aggregasi events per candle window.
 * Dipakai chart overlay — ambil summary dev/whale activity per candle.
 *
 * @param {string} tokenAddress
 * @param {string} timeframe        - '1m' | '5m' | '15m' | dll
 * @param {Date}   from
 * @param {Date}   to
 * @returns {Promise<object[]>}  - array of { candle_ts, dev_buy_vol, dev_sell_vol, whale_buy_vol, whale_sell_vol, ... }
 */
export async function getEventSummaryByCandle(tokenAddress, timeframe, from, to) {

  // Tentukan window seconds berdasarkan timeframe
  const WINDOW_MAP = {
    "1s":  1, "15s": 15, "30s": 30,
    "1m":  60, "5m": 300, "15m": 900, "30m": 1_800,
    "1h":  3_600, "4h": 14_400, "1d": 86_400,
  };

  const windowSec = WINDOW_MAP[timeframe];
  if (!windowSec) throw new Error(`Unknown timeframe: ${timeframe}`);

  const { rows } = await db.query(`
    SELECT
      -- align ke candle window
      to_timestamp(
        floor(extract(epoch FROM time) / $4) * $4
      )                                             AS candle_ts,

      -- dev activity
      SUM(CASE WHEN wallet_type = 'dev'   AND position = 'BUY'  THEN volume_usdt ELSE 0 END) AS dev_buy_vol,
      SUM(CASE WHEN wallet_type = 'dev'   AND position = 'SELL' THEN volume_usdt ELSE 0 END) AS dev_sell_vol,
      COUNT(CASE WHEN wallet_type = 'dev' AND position = 'BUY'  THEN 1 END)                  AS dev_buy_count,
      COUNT(CASE WHEN wallet_type = 'dev' AND position = 'SELL' THEN 1 END)                  AS dev_sell_count,

      -- whale activity
      SUM(CASE WHEN wallet_type = 'whale'   AND position = 'BUY'  THEN volume_usdt ELSE 0 END) AS whale_buy_vol,
      SUM(CASE WHEN wallet_type = 'whale'   AND position = 'SELL' THEN volume_usdt ELSE 0 END) AS whale_sell_vol,
      COUNT(CASE WHEN wallet_type = 'whale' AND position = 'BUY'  THEN 1 END)                  AS whale_buy_count,
      COUNT(CASE WHEN wallet_type = 'whale' AND position = 'SELL' THEN 1 END)                  AS whale_sell_count,

      -- total activity
      COUNT(*)                                                  AS total_tx,
      SUM(volume_usdt)                                          AS total_volume

    FROM candle_events
    WHERE token_address = $1
      AND time >= $2
      AND time <= $3
    GROUP BY candle_ts
    ORDER BY candle_ts ASC
  `, [tokenAddress, from, to, windowSec]);

  return rows;
}