// ===============================================================
// logger.js — Centralized logger + WS publisher untuk semua handler
//
// FILTER MODE — debug per token address:
//   Set env variable LOG_TOKEN=0xabc... sebelum start
//   Hanya log yang berkaitan dengan token itu yang tampil
//
//   Contoh:
//     LOG_TOKEN=0x69ebee85603de46166966d873ae0514f96ae4444 pm2 start ...
//     LOG_TOKEN=0x69ebee85603de46166966d873ae0514f96ae4444 node src/server.js
//
// CANDLE DEBUG — lihat candle real-time per token:
//   Set LOG_CANDLE=0xabc... untuk print setiap update candle 1s
//   dan setiap flush ke DB untuk token tersebut
//
//   Contoh:
//     LOG_CANDLE=0x69ebee85... node src/server.js
//
//   Atau gabung keduanya:
//     LOG_TOKEN=0x69ebee85... LOG_CANDLE=0x69ebee85... node src/server.js
// ===============================================================

import { publish } from "./wsbroker.js";

// ===============================================================
// FILTER CONFIG — baca dari env saat startup
// ===============================================================

const _filterToken  = process.env.LOG_TOKEN?.toLowerCase()  || null;
const _filterCandle = process.env.LOG_CANDLE?.toLowerCase() || null;

// Tampilkan filter aktif saat startup
if (_filterToken) {
  console.log(`\x1b[33m[LOGGER] Filter aktif: hanya token ${_filterToken}\x1b[0m`);
}
if (_filterCandle) {
  console.log(`\x1b[33m[LOGGER] Candle debug aktif: ${_filterCandle}\x1b[0m`);
}

// Helper — cek apakah token ini boleh di-log
function _allowed(tokenAddress) {
  if (!_filterToken) return true; // tidak ada filter → semua boleh
  return tokenAddress?.toLowerCase() === _filterToken;
}

// Helper — cek apakah candle token ini boleh di-debug
function _candleAllowed(tokenAddress) {
  if (!_filterCandle) return false; // candle debug hanya kalau di-set
  return tokenAddress?.toLowerCase() === _filterCandle;
}

// ===============================================================
// ANSI COLORS
// ===============================================================

const C = {
  reset  : "\x1b[0m",
  bold   : "\x1b[1m",
  dim    : "\x1b[2m",

  white  : "\x1b[37m",
  gray   : "\x1b[90m",
  cyan   : "\x1b[36m",
  yellow : "\x1b[33m",
  green  : "\x1b[32m",
  red    : "\x1b[31m",
  blue   : "\x1b[34m",
  magenta: "\x1b[35m",

  bgGreen  : "\x1b[42m",
  bgRed    : "\x1b[41m",
  bgYellow : "\x1b[43m",
  bgBlue   : "\x1b[44m",
  bgMagenta: "\x1b[45m",
  bgCyan   : "\x1b[46m",
};

// ===============================================================
// HELPERS
// ===============================================================

function fmt(label, value, valueColor = C.white) {
  return `  ${C.gray}${label.padEnd(20)}${C.reset}${valueColor}${value}${C.reset}`;
}

function fmtUSD(value) {
  if (value == null || isNaN(value)) return "N/A";
  if (value >= 1000)  return `$${value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  if (value >= 1)     return `$${value.toFixed(4)}`;
  if (value >= 0.001) return `$${value.toFixed(6)}`;
  return `$${value.toFixed(10)}`;
}

function fmtToken(value) {
  if (value == null || isNaN(value)) return "N/A";
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (value >= 1_000)     return `${(value / 1_000).toFixed(2)}K`;
  return value.toFixed(4);
}

function fmtTime(ts) {
  return new Date(ts).toISOString().replace("T", " ").slice(0, 23);
}

function fmtUnix(unixSec) {
  return new Date(unixSec * 1000).toISOString().replace("T", " ").slice(0, 23);
}

function positionLabel(position) {
  const map = {
    BUY           : `${C.bold}${C.bgGreen}  BUY  ${C.reset}`,
    SELL          : `${C.bold}${C.bgRed}  SELL ${C.reset}`,
    GENESIS       : `${C.bold}${C.bgBlue} GENESIS ${C.reset}`,
    ADD_LIQUIDITY : `${C.bold}${C.bgMagenta} ADD_LIQ ${C.reset}`,
  };
  return map[position] || `${C.bold}${C.bgYellow} ${position} ${C.reset}`;
}

function platformLabel(platform) {
  const map = {
    fourmeme : `${C.bold}${C.cyan}[FOUR.MEME]${C.reset}`,
    flap     : `${C.bold}${C.yellow}[FLAP.SH] ${C.reset}`,
    pancake  : `${C.bold}${C.green}[PANCAKE] ${C.reset}`,
  };
  return map[platform] || `[${platform.toUpperCase()}]`;
}

function separator(char = "─", len = 60) {
  return C.gray + char.repeat(len) + C.reset;
}

// ===============================================================
// CANDLE DEBUG
// ===============================================================

// Dipanggil dari candleBuilder.js setiap kali candle diupdate
// Hanya print kalau tokenAddress === LOG_CANDLE
export function logCandleUpdate(tokenAddress, candle, source = "TX") {
  if (!_candleAllowed(tokenAddress)) return;

  const dir   = source === "FLUSH" ? `${C.yellow}◀ FLUSH${C.reset}` : `${C.cyan}↑ UPDATE${C.reset}`;
  const ohlcv = [
    `O:${C.yellow}${fmtUSD(candle.open)}${C.reset}`,
    `H:${C.green}${fmtUSD(candle.high)}${C.reset}`,
    `L:${C.red}${fmtUSD(candle.low)}${C.reset}`,
    `C:${C.white}${fmtUSD(candle.close)}${C.reset}`,
    `V:${C.gray}${fmtUSD(candle.volume)}${C.reset}`,
    `TX:${C.gray}${candle.txCount}${C.reset}`,
  ].join("  ");

  console.log(
    `${C.magenta}[CANDLE 1s]${C.reset} ${dir}  ` +
    `${C.gray}${fmtUnix(candle.time)}${C.reset}  ${ohlcv}`
  );
}

// Dipanggil dari candleAggregator.js setiap candle timeframe diupdate
export function logCandleAgg(tokenAddress, label, candle, closed = false) {
  if (!_candleAllowed(tokenAddress)) return;

  const status = closed
    ? `${C.yellow}◀ CLOSED${C.reset}`
    : `${C.cyan}↑ LIVE  ${C.reset}`;

  const ohlcv = [
    `O:${C.yellow}${fmtUSD(candle.open)}${C.reset}`,
    `H:${C.green}${fmtUSD(candle.high)}${C.reset}`,
    `L:${C.red}${fmtUSD(candle.low)}${C.reset}`,
    `C:${C.white}${fmtUSD(candle.close)}${C.reset}`,
    `V:${C.gray}${fmtUSD(candle.volume)}${C.reset}`,
  ].join("  ");

  console.log(
    `${C.magenta}[CANDLE ${label.padEnd(3)}]${C.reset} ${status}  ` +
    `${C.gray}${fmtUnix(candle.time)}${C.reset}  ${ohlcv}`
  );
}

// Dipanggil dari candleBuilder.js saat flush ke DB selesai
// Tampilkan open/close sambung atau tidak
export function logCandleFlush(tokenAddress, candle, prevClose) {
  if (!_candleAllowed(tokenAddress)) return;

  const gapOk  = prevClose == null || Math.abs(candle.open - prevClose) < 0.0000000001;
  const gapStr = gapOk
    ? `${C.green}✓ open sambung${C.reset}`
    : `${C.red}✗ GAP! open=${fmtUSD(candle.open)} prevClose=${fmtUSD(prevClose)}${C.reset}`;

  console.log(
    `${C.magenta}[CANDLE DB ]${C.reset} ${C.yellow}◀ FLUSH${C.reset}  ` +
    `${C.gray}${fmtUnix(candle.time)}${C.reset}  ` +
    `close=${C.white}${fmtUSD(candle.close)}${C.reset}  ${gapStr}`
  );
}

// ===============================================================
// TRADE LOG
// ===============================================================

export function logTrade({
  platform,
  position,
  tokenAddress,
  tokenSymbol,
  tokenAmount,
  baseSymbol,
  baseAmount,
  priceBase,
  priceUSDT,
  volumeUSDT,
  txHash,
  blockNumber,
  timestamp,
  wallet,
  isDev,
  pairAddress,
}) {

  // ── Filter ─────────────────────────────────────────────────
  if (!_allowed(tokenAddress)) return;

  const plat  = platformLabel(platform);
  const pos   = positionLabel(position);
  const arrow = position === "BUY" ? `${C.green}▲${C.reset}` : `${C.red}▼${C.reset}`;

  console.log(`\n${separator()}`);
  console.log(`  ${plat} ${pos} ${arrow}  ${C.bold}${tokenSymbol ?? tokenAddress.slice(0,10)+"..."}${C.reset}  ${C.gray}${fmtTime(timestamp)}${C.reset}`);
  console.log(separator("─", 60));

  console.log(fmt("Token Address",  tokenAddress,                  C.cyan));
  if (pairAddress)
  console.log(fmt("Pair Address",   pairAddress,                   C.cyan));
  console.log(fmt("TX Hash",        txHash,                        C.blue));
  console.log(fmt("Block",          `#${blockNumber}`,             C.gray));
  console.log(fmt("Wallet",         wallet + (isDev ? ` ${C.yellow}[DEV]${C.reset}` : ""), C.white));

  console.log(separator("·", 60));

  console.log(fmt("Token Amount",   fmtToken(tokenAmount),         C.white));
  console.log(fmt("Base Paid",      `${baseAmount?.toFixed(6) ?? "N/A"} ${baseSymbol}`, C.white));
  console.log(fmt("Price",          `${fmtUSD(priceUSDT)} / ${priceBase?.toFixed(10) ?? "N/A"} ${baseSymbol}`, C.yellow));
  console.log(fmt("Volume",         fmtUSD(volumeUSDT),            C.green));

  console.log(`${separator()}\n`);

  // ── WS publish — selalu, tidak peduli filter ───────────────
  const _tp = { platform, position, tokenAddress, tokenSymbol, tokenAmount,
    baseSymbol, baseAmount, priceBase, priceUSDT, volumeUSDT,
    txHash, blockNumber, timestamp, wallet, isDev, pairAddress: pairAddress ?? null };
  publish(`transaction:${tokenAddress}`, _tp);
  publish("transaction:all", _tp);
  publish(`price:${tokenAddress}`, { tokenAddress, priceUSDT, priceBase, volumeUSDT, timestamp });
}

// ===============================================================
// CREATE TOKEN LOG
// ===============================================================

export function logCreate({
  platform,
  tokenAddress,
  tokenSymbol,
  tokenName,
  creator,
  basePair,
  baseAddress,
  taxBuy,
  taxSell,
  txHash,
  blockNumber,
  timestamp,
  firstBuyAmount,
  firstBuyUSD,
}) {

  if (!_allowed(tokenAddress)) return;

  const plat = platformLabel(platform);

  console.log(`\n${separator("═", 60)}`);
  console.log(`  ${plat} ${C.bold}${C.bgBlue} NEW TOKEN ${C.reset}  ${C.bold}${tokenName} (${tokenSymbol})${C.reset}`);
  console.log(`  ${C.gray}${fmtTime(timestamp)}${C.reset}`);
  console.log(separator("═", 60));

  console.log(fmt("Token Address",  tokenAddress,     C.cyan));
  console.log(fmt("Creator",        creator,          C.white));
  console.log(fmt("TX Hash",        txHash,           C.blue));
  console.log(fmt("Block",          `#${blockNumber}`, C.gray));

  console.log(separator("·", 60));

  console.log(fmt("Base Pair",      basePair,         C.yellow));
  console.log(fmt("Base Address",   baseAddress,      C.gray));
  console.log(fmt("Tax Buy",        `${taxBuy ?? "N/A"}%`,  C.white));
  console.log(fmt("Tax Sell",       `${taxSell ?? "N/A"}%`, C.white));

  if (firstBuyAmount !== undefined) {
    console.log(separator("·", 60));
    console.log(fmt("First Buy Amt",  fmtToken(firstBuyAmount), C.green));
    console.log(fmt("First Buy USD",  fmtUSD(firstBuyUSD),      C.green));
  }

  console.log(`${separator("═", 60)}\n`);

  publish("new_token", { platform, tokenAddress, tokenSymbol, tokenName,
    creator, basePair, baseAddress, taxBuy, taxSell, txHash, blockNumber, timestamp,
    firstBuyAmount: firstBuyAmount ?? null, firstBuyUSD: firstBuyUSD ?? null });
}

// ===============================================================
// ADD LIQUIDITY LOG
// ===============================================================

export function logAddLiquidity({
  platform,
  tokenAddress,
  pairAddress,
  baseSymbol,
  baseAddress,
  tokenAmount,
  baseAmount,
  priceBase,
  priceUSDT,
  volumeUSDT,
  txHash,
  blockNumber,
  timestamp,
  sender,
}) {

  if (!_allowed(tokenAddress)) return;

  const plat = platformLabel(platform);

  console.log(`\n${separator("═", 60)}`);
  console.log(`  ${plat} ${C.bold}${C.bgMagenta} MIGRATED TO DEX ${C.reset}`);
  console.log(`  ${C.gray}${fmtTime(timestamp)}${C.reset}`);
  console.log(separator("═", 60));

  console.log(fmt("Token Address",  tokenAddress,            C.cyan));
  console.log(fmt("Pair Address",   pairAddress,             C.cyan));
  console.log(fmt("TX Hash",        txHash,                  C.blue));
  console.log(fmt("Block",          `#${blockNumber}`,       C.gray));
  console.log(fmt("Sender",         sender,                  C.white));

  console.log(separator("·", 60));

  console.log(fmt("Token Amount",   fmtToken(tokenAmount),   C.white));
  console.log(fmt("Base Amount",    `${baseAmount?.toFixed(6) ?? "N/A"} ${baseSymbol}`, C.white));
  console.log(fmt("Price",          `${fmtUSD(priceUSDT)} / ${priceBase?.toFixed(10) ?? "N/A"} ${baseSymbol}`, C.yellow));
  console.log(fmt("Liquidity USD",  fmtUSD(volumeUSDT),      C.green));

  console.log(`${separator("═", 60)}\n`);

  publish("migrate", { platform, tokenAddress, pairAddress, baseSymbol, baseAddress,
    tokenAmount, baseAmount, priceBase, priceUSDT, volumeUSDT, txHash, blockNumber, timestamp, sender });
  publish(`price:${tokenAddress}`, { tokenAddress, priceUSDT, priceBase, volumeUSDT, timestamp });
}

// ===============================================================
// SYSTEM LOG
// ===============================================================

export const log = {
  info  : (...args) => console.log(`${C.cyan}[INFO]${C.reset}  `, ...args),
  warn  : (...args) => console.log(`${C.yellow}[WARN]${C.reset}  `, ...args),
  error : (...args) => console.log(`${C.red}[ERROR]${C.reset} `, ...args),
  ok    : (...args) => console.log(`${C.green}[OK]${C.reset}    `, ...args),
  debug : (...args) => console.log(`${C.gray}[DEBUG]${C.reset} `, ...args),
};