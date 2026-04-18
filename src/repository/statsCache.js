// ===============================================================
// statsCache.js
// Memory cache untuk token stats — source of truth untuk WS publish
//
// Tanggung jawab:
//   - Simpan & ambil stats token di memory (Map)
//   - Update balance holder (BUY/SELL)
//   - Hitung devSupply, top10Sum, holderCount
// ===============================================================

const TOTAL_SUPPLY = 1_000_000_000;

// Map: tokenAddress → stats object
const tokenStats = new Map();

// ===============================================================
// GET OR CREATE
// ===============================================================

export function getOrCreateStats(tokenAddress) {
  let stats = tokenStats.get(tokenAddress);
  if (!stats) {
    stats = {
      price:        0,
      marketcap:    0,
      volume24h:    0,
      txCount:      0,
      holders:      new Map(), // wallet → balance
      devSupply:    0,
      paperHandPct: 0,
    };
    tokenStats.set(tokenAddress, stats);
  }
  return stats;
}

// ===============================================================
// UPDATE STATS SETELAH TRANSAKSI
// ===============================================================

// [MODIFIED]
export function updateStatsCache({
  tokenAddress,
  wallet,
  amount,
  priceUSDT,
  inUSDTPayable,
  isDev
}) {

  const stats = getOrCreateStats(tokenAddress);

  // ── Price & marketcap ─────────────────────────────────────
  if (priceUSDT) {
    stats.price     = Number(priceUSDT);
    stats.marketcap = stats.price * TOTAL_SUPPLY;
  }

  // ── Volume & tx ───────────────────────────────────────────
  const vol = Number(inUSDTPayable);

  // [FIXED] prevent NaN overwrite
  if (!Number.isNaN(vol) && vol > 0) {
    stats.volume24h = (stats.volume24h || 0) + vol;
  }

  stats.txCount = (stats.txCount || 0) + 1;

  // ── Balance holder ────────────────────────────────────────
  const prevBalance = stats.holders.get(wallet) || 0;
  const newBalance  = Math.max(prevBalance + amount, 0);
  stats.holders.set(wallet, newBalance);

  // ── Dev supply ────────────────────────────────────────────
  if (isDev === true) {
    stats.devSupply = Math.max((stats.devSupply || 0) + amount, 0);
  }

  return stats;
}

// ===============================================================
// HITUNG TOP 10 SUM
// ===============================================================

export function calcTop10Sum(tokenAddress) {
  const stats = tokenStats.get(tokenAddress);
  if (!stats) return 0;

  return Array.from(stats.holders.values())
    .filter(b => b > 0)
    .sort((a, b) => b - a)
    .slice(0, 10)
    .reduce((sum, b) => sum + b, 0);
}

// ===============================================================
// HITUNG HOLDER COUNT (balance > 0 saja)
// ===============================================================

export function calcHolderCount(tokenAddress) {
  const stats = tokenStats.get(tokenAddress);
  if (!stats) return 0;
  return Array.from(stats.holders.values()).filter(b => b > 0).length;
}

// ===============================================================
// SET PAPERHAND PCT (dari DB)
// ===============================================================

export function setPaperHandPct(tokenAddress, pct) {
  const stats = getOrCreateStats(tokenAddress);
  stats.paperHandPct = Number(pct || 0);
}

// ===============================================================
// GET STATS (untuk WS publish)
// ===============================================================

export function getStats(tokenAddress) {
  return tokenStats.get(tokenAddress) || null;
}