// ===============================================================
// binancePrice.js
// Harga base token via DexScreener API (free, no key needed)
// FIX:
//   1. Fetch gagal → pakai harga terakhir (lastKnown), tidak drop ke 0
//   2. Interval pertama 30 detik, setelah stabil baru 5 menit
//   3. getBasePrice tidak pernah return 0 kalau pernah dapat harga
//   4. Retry per-pair 3x sebelum fallback ke lastKnown
// ===============================================================

const DS_API = "https://api.dexscreener.com/latest/dex/pairs/bsc";

// ── Pair address per symbol di BSC ────────────────────────────
const PAIR_MAP = {
  BNB:  "0x16b9a82891338f9bA80E2D6970FddA79D1eb0daE", // WBNB/USDT PancakeSwap v2
  CAKE: "0x0eD7e52944161450477ee417DE9Cd3a859b14fD0", // CAKE/WBNB PancakeSwap v2
  ASTER:"0x7E58f160B5B77b8B24Cd9900C09A3E730215aC47", // ASTER/USDT PancakeSwap v3
  币安人生: "0x66f289De31EEF70d52186729d2637Ac978CFC56B",
     FORM: "0x7Cb113B487e025b3a69537fcA579559433240cb5",
};

// ── Stablecoins selalu 1 ───────────────────────────────────────
const STABLECOINS = new Set(["USDT", "USDC", "USD1", "U", "USDB", "BUSD", "UUSD"]);

// ── State ──────────────────────────────────────────────────────
const _state = {
  cache:     {},      // symbol → harga terkini (bisa kosong kalau belum pernah fetch)
  lastKnown: {},      // symbol → harga terakhir yang valid (TIDAK pernah dihapus)
  interval:  null,
  fastTimer: null,    // interval cepat 30 detik untuk warmup awal
};

// ===============================================================
// FETCH SINGLE PAIR — retry 3x, fallback ke lastKnown
// ===============================================================

async function fetchDexScreenerPrice(symbol, pairAddress) {
  let lastErr = null;

  for (let attempt = 0; attempt < 3; attempt++) {

    try {
      const res = await fetch(`${DS_API}/${pairAddress}`);

      if (!res.ok) {
        lastErr = `HTTP ${res.status}`;
        await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
        continue;
      }

      const json = await res.json();

      const pair =
        json?.pair ??
        (Array.isArray(json?.pairs) ? json.pairs[0] : null);

      if (!pair) {
        lastErr = `No pair data`;
        continue;
      }

      const price = parseFloat(pair.priceUsd);

      if (!price || isNaN(price) || price <= 0) {
        lastErr = `Invalid price: ${pair.priceUsd}`;
        continue;
      }

      const prev = _state.cache[symbol];
      _state.cache[symbol]     = price;
      _state.lastKnown[symbol] = price; // ← simpan sebagai lastKnown

      if (!prev || Math.abs(price - prev) / prev > 0.0001) {
        const base  = pair.baseToken?.symbol  ?? "?";
        const quote = pair.quoteToken?.symbol ?? "?";
        console.log(`[PRICE] ${symbol} = $${price.toFixed(6)} (${base}/${quote})`);
      }

      return; // ← sukses, keluar dari retry loop

    } catch (err) {
      lastErr = err.message;
      await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
    }
  }

  // Semua attempt gagal
  const fallback = _state.lastKnown[symbol];
  if (fallback) {
    console.warn(`[PRICE] fetch failed for ${symbol} (${lastErr}), using lastKnown: $${fallback}`);
    _state.cache[symbol] = fallback; // ← tetap pakai harga lama, tidak drop ke 0
  } else {
    console.error(`[PRICE] fetch failed for ${symbol} (${lastErr}), no lastKnown available`);
  }
}

// ===============================================================
// UPDATE ALL (parallel)
// ===============================================================

async function updatePrices() {
  await Promise.allSettled(
    Object.entries(PAIR_MAP).map(([symbol, pair]) =>
      fetchDexScreenerPrice(symbol, pair)
    )
  );
}

// ===============================================================
// PUBLIC API
// ===============================================================

export function startPriceStream() {
  if (_state.interval) return;

  console.log("[PRICE] Starting DexScreener price polling");

  // Fetch pertama langsung
  updatePrices();

  // Fast interval 30 detik untuk 10 menit pertama
  // supaya harga cepat tersedia setelah server start
  let fastCount = 0;
  _state.fastTimer = setInterval(async () => {
    await updatePrices();
    fastCount++;
    if (fastCount >= 20) { // 20 × 30s = 10 menit
      clearInterval(_state.fastTimer);
      _state.fastTimer = null;
      console.log("[PRICE] Switched to 5-minute interval");
    }
  }, 30_000);

  // Normal interval 5 menit (jalan paralel, untuk jangka panjang)
  _state.interval = setInterval(updatePrices, 5 * 60_000);
}

export function stopPriceStream() {
  if (_state.interval) {
    clearInterval(_state.interval);
    _state.interval = null;
  }
  if (_state.fastTimer) {
    clearInterval(_state.fastTimer);
    _state.fastTimer = null;
  }
  console.log("[PRICE] Stopped");
}

export function getBasePrice(baseSymbol) {
  if (!baseSymbol) return 0;
  if (STABLECOINS.has(baseSymbol)) return 1;

  // ✅ FIX: pakai lastKnown kalau cache kosong
  //    lastKnown tidak pernah dihapus — harga lama lebih baik dari 0
  const price = _state.cache[baseSymbol] || _state.lastKnown[baseSymbol];

  if (!price) {
    console.warn(`[PRICE] No price available for ${baseSymbol}`);
    return 0;
  }

  return price;
}

export async function waitPricesReady(timeoutMs = 15000) {
  const required = Object.keys(PAIR_MAP);
  const step     = 200;
  let   elapsed  = 0;

  console.log("[PRICE] Waiting for prices:", required.join(", "));

  while (elapsed < timeoutMs) {
    const missing = required.filter(s => !_state.cache[s] && !_state.lastKnown[s]);
    if (missing.length === 0) {
      console.log("[PRICE] All prices ready:", _state.cache);
      return true;
    }
    await new Promise(r => setTimeout(r, step));
    elapsed += step;
  }

  console.warn("[PRICE] Timeout, partial cache:", _state.cache);
  return false;
}

export function getPriceCache() {
  return { ..._state.cache };
}

export function addPair(symbol, pairAddress) {
  PAIR_MAP[symbol] = pairAddress;
  console.log(`[PRICE] Pair added: ${symbol} → ${pairAddress}`);
  fetchDexScreenerPrice(symbol, pairAddress);
}