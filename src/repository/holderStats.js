// ===============================================================
// holderStats.js
// DB operations untuk holder balance & paper hand logic
// FIX: semua fungsi terima optional `client` parameter
//      supaya bisa dipakai dalam satu koneksi bersama
//      transaction.repository.js (withClient pattern)
// ===============================================================

import { db } from "../infra/database.js";

// Helper — pakai client kalau ada, fallback ke pool
const q = (client, sql, params) => client
  ? client.query(sql, params)
  : db.query(sql, params);

// ===============================================================
// HANDLE BUY
// ===============================================================

export async function handleBuyHolder({ tokenAddress, wallet, amount, time, client }) {

  const { rows: existing } = await q(client, `
    SELECT 1 FROM token_holders
    WHERE token_address  = $1
    AND   holder_address = $2
    LIMIT 1
  `, [tokenAddress, wallet]);

  await q(client, `
    INSERT INTO token_holders (
      token_address,
      holder_address,
      balance,
      first_buy_time,
      last_updated
    )
    VALUES ($1, $2, $3, $4, NOW())
    ON CONFLICT (token_address, holder_address)
    DO UPDATE SET
      balance      = token_holders.balance + $3,
      last_updated = NOW()
  `, [tokenAddress, wallet, amount, time]);

  if (!existing.length) {
    await q(client, `
      UPDATE token_stats
      SET buyer_wallets = buyer_wallets + 1
      WHERE token_address = $1
    `, [tokenAddress]);
  }

}

// ===============================================================
// HANDLE SELL
// ===============================================================

export async function handleSellHolder({ tokenAddress, wallet, amount, time, client }) {

  const sellAmount = Math.abs(amount);

  await q(client, `
    UPDATE token_holders
    SET
      balance      = GREATEST(balance - $3, 0),
      last_updated = NOW()
    WHERE token_address  = $1
    AND   holder_address = $2
  `, [tokenAddress, wallet, sellAmount]);

  const { rows } = await q(client, `
    SELECT first_buy_time, is_paperhand
    FROM token_holders
    WHERE token_address  = $1
    AND   holder_address = $2
  `, [tokenAddress, wallet]);

  const firstBuy     = rows[0]?.first_buy_time;
  const alreadyPaper = rows[0]?.is_paperhand ?? false;

  if (firstBuy && !alreadyPaper) {
    const diff = new Date(time) - new Date(firstBuy);
    if (diff <= 30 * 60 * 1000) {
      await q(client, `
        UPDATE token_holders
        SET is_paperhand = true
        WHERE token_address  = $1
        AND   holder_address = $2
      `, [tokenAddress, wallet]);

      await q(client, `
        UPDATE token_stats
        SET
          paper_wallets = paper_wallets + 1,
          paperhand_pct = (paper_wallets + 1)::numeric
                          / NULLIF(buyer_wallets, 0) * 100
        WHERE token_address = $1
      `, [tokenAddress]);
    }
  }
}

// ===============================================================
// UPDATE HOLDER COUNT
// FIX: ganti subquery COUNT(*) → UPDATE langsung pakai delta
//
// Subquery COUNT(*) sebelumnya:
//   - Baca semua baris token_holders tiap TX → lambat kalau holder banyak
//   - Kalau TX ramai dan withClient timeout → count tidak ter-update
//   - Hasilnya bisa undercount kalau ada TX yang skip
//
// Sekarang pakai delta (+1 / -1 / 0) yang dihitung dari hasil
// INSERT/UPDATE token_holders — tidak perlu baca semua baris lagi
// ===============================================================

export async function updateHolderCount(tokenAddress, client, delta = 0) {
  if (delta === 0) return; // tidak ada perubahan → skip query
  await q(client, `
    UPDATE token_stats
    SET holder_count = GREATEST(holder_count + $2, 0)
    WHERE token_address = $1
  `, [tokenAddress, delta]);
}

// ── Repair holder_count dari DB (dipanggil saat warmup) ────────
// Hitung ulang dari token_holders yang actual — bukan dari cache
export async function repairHolderCount(tokenAddress) {
  await db.query(`
    UPDATE token_stats
    SET holder_count = (
      SELECT COUNT(*)
      FROM token_holders
      WHERE token_address = $1
        AND balance > 0
    )
    WHERE token_address = $1
  `, [tokenAddress]);
}

// ===============================================================
// GET PAPERHAND PCT
// ===============================================================

export async function getPaperHandPct(tokenAddress, client) {
  const { rows } = await q(client, `
    SELECT paperhand_pct
    FROM token_stats
    WHERE token_address = $1
  `, [tokenAddress]);
  return Number(rows[0]?.paperhand_pct || 0);
}