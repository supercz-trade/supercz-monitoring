import pkg from "pg";
import dotenv from "dotenv";

dotenv.config();

const { Pool } = pkg;

export const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },

  max: 30,
  min: 2,

  connectionTimeoutMillis: 10_000,
  idleTimeoutMillis:       20_000,  // buang idle SEBELUM server DB kill (biasanya 30-60s)
  statement_timeout:       30_000,
  keepAlive:               true,
  keepAliveInitialDelayMillis: 5_000,
});

db.on("error", (err) => {
  // "terminated unexpectedly" — koneksi di-kill dari sisi server DB
  // Pool akan otomatis buat koneksi baru saat dibutuhkan
  console.error("[DB POOL] unexpected error:", err.message);
});

// Force UTC pada setiap koneksi baru dari pool
// Ini fix root cause timezone WIB — tidak bergantung pada ALTER DATABASE
db.on("connect", (client) => {
  client.query("SET timezone = 'UTC'").catch(err =>
    console.error("[DB POOL] set timezone error:", err.message)
  );
});

// ── Keepalive query ────────────────────────────────────────────
// Cegah server DB kill idle connection
// Jalan setiap 15 detik — lebih pendek dari idle timeout manapun
setInterval(async () => {
  try { await db.query("SELECT 1"); } catch (_) {}
}, 15_000);

// ── Pool stats log ─────────────────────────────────────────────
setInterval(() => {
  const { totalCount, idleCount, waitingCount } = db;
  if (waitingCount > 0 || totalCount > 10) {
    console.warn(`[DB POOL] total=${totalCount} idle=${idleCount} waiting=${waitingCount}`);
  }
}, 10_000);

// ===============================================================
// withClient
// ===============================================================

export async function withClient(fn) {
  const client = await db.connect();
  try {
    return await fn(client);
  } finally {
    client.release();
  }
}

// ===============================================================
// withClientRetry — retry otomatis saat koneksi putus mendadak
//
// Aman dipakai untuk:
//   ✓ insertTransaction  (ON CONFLICT DO NOTHING — idempotent)
//   ✓ getLaunchByToken   (SELECT — selalu aman)
//   ✓ _flushAggBatch     (ON CONFLICT DO UPDATE — idempotent)
//
// JANGAN pakai untuk UPDATE counter tanpa idempotency guard
// ===============================================================

const RETRYABLE_ERRORS = [
  "Connection terminated unexpectedly",
  "Connection terminated due to connection timeout",
  "timeout exceeded when trying to connect",
  "Client was closed and is not queryable",
];

function isRetryable(err) {
  return RETRYABLE_ERRORS.some(msg => err?.message?.includes(msg));
}

export async function withClientRetry(fn, maxRetries = 3) {
  let lastErr;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const client = await db.connect();
      try {
        return await fn(client);
      } finally {
        client.release();
      }
    } catch (err) {
      lastErr = err;
      if (!isRetryable(err)) throw err;
      const delay = attempt * 500;
      console.warn(`[DB RETRY] attempt ${attempt}/${maxRetries} — ${err.message} — retry in ${delay}ms`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  throw lastErr;
}

// ===============================================================
// withTransaction
// ===============================================================

export async function withTransaction(fn) {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}