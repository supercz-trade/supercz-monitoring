import dotenv from "dotenv";
dotenv.config();

import { startPriceStream, waitPricesReady } from "./price/binancePrice.js";
import { startBlockDispatcher } from "./infra/blockDispatcher.js";

// ── watchdog state ─────────────────────────────
let lastHeartbeat = Date.now();
const APP_START = Date.now();

// ================= PRICE LAYER =================

async function initPriceLayer() {

  console.log("[PRICE] Initializing price polling...");

  // Mulai REST polling (update setiap 1 menit)
  startPriceStream();

  // Tunggu harga pertama masuk cache sebelum dispatcher jalan
  await waitPricesReady(10_000);

}

// ================= WATCHDOG =================

// heartbeat setiap 10 detik (indikasi event loop hidup)
setInterval(() => {
  lastHeartbeat = Date.now();
}, 10_000);

// watchdog monitor setiap 30 detik
setInterval(() => {

  const now = Date.now();
  const memMB = process.memoryUsage().rss / 1024 / 1024;

  // 1. event loop freeze
  if (now - lastHeartbeat > 60_000) {
    console.log("[WATCHDOG] Event loop stalled > 60s. Restarting...");
    process.exit(1);
  }

  // 2. memory guard
  if (memMB > 2000) {
    console.log(`[WATCHDOG] Memory ${memMB.toFixed(0)}MB exceeded limit. Restarting...`);
    process.exit(1);
  }

  // 3. runtime guard (30 menit)
  if (now - APP_START > 30 * 60 * 1000) {
    console.log("[WATCHDOG] Scheduled restart (30 min)...");
    process.exit(1);
  }

}, 30_000);

// ================= MAIN =================

async function main() {

  await initPriceLayer();

  console.log("[APP] QuantX Monitoring Started...");

  // start blockchain listener
  await startBlockDispatcher();

}

main().catch(err => {
  console.error("[FATAL]", err);
  process.exit(1);
});