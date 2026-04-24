// ===============================================================
// server.js  ← entry point (FIXED: warmup blocking, no race)
// ===============================================================

import dotenv from "dotenv";
dotenv.config();

import Fastify  from "fastify";
import cors     from "@fastify/cors";
import wsPlugin from "@fastify/websocket";

import { startPriceStream, waitPricesReady }     from "./price/binancePrice.js";
import { startBlockDispatcher }                  from "./infra/blockDispatcher.js";
import { registerWsRoutes }                      from "./infra/wsServer.js";
import { warmupStatsCache }                      from "./repository/transaction.repository.js";
import { warmupLiquidityCache } from "./cache/liquidity.cache.js";
import { startCandleFlush, warmupLastClose }     from "./repository/candleBuilder.js";
import { startCandleRepair }                     from "./repository/candleRepair.js";

import { getCandles, getEvents, getEventsByAddress }  from "./api/candles.route.js";
import { getHolders }             from "./api/holders.route.js";
import { getWalletOverview } from "./api/wallet.route.js";
import { getTopTraders } from "./api/top_traders.route.js";
import {
  getNewTokens,
  getTokenInfo,
  getTokensMigrating,
  getTokensMigrated
} from "./api/tokens.route.js";
import {
  getTransactionsByToken,
  getTransactionsByWallet,
  getTransactionsByTokenAndWallet
} from "./api/transactions.route.js";

import { getPlatformStats, getPlatformVolumeChart } from "./api/platform.route.js";
import { getGasPrice } from "./api/utils_route.js"; // [ADDED]
import debugRoutes from "./api/debug.routes.js";

import fs   from "fs";
import path from "path";

// ===============================================================
// Candle log to file
// ===============================================================

const logDir    = "./logs";
const candleLog = path.join(logDir, `candle-${new Date().toISOString().slice(0,10)}.log`);

if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir, { recursive: true });
}

const candleStream = fs.createWriteStream(candleLog, { flags: "a" });
const _origWrite   = process.stdout.write.bind(process.stdout);

process.stdout.write = (chunk, ...args) => {
  if (typeof chunk === "string" && chunk.includes("[CANDLE")) {
    candleStream.write(chunk);
  }
  return _origWrite(chunk, ...args);
};

console.log(`[LOG] Candle log → ${candleLog}`);

// ===============================================================

const fastify = Fastify({ logger: true });

// ===============================================================
// Graceful shutdown
// ===============================================================

process.on("SIGINT",  () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

async function shutdown(signal) {
  console.log(`[SERVER] ${signal} received, shutting down gracefully...`);
  await fastify.close();
  process.exit(0);
}

// ===============================================================
// MAIN
// ===============================================================

async function main() {

  // ============================================================
  // 1. Price layer
  // ============================================================

  console.log("[PRICE] Initializing price polling...");
  startPriceStream();
  await waitPricesReady(10_000);
  console.log("[PRICE] Prices ready");

  // ============================================================
  // 2. Warmup stats cache (FIXED)
  // ============================================================

  console.log("[CACHE] Warming up stats cache from DB...");
  await warmupStatsCache();
  console.log("[CACHE] Stats cache ready");

  await warmupLiquidityCache();
  console.log("[CACHE] Liquidity cache ready");

  // ============================================================
  // 3. Warmup candle last close
  // ============================================================

  console.log("[CANDLE] Warming up last close prices...");
  await Promise.all([
    warmupLastClose()
  ]);
  console.log("[CANDLE] Last close ready");

  // ============================================================
  // 4. Register plugins
  // ============================================================

  await fastify.register(cors, {
    origin: ["https://supercz.pro","http://localhost:5173"],
    methods: ["GET","POST","PUT","DELETE","OPTIONS"],
    allowedHeaders: ["Content-Type","Authorization"],
    credentials: false
  });

  await fastify.register(wsPlugin, {
    options: {
      verifyClient: (info, cb) => cb(true)
    }
  });

  // ============================================================
  // 5. REST routes
  // ============================================================

  fastify.get("/tokens/new",        getNewTokens);
  fastify.get("/tokens/migrating",  getTokensMigrating);
  fastify.get("/tokens/migrated",   getTokensMigrated);
  fastify.get("/tokens/:address",   getTokenInfo);

  fastify.get("/tokens/:address/candles",       getCandles);

  fastify.get("/tokens/:address/holders",       getHolders);
  fastify.get("/tokens/:address/top-traders",   getTopTraders);

  fastify.get("/tokens/:address/transactions",  getTransactionsByToken);
  fastify.get("/tokens/:address/transactions/:wallet",  getTransactionsByTokenAndWallet);
  
  fastify.get("/wallets/:address/overview", getWalletOverview);
  fastify.get("/wallets/:address/transactions", getTransactionsByWallet);

  fastify.get("/tokens/:address/events",         getEvents);
  fastify.get("/tokens/:address/events/:wallet", getEventsByAddress);
  fastify.get("/platform/stats",  getPlatformStats);
  fastify.get("/platform/chart",  getPlatformVolumeChart);

  // [ADDED] Gas price endpoint — cache 15s, hemat RPC
  fastify.get("/utils/gas-price", getGasPrice);

  // [ADDED] health check endpoint untuk monitoring
  fastify.get("/health", async () => ({ status: "ok" }));

  await fastify.register(debugRoutes);

  // ============================================================
  // 6. WebSocket routes
  // ============================================================

  registerWsRoutes(fastify);

  // ============================================================
  // 7. Start server
  // ============================================================

  await fastify.listen({ port: 3000, host: "0.0.0.0" });

  const BASE_URL = process.env.BASE_URL || "http://localhost:3000";
  console.log(`[SERVER] API   → ${BASE_URL}`);
  const WS_BASE = BASE_URL.startsWith("https")
    ? BASE_URL.replace("https", "wss")
    : BASE_URL.replace("http", "ws");
  console.log(`[SERVER] WS    → ${WS_BASE}/ws`);
  console.log(`[SERVER] Stats → ${BASE_URL}/ws/stats`);

  // ============================================================
  // 8. Start candle systems
  // ============================================================

  startCandleFlush();
  startCandleRepair();
  console.log("[CANDLE] Flush loop started");

  // ============================================================
  // 9. Start block dispatcher
  // ============================================================

  console.log("[APP] QuantX Monitoring Started...");
  await startBlockDispatcher();

}

// ===============================================================

main().catch(err => {
  console.error("[FATAL]", err);
  process.exit(1);
});