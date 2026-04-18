// ===============================================================
// wsServer.js
// FIX: channel "candle:{address}" divalidasi dengan benar
// ===============================================================

import { subscribe, unsubscribe, unsubscribeAll, getBrokerStats } from "./wsbroker.js";

// [MODIFIED]

const VALID_CHANNELS = new Set([
  "new_token",
  "token_update",   // [ADDED]
  "transaction:all",
  "migrate",
]);

function isValidChannel(channel) {
  if (VALID_CHANNELS.has(channel)) return true;

  // Channel dengan address: transaction:0x / price:0x / holder:0x / candle:0x
  const colonIdx = channel.indexOf(":");
  if (colonIdx === -1) return false;

  const type    = channel.slice(0, colonIdx);
  const address = channel.slice(colonIdx + 1);

  if (!address || address.length < 10) return false;

  return ["transaction", "transactions", "price", "holder", "candle"].includes(type);
}

export function registerWsRoutes(fastify) {

  fastify.get("/ws", { websocket: true }, (socket, req) => {

    console.log("[WS] Client connected:", req.ip);

    // ── WELCOME ──────────────────────────────────────────────────
    socket.send(JSON.stringify({
      action  : "connected",
      message : "QuantX WS ready. Send { action: 'subscribe', channel: '...' }",
      channels: [
        "new_token",
        "transaction:all",
        "transaction:{tokenAddress}",
        "price:{tokenAddress}",
        "holder:{tokenAddress}",
        "candle:{tokenAddress}",
        "migrate",
        "token_update",
      ]
    }));

    // ── INCOMING MESSAGE ──────────────────────────────────────────
    socket.on("message", (raw) => {

      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        socket.send(JSON.stringify({ error: "invalid_json" }));
        return;
      }

      const { action, channel } = msg;

      if (!action || !channel) {
        socket.send(JSON.stringify({ error: "missing_action_or_channel" }));
        return;
      }

      if (!isValidChannel(channel)) {
        socket.send(JSON.stringify({ error: "invalid_channel", channel }));
        return;
      }

      if (action === "subscribe") {
        subscribe(channel, socket);
        socket.send(JSON.stringify({ ok: true, action: "subscribed", channel }));
        return;
      }

      if (action === "unsubscribe") {
        unsubscribe(channel, socket);
        socket.send(JSON.stringify({ ok: true, action: "unsubscribed", channel }));
        return;
      }

      if (action === "ping") {
        socket.send(JSON.stringify({ action: "pong", ts: Date.now() }));
        return;
      }

      socket.send(JSON.stringify({ error: "unknown_action", action }));
    });

    // ── DISCONNECT ────────────────────────────────────────────────
    socket.on("close", () => {
      unsubscribeAll(socket);
      console.log("[WS] Client disconnected:", req.ip);
    });

    socket.on("error", (err) => {
      console.error("[WS] Socket error:", err.message);
      unsubscribeAll(socket);
    });
  });

  // ── WS STATS ─────────────────────────────────────────────────────
  fastify.get("/ws/stats", async () => getBrokerStats());
}