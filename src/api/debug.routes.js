// ===============================================================
// debug.routes.js
// ===============================================================

import {
  getAggLogs,
  clearAggLogs,
  exportAggLogs
} from "../infra/aggDebugBuffer.js";

// [ADDED]
export default async function (fastify) {

  fastify.get("/debug/agg-logs", async (req) => {
    const { token, stage, tf, limit } = req.query;

    return getAggLogs({
      tokenAddress: token || null,
      stage: stage || null,
      tf: tf || null,
      limit: Number(limit) || 500,
    });
  });

  fastify.post("/debug/agg-logs/clear", async () => {
    clearAggLogs();
    return { ok: true };
  });

  fastify.get("/debug/agg-logs/export", async () => {
    return exportAggLogs();
  });
}