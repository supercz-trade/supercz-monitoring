// ===============================================================
// txBuffer.js (ORDER FIX + DEBUG BUFFER)
// ===============================================================

import { updateCandle } from "./candleBuilder.js";
import { pushAggLog } from "../infra/aggDebugBuffer.js"; // [ADDED]

const TX_BUFFER_DELAY_MS = 200;
const buffers = new Map();

function getKey(tokenAddress) {
  return tokenAddress.toLowerCase();
}

// ===============================================================
// PUSH
// ===============================================================

export function pushTxToBuffer(tx) {

  if (!tx || !tx.tokenAddress) return; // [ADDED]

  const key = getKey(tx.tokenAddress);

  if (!buffers.has(key)) {
    buffers.set(key, []);
  }

  buffers.get(key).push(tx);

  // [ADDED] DEBUG PUSH
  pushAggLog({
    stage: "TX_PUSH",
    tokenAddress: key,
    time: tx.time,
    price: tx.priceUSDT,
    block: tx.blockNumber,
    logIndex: tx.logIndex || 0
  });

  scheduleFlush(key);
}

// ===============================================================
// FLUSH
// ===============================================================

function scheduleFlush(key) {

  const buf = buffers.get(key);
  if (!buf || buf._scheduled) return;

  buf._scheduled = true;

  setTimeout(() => {

    try {

      if (!buf.length) {
        buffers.set(key, []);
        return;
      }

      // ===========================================================
      // [DEBUG] BEFORE SORT
      // ===========================================================
      pushAggLog({
        stage: "TX_BUFFER_BEFORE_SORT",
        tokenAddress: key,
        size: buf.length,
        first: buf[0],
        last: buf[buf.length - 1]
      });

      // ===========================================================
      // SORT (blockNumber + logIndex)
      // ===========================================================
      buf.sort((a, b) => {
        if (a.blockNumber !== b.blockNumber) {
          return a.blockNumber - b.blockNumber;
        }
        return (a.logIndex || 0) - (b.logIndex || 0);
      });

      // ===========================================================
      // [DEBUG] AFTER SORT
      // ===========================================================
      pushAggLog({
        stage: "TX_BUFFER_AFTER_SORT",
        tokenAddress: key,
        first: buf[0],
        last: buf[buf.length - 1]
      });

      // ===========================================================
      // ORDER VALIDATION
      // ===========================================================
      for (let i = 1; i < buf.length; i++) {

        if (buf[i].time < buf[i - 1].time) {

          pushAggLog({
            stage: "TX_ORDER_ISSUE",
            tokenAddress: key,
            prev: {
              time: buf[i - 1].time,
              block: buf[i - 1].blockNumber,
              logIndex: buf[i - 1].logIndex
            },
            curr: {
              time: buf[i].time,
              block: buf[i].blockNumber,
              logIndex: buf[i].logIndex
            }
          });
        }
      }

      // ===========================================================
      // PROCESS TX → CANDLE
      // ===========================================================
      for (const tx of buf) {

        // [ADDED] TRACE FLOW TO CANDLE
        pushAggLog({
          stage: "TX_TO_CANDLE",
          tokenAddress: tx.tokenAddress,
          time: tx.time,
          price: tx.priceUSDT,
          block: tx.blockNumber,
          logIndex: tx.logIndex || 0
        });

        updateCandle({
          tokenAddress: tx.tokenAddress,
          priceUSDT: tx.priceUSDT,
          inUSDTPayable: tx.inUSDTPayable,
          time: tx.time,
        });
      }

    } catch (err) {

      // [ADDED] ERROR TRACE
      pushAggLog({
        stage: "TX_BUFFER_ERROR",
        tokenAddress: key,
        error: err.message
      });

    } finally {

      // reset buffer
      buffers.set(key, []);
    }

  }, TX_BUFFER_DELAY_MS);
}