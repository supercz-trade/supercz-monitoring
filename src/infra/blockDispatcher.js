// ===============================================================
// blockDispatcher.js
// Dispatcher untuk Flap, Four.meme, dan Pancake (token migrate)
// ===============================================================

import { onBlock } from "./provider.js";
import { getLogs, getBlock, getQueueStats } from "./rpcQueue.js";

import { handleFlapBlock } from "../listener/flapHandler.js";
import { handleFourmemeBlock } from "../listener/fourmemeHandler.js";
import { handleTokenMigratedBlock } from "../listener/pancakeHandler.js";
import { runFlapCleanup } from "../listener/flapCleanup.job.js";

const FLAP_PORTAL = process.env.FLAP_PORTAL?.toLowerCase();
const FOUR_MANAGER = process.env.FOUR_MEME_MANAGER?.toLowerCase();

// FIX: seenTx dipisah per platform supaya satu platform ramai
// tidak mempengaruhi deduplication platform lain.
// FIX: ganti .clear() dengan FIFO sliding window — hapus entry
// terlama satu per satu, bukan hapus semua sekaligus.

const MAX_SEEN = 5000;

function makeSeen() {
  const set = new Set();
  return function markSeen(txHash) {
    if (set.has(txHash)) return false;
    set.add(txHash);
    if (set.size > MAX_SEEN) {
      set.delete(set.values().next().value);
    }
    return true;
  };
}

const markSeenFlap = makeSeen();
const markSeenFourmeme = makeSeen();
const markSeenPancake = makeSeen();

// ================= START =================

export async function startBlockDispatcher() {

  console.log("[DISPATCHER] Starting unified block listener...");
  console.log("[DISPATCHER] FLAP_PORTAL  :", FLAP_PORTAL);
  console.log("[DISPATCHER] FOUR_MANAGER :", FOUR_MANAGER);

  await handleFlapBlock.init();

  // load pair registry untuk token yang sudah migrate ke pancake
  await handleTokenMigratedBlock.init();

  setTimeout(runFlapCleanup, 65 * 60 * 1000);

  // tetap jalankan sekali saat startup untuk cleanup sisa restart sebelumnya
  runFlapCleanup();

  setInterval(runFlapCleanup, 5 * 60 * 1000);

  // FIX: pakai onBlock() dari provider — listener otomatis dipasang
  // ulang ke provider baru saat reconnect terjadi.
  // Sebelumnya: wssProvider.on("block", ...) langsung → setelah
  // reconnect, wssProvider sudah provider baru tapi listener masih
  // terdaftar di provider lama yang sudah mati.
  onBlock(async (blockNumber) => {
    try {
      await onBlock_handler(blockNumber);
    } catch (err) {
      console.error("[DISPATCHER] Block error:", err.message);
    }
  });

}

// ================= PER BLOCK =================

async function onBlock_handler(blockNumber) {

  const fixedAddresses = [FLAP_PORTAL, FOUR_MANAGER].filter(Boolean);

  const platformLogs =
    fixedAddresses.length
      ? await getLogs({
        address: fixedAddresses,
        fromBlock: blockNumber,
        toBlock: blockNumber
      })
      : [];

  const block = await getBlock(blockNumber);
  if (!block) return;

  const flapLogs = platformLogs.filter(l => l.address.toLowerCase() === FLAP_PORTAL);
  const fourmemeLogs = platformLogs.filter(l => l.address.toLowerCase() === FOUR_MANAGER);

  const flapTxMap = groupByTx(flapLogs, markSeenFlap);
  const fourmemeTxMap = groupByTx(fourmemeLogs, markSeenFourmeme);

  const tasks = [];

  if (flapTxMap.size)
    tasks.push(handleFlapBlock({ txMap: flapTxMap, block, blockNumber }));

  if (fourmemeTxMap.size)
    tasks.push(handleFourmemeBlock({ txMap: fourmemeTxMap, block, blockNumber }));

  // trade langsung ke kontrak token flap
  tasks.push(handleFlapBlock.scanDirect({ block, blockNumber }));

  // swap dari token yang sudah migrate ke Pancake
  tasks.push(handleTokenMigratedBlock({ blockNumber }));

  await Promise.allSettled(tasks);

  if (blockNumber % 50 === 0) {
    console.log("[QUEUE STATS]", getQueueStats());
  }

}

// ================= HELPER =================

function groupByTx(logs, markSeen) {

  const map = new Map();

  for (const log of logs) {

    const txHash = log.transactionHash;

    if (!markSeen(txHash)) continue;

    if (!map.has(txHash)) map.set(txHash, []);

    map.get(txHash).push(log);

  }

  return map;

}