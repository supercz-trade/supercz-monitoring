// ===============================================================
// flapCleanup.job.js
// Cleanup token flap yang mati — tidak ada aktivitas > 1 jam
// setelah firstBuy. Jalankan via setInterval dari index.js
// ===============================================================

import { getDeadTokenFlap, deleteManyTokenFlap } from "../repository/tokenFlap.repository.js";
import { flapTokenSet } from "./flapHandler.js";

export async function runFlapCleanup() {

  try {

    const dead = await getDeadTokenFlap();

    if (!dead.length) {
      console.log("[FLAP CLEANUP] No dead tokens found");
      return;
    }

    console.log("[FLAP CLEANUP] Dead tokens:", dead.length);

    // hapus dari DB
    await deleteManyTokenFlap(dead);

    // hapus dari memory
    for (const token of dead) {
      flapTokenSet.delete(token);
    }

    console.log("[FLAP CLEANUP] Done, removed:", dead.join(", "));

  } catch (err) {
    console.error("[FLAP CLEANUP ERROR]", err.message);
  }

}