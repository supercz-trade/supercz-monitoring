import { syncBondingFromHelper } from "./bondingHelper.service.js";

const pending = new Map();

export function scheduleBondingSync(tokenAddress) {
  if (pending.has(tokenAddress)) return;

  pending.set(tokenAddress, true);

  setTimeout(async () => {
    try {
      await syncBondingFromHelper(tokenAddress);
    } catch (err) {
      console.error("[BONDING SYNC ERROR]", err.message);
    } finally {
      pending.delete(tokenAddress);
    }
  }, 1500);
}