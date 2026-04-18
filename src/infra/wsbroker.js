// ===============================================================
// wsbroker.js
// Pub/Sub internal untuk broadcast event ke WebSocket clients
// Handler (flap/fourmeme/pancake) emit ke sini,
// WS server push ke client yang subscribe channel yang relevan
//
// Channel yang tersedia:
//   "new_token"              → token baru listing
//   "transaction:{address}"  → trade untuk token tertentu
//   "transaction:all"        → semua trade semua token
//   "price:{address}"        → update harga token tertentu
//   "holder:{address}"       → update holder token tertentu
//   "migrate"                → token migrasi ke DEX
// ===============================================================

const subscribers = new Map(); // channel → Set<ws>

// ================= SUBSCRIBE =================

export function subscribe(channel, ws) {
  if (!subscribers.has(channel)) subscribers.set(channel, new Set());
  subscribers.get(channel).add(ws);
}

export function unsubscribe(channel, ws) {
  subscribers.get(channel)?.delete(ws);
}

export function unsubscribeAll(ws) {
  for (const subs of subscribers.values()) {
    subs.delete(ws);
  }
}

// ================= PUBLISH =================

export function publish(channel, data) {
  const subs = subscribers.get(channel);
  if (!subs || subs.size === 0) return;

  const payload = JSON.stringify({ channel, data, ts: Date.now() });

  for (const ws of subs) {
    if (ws.readyState === 1) {  // OPEN
      ws.send(payload);
    }
  }
}

// ================= HOLDER PUBLISH HELPER =================
// Dipanggil setelah updateHolderBalance agar client dapat update real-time

export function publishHolderUpdate(tokenAddress, holderAddress, delta) {
  publish(`holder:${tokenAddress.toLowerCase()}`, {
    tokenAddress : tokenAddress.toLowerCase(),
    holderAddress: holderAddress.toLowerCase(),
    delta        : Number(delta),
    ts           : Date.now()
  });
}

// ================= STATS =================

export function getBrokerStats() {
  const result = {};
  for (const [channel, subs] of subscribers.entries()) {
    result[channel] = subs.size;
  }
  return result;
}