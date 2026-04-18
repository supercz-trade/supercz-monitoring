const GATEWAYS = [
  "https://ipfs.io/ipfs/",
  "https://cloudflare-ipfs.com/ipfs/",
  "https://gateway.pinata.cloud/ipfs/"
]; // [ADDED]

function normalizeCID(input) { // [ADDED]
  if (!input) return null;

  let cid = input.trim();

  if (cid.startsWith("ipfs://")) {
    cid = cid.replace("ipfs://", "");
  }

  if (cid.includes("/ipfs/")) {
    cid = cid.split("/ipfs/")[1];
  }

  return cid;
}

async function fetchWithTimeout(url, ms = 8000) { // [ADDED]
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), ms);

  try {
    const res = await fetch(url, { signal: controller.signal });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  } finally {
    clearTimeout(id);
  }
}

export async function fetchIPFSMeta(cid) { // [ADDED]
  const normalized = normalizeCID(cid);
  if (!normalized) return null;

  for (const gw of GATEWAYS) {
    const url = gw + normalized;

    const json = await fetchWithTimeout(url);

    if (json) return json;
  }

  return null;
}