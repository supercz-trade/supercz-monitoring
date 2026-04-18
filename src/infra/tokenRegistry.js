// ===============================================================
// tokenRegistry.js
// State untuk daftar token yang sudah migrasi ke PancakeSwap
// Dipisah dari blockDispatcher agar tidak circular import
// ===============================================================

let pancakeTokens = [];

export function setPancakeTokens(addresses) {
  pancakeTokens = addresses.map(a => a.toLowerCase());
}

export function addPancakeToken(address) {
  const lower = address.toLowerCase();
  if (!pancakeTokens.includes(lower)) {
    pancakeTokens.push(lower);
  }
}

export function getPancakeTokens() {
  return pancakeTokens;
}