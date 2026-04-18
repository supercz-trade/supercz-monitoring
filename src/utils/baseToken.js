// ================= BASE TOKEN WHITELIST =================

export const BASE_TOKEN_WHITELIST = {

  BNB: [
    "0x0000000000000000000000000000000000000000", // native BNB
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"  // WBNB
  ],

  USDT: [
    "0x55d398326f99059ff775485246999027b3197955"
  ],

  USD1: [
    "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d"
  ],

  USDC: [
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d"
  ],

  ASTER: [
    "0x000ae314e2a2172a039b26378814c252734f556a"
  ],

  CAKE: [
    "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82"
  ],

  U: [
    "0xce24439f2d9c6a2289f741120fe202248b666666"
  ],
  UUSD: [
    "0x61a10E8556BEd032eA176330e7F17D6a12a10000"
  ]
};


// ================= ADDRESS → SYMBOL MAP =================

const BASE_ADDRESS_MAP = {};

for (const [symbol, addresses] of Object.entries(BASE_TOKEN_WHITELIST)) {

  for (const addr of addresses) {

    BASE_ADDRESS_MAP[addr.toLowerCase()] = symbol;

  }

}


// ================= BASE PAIR RESOLVER =================

export function getBasePair(address) {

  if (!address) return null;

  const lower = address.toLowerCase();

  return BASE_ADDRESS_MAP[lower] || null;

}


// ================= BASE ADDRESS NORMALIZER =================

export function normalizeBaseAddress(address) {

  if (!address) return null;

  return address.toLowerCase();

}