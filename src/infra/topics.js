// ===============================================================
// topics.js
// Semua event topic signatures dikumpulkan di sini
// Dipisah agar tidak ada circular import antara
// blockDispatcher ↔ handler
// ===============================================================

import { ethers } from "ethers";

export const TOPICS = {
  TOKEN_CREATED: ethers.id("TokenCreated(uint256,address,uint256,address,string,string,string)"),
  TOKEN_BOUGHT: ethers.id("TokenBought(uint256,address,address,uint256,uint256,uint256,uint256)"),
  TOKEN_SOLD: ethers.id("TokenSold(uint256,address,address,uint256,uint256,uint256,uint256)"),
  LAUNCHED_TO_DEX: ethers.id("LaunchedToDEX(address,address,uint256,uint256)"),
  TOKEN_QUOTE_SET: ethers.id("TokenQuoteSet(address,address)"),
  TAX_SET: ethers.id("FlapTokenTaxSet(address,uint256)"),
  PROGRESS_CHANGED: ethers.id("FlapTokenProgressChanged(address,uint256)"),
  TRANSFER_FLAP: ethers.id("TransferFlapToken(address,address,uint256)"),
  ERC20_TRANSFER: ethers.id("Transfer(address,address,uint256)"),
  SYNC        : ethers.id("Sync(uint112,uint112)"),
  PAIR_CREATED: ethers.id("PairCreated(address,address,address,uint256)"),
  SWAP: ethers.id("Swap(address,uint256,uint256,uint256,uint256,address)"),
};