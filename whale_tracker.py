"""
whale_tracker.py — Monitors a specific Solana wallet address for new buys.
"""

import asyncio
import logging
import aiohttp
import time
from typing import Optional

# Setup logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [WHALE] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("whale")

# Constants
HELIUS_API_KEY = "87eeee41-3eb7-499f-9fad-0c242d9be5bf"
RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

# Placeholder for the user to edit
TARGET_WALLET = "YOUR_WHALE_WALLET_ADDRESS_HERE"

# Keep track of the last processed signature to avoid duplicates
last_signature = None

async def get_latest_signatures(session: aiohttp.ClientSession, wallet: str, limit: int = 5):
    """Fetch the latest transaction signatures for the wallet."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [
            wallet,
            {"limit": limit}
        ]
    }
    try:
        async with session.post(RPC_URL, json=payload) as resp:
            data = await resp.json()
            return data.get("result", [])
    except Exception as e:
        log.error(f"Error fetching signatures: {e}")
        return []

async def get_transaction(session: aiohttp.ClientSession, signature: str):
    """Fetch the full parsed transaction to check token balances."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}
        ]
    }
    try:
        async with session.post(RPC_URL, json=payload) as resp:
            data = await resp.json()
            return data.get("result")
    except Exception as e:
        log.error(f"Error fetching transaction {signature}: {e}")
        return None

async def parse_buy_from_tx(tx: dict, wallet: str) -> Optional[str]:
    """
    Looks at pre and post token balances. 
    If the wallet gained a token (balance increased) and lost SOL, it's a buy.
    Returns the token mint address if found.
    """
    try:
        meta = tx.get("meta")
        if not meta or meta.get("err") is not None:
            return None # Transaction failed

        pre_balances = meta.get("preTokenBalances", [])
        post_balances = meta.get("postTokenBalances", [])

        # Find balances belonging to the target wallet
        pre_map = {b["mint"]: b["uiTokenAmount"]["uiAmount"] for b in pre_balances if b.get("owner") == wallet}
        post_map = {b["mint"]: b["uiTokenAmount"]["uiAmount"] for b in post_balances if b.get("owner") == wallet}

        for mint, post_amt in post_map.items():
            pre_amt = pre_map.get(mint, 0.0)
            if post_amt and pre_amt is not None and post_amt > pre_amt:
                # The wallet's balance for this mint increased! This is a buy.
                return mint
                
    except Exception as e:
        log.error(f"Error parsing tx: {e}")
    
    return None

async def monitor_whale(wallet_pubkey: str = "", private_key_b58: str = ""):
    """Main loop to monitor the target wallet."""
    global last_signature
    
    if TARGET_WALLET == "YOUR_WHALE_WALLET_ADDRESS_HERE":
        log.info("No whale wallet configured. Edit whale_tracker.py to set TARGET_WALLET.")
        return

    log.info(f"Starting Smart Whale Tracker for wallet: {TARGET_WALLET[:8]}...")
    
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Initialize last_signature so we don't process old txs
        sigs = await get_latest_signatures(session, TARGET_WALLET, limit=1)
        if sigs:
            last_signature = sigs[0]["signature"]
            
        while True:
            await asyncio.sleep(2) # Poll every 2 seconds
            
            sigs = await get_latest_signatures(session, TARGET_WALLET, limit=10)
            if not sigs:
                continue
                
            new_sigs = []
            for sig_info in sigs:
                sig = sig_info["signature"]
                if sig == last_signature:
                    break
                new_sigs.append(sig)
                
            if new_sigs:
                last_signature = new_sigs[0] # Update pointer to the absolute newest
                
                # Process the new signatures (oldest first so we buy in order)
                for sig in reversed(new_sigs):
                    tx = await get_transaction(session, sig)
                    if tx:
                        mint = await parse_buy_from_tx(tx, TARGET_WALLET)
                        if mint:
                            log.info(f"WHALE BUY DETECTED! Token: {mint}")
                            # Send it to autotrader to instantly buy
                            from autotrader import on_whale_buy
                            asyncio.create_task(on_whale_buy(
                                session=None, 
                                mint=mint, 
                                wallet_pubkey=wallet_pubkey, 
                                private_key_b58=private_key_b58
                            ))
