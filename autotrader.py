"""
autotrader.py — Automated trading bot for 100-score memecoin strategy.

Strategy:
  - Auto-buy 100-score coins that show upward 1-min momentum
  - Auto-sell half at 2x (locks in original investment)
  - Auto-stop-loss at -50% (cuts dead coins fast)
  - Auto-sell moonbag after 24 hours

Modes:
  PAPER_MODE = True  → simulate trades, no real money (safe to test)
  PAPER_MODE = False → real trades via Jupiter API (requires wallet setup)
"""

import asyncio
import json
import time
import os
import logging
from typing import Dict, Optional
from dataclasses import dataclass, field

import aiohttp

# ── CONFIG ────────────────────────────────────────────────────────────────────
PAPER_MODE       = True          # Set False to trade real SOL
TRADE_AMOUNT_SOL = 0.02          # SOL per trade
HALF_SELL_AT     = 2.0           # Sell half when coin reaches 2x
STOP_LOSS_AT     = 0.50          # Sell everything if coin drops to 50% of entry
MOONBAG_EXIT_HRS = 24            # Sell moonbag after this many hours
MIN_1MIN_MULT    = 1.0           # Min 1-min mult to enter (1.0 = any, 1.2 = must be pumping)
POLL_SECS        = 8             # How often to check open trade prices (seconds)

# Solana RPC (free public endpoint — replace with paid RPC for better reliability)
SOLANA_RPC       = "https://api.mainnet-beta.solana.com"

# Jupiter API
JUPITER_QUOTE    = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP     = "https://quote-api.jup.ag/v6/swap"
DEXSCREENER_URL  = "https://api.dexscreener.com/latest/dex/tokens/{}"

# SOL mint address
SOL_MINT         = "So11111111111111111111111111111111111111112"

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TRADER] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("autotrader")

# ── TRADE STATE ───────────────────────────────────────────────────────────────
@dataclass
class Trade:
    mint:         str
    name:         str
    entry_mc:     float
    entry_time:   float = field(default_factory=time.time)
    sol_invested: float = TRADE_AMOUNT_SOL
    half_sold:    bool  = False
    sol_returned: float = 0.0
    closed:       bool  = False
    close_reason: str   = ""

    @property
    def elapsed_hrs(self) -> float:
        return (time.time() - self.entry_time) / 3600

    @property
    def pnl(self) -> float:
        return self.sol_returned - self.sol_invested

open_trades: Dict[str, Trade] = {}
closed_trades: list = []


# ── PRICE FETCHER ─────────────────────────────────────────────────────────────
async def fetch_current_mc(session: aiohttp.ClientSession, mint: str) -> Optional[float]:
    try:
        async with session.get(
            DEXSCREENER_URL.format(mint),
            timeout=aiohttp.ClientTimeout(total=8)
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)
            pairs = data.get("pairs") or []
            if not pairs:
                return None
            best = max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
            mc = best.get("marketCap") or best.get("fdv")
            return float(mc) if mc else None
    except Exception:
        return None


# ── JUPITER SWAP EXECUTION ────────────────────────────────────────────────────
async def execute_swap(
    session: aiohttp.ClientSession,
    input_mint: str,
    output_mint: str,
    amount_sol: float,
    wallet_pubkey: str,
    private_key_b58: str
) -> Optional[str]:
    """Execute a real swap via Jupiter. Returns tx signature or None."""
    if PAPER_MODE:
        log.info(f"[PAPER] Would swap {amount_sol:.4f} SOL: {input_mint[:8]}... -> {output_mint[:8]}...")
        return "PAPER_TX"

    try:
        # Step 1: Get quote
        lamports = int(amount_sol * 1_000_000_000)
        async with session.get(JUPITER_QUOTE, params={
            "inputMint":        input_mint,
            "outputMint":       output_mint,
            "amount":           lamports,
            "slippageBps":      500,   # 5% slippage
            "onlyDirectRoutes": "false"
        }) as resp:
            if resp.status != 200:
                log.error(f"Jupiter quote failed: {resp.status}")
                return None
            quote = await resp.json()

        # Step 2: Get swap transaction
        async with session.post(JUPITER_SWAP, json={
            "quoteResponse":          quote,
            "userPublicKey":          wallet_pubkey,
            "wrapAndUnwrapSol":       True,
            "computeUnitPriceMicroLamports": 200_000,  # priority fee
        }) as resp:
            if resp.status != 200:
                log.error(f"Jupiter swap failed: {resp.status}")
                return None
            swap_data = await resp.json()

        # Step 3: Sign and send transaction
        from solders.keypair import Keypair
        from solders.transaction import VersionedTransaction
        from solana.rpc.async_api import AsyncClient
        import base64

        keypair = Keypair.from_base58_string(private_key_b58)
        tx_bytes = base64.b64decode(swap_data["swapTransaction"])
        tx = VersionedTransaction.from_bytes(tx_bytes)
        signed_tx = keypair.sign_message(bytes(tx.message))

        client = AsyncClient(SOLANA_RPC)
        result = await client.send_raw_transaction(bytes(tx))
        await client.close()

        sig = str(result.value)
        log.info(f"TX sent: https://solscan.io/tx/{sig}")
        return sig

    except Exception as e:
        log.error(f"Swap error: {e}")
        return None


# ── BUY ───────────────────────────────────────────────────────────────────────
async def buy_coin(
    session: aiohttp.ClientSession,
    mint: str,
    name: str,
    entry_mc: float,
    wallet_pubkey: str = "",
    private_key_b58: str = ""
):
    if mint in open_trades:
        return  # already in this trade

    log.info(f"BUY | {name} | MC ${entry_mc/1000:.1f}k | {TRADE_AMOUNT_SOL} SOL {'[PAPER]' if PAPER_MODE else '[LIVE]'}")

    tx = await execute_swap(
        session, SOL_MINT, mint,
        TRADE_AMOUNT_SOL, wallet_pubkey, private_key_b58
    )
    if tx is None and not PAPER_MODE:
        log.error(f"Buy failed for {name}")
        return

    open_trades[mint] = Trade(
        mint=mint, name=name,
        entry_mc=entry_mc,
        sol_invested=TRADE_AMOUNT_SOL
    )


# ── SELL ──────────────────────────────────────────────────────────────────────
async def sell_coin(
    session: aiohttp.ClientSession,
    trade: Trade,
    fraction: float,
    reason: str,
    wallet_pubkey: str = "",
    private_key_b58: str = ""
):
    sol_out = trade.sol_invested * fraction * (
        # In paper mode, calculate the theoretical return based on current MC vs entry
        1.0  # We'll set this properly in the monitor loop
    )

    log.info(f"SELL {int(fraction*100)}% | {trade.name} | Reason: {reason} {'[PAPER]' if PAPER_MODE else '[LIVE]'}")

    if not PAPER_MODE:
        await execute_swap(
            session, trade.mint, SOL_MINT,
            trade.sol_invested * fraction,
            wallet_pubkey, private_key_b58
        )


# ── TRADE MONITOR LOOP ────────────────────────────────────────────────────────
async def monitor_loop(wallet_pubkey: str = "", private_key_b58: str = ""):
    """Fast loop that checks all open trade prices every POLL_SECS seconds."""
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            to_close = []

            for mint, trade in list(open_trades.items()):
                if trade.closed:
                    to_close.append(mint)
                    continue

                current_mc = await fetch_current_mc(session, mint)
                if current_mc is None:
                    continue

                mult = current_mc / trade.entry_mc if trade.entry_mc > 0 else 0

                # ── STOP LOSS ─────────────────────────────────────────────
                if mult <= STOP_LOSS_AT and not trade.half_sold:
                    sol_back = trade.sol_invested * mult
                    trade.sol_returned += sol_back
                    trade.closed = True
                    trade.close_reason = f"Stop loss at {mult:.2f}x"
                    log.info(f"STOP LOSS | {trade.name} | {mult:.2f}x | PnL: {trade.pnl:+.4f} SOL")
                    await sell_coin(session, trade, 1.0, "stop loss", wallet_pubkey, private_key_b58)
                    to_close.append(mint)

                # ── HALF SELL AT 2x ───────────────────────────────────────
                elif not trade.half_sold and mult >= HALF_SELL_AT:
                    sol_back = (trade.sol_invested / 2) * HALF_SELL_AT  # lock in exact 2x on half
                    trade.sol_returned += sol_back
                    trade.half_sold = True
                    log.info(f"HALF SELL | {trade.name} | {mult:.2f}x | Locked in {sol_back:.4f} SOL")
                    await sell_coin(session, trade, 0.5, "half at 2x", wallet_pubkey, private_key_b58)

                # ── 24HR MOONBAG EXIT ─────────────────────────────────────
                elif trade.elapsed_hrs >= MOONBAG_EXIT_HRS:
                    rem_fraction = 0.5 if trade.half_sold else 1.0
                    sol_back = (trade.sol_invested * rem_fraction) * mult
                    trade.sol_returned += sol_back
                    trade.closed = True
                    trade.close_reason = f"24hr exit at {mult:.2f}x"
                    log.info(f"24HR EXIT | {trade.name} | {mult:.2f}x | PnL: {trade.pnl:+.4f} SOL")
                    await sell_coin(session, trade, rem_fraction, "24hr moonbag", wallet_pubkey, private_key_b58)
                    to_close.append(mint)

                else:
                    log.info(f"WATCHING  | {trade.name} | {mult:.2f}x | {trade.elapsed_hrs:.1f}hr elapsed")

            # Move closed trades out
            for mint in to_close:
                if mint in open_trades:
                    closed_trades.append(open_trades.pop(mint))

            # Print running PnL summary
            if closed_trades:
                total_pnl = sum(t.pnl for t in closed_trades)
                log.info(f"=== PnL Summary: {len(closed_trades)} closed | {total_pnl:+.4f} SOL total ===")

            await asyncio.sleep(POLL_SECS)


# ── CALLED BY MAIN BOT WHEN A NEW 100-SCORE COIN IS FOUND ────────────────────
async def on_new_coin(
    session: Optional[aiohttp.ClientSession],
    mint: str,
    name: str,
    entry_mc: float,
    mc_1min: Optional[float] = None,
    wallet_pubkey: str = "",
    private_key_b58: str = ""
):
    """
    Called automatically by main.py when a new 100-score coin is detected.
    Checks 1-min momentum then buys if conditions are met.
    Creates its own session if none is provided.
    """
    own_session = session is None
    if own_session:
        connector = aiohttp.TCPConnector(ssl=False)
        session = aiohttp.ClientSession(connector=connector)

    try:
        if mc_1min is None:
            log.info(f"WAITING 1min | {name} | MC ${entry_mc/1000:.1f}k")
            await asyncio.sleep(65)
            mc_1min = await fetch_current_mc(session, mint)

        if mc_1min is None:
            log.info(f"SKIP (no price data) | {name}")
            return

        mult_1min = mc_1min / entry_mc if entry_mc > 0 else 0

        if mult_1min <= STOP_LOSS_AT:
            log.info(f"SKIP (dumping {mult_1min:.2f}x at 1min) | {name}")
            return

        if mult_1min < MIN_1MIN_MULT:
            log.info(f"SKIP (1min mult {mult_1min:.2f}x < {MIN_1MIN_MULT}x) | {name}")
            return

        await buy_coin(session, mint, name, entry_mc, wallet_pubkey, private_key_b58)
    finally:
        if own_session:
            await session.close()


# ── STANDALONE TEST ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info(f"AutoTrader starting in {'PAPER' if PAPER_MODE else 'LIVE'} mode")
    log.info(f"Trade size: {TRADE_AMOUNT_SOL} SOL | Stop loss: {STOP_LOSS_AT*100:.0f}% | Half sell: {HALF_SELL_AT}x | Exit: {MOONBAG_EXIT_HRS}hr")
    asyncio.run(monitor_loop())
