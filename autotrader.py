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
from paper import update_paper_trade

# ── CONFIG ────────────────────────────────────────────────────────────────────
PAPER_MODE       = True          # Set False to trade real SOL
TRADE_AMOUNT_SOL = 0.02          # SOL per trade
TIER1_SELL_AT    = 1.5           # Tier 1: Sell 50% when coin reaches 1.5x (returns 0.015 SOL)
TIER2_SELL_AT    = 3.0           # Tier 2: Sell 25% when coin reaches 3.0x (returns 0.015 SOL)
STOP_LOSS_AT     = 0.75          # Sell everything if coin drops to 75% of entry (-25%)
TRAILING_STOP_PCT= 0.50          # Trailing stop loss for moonbag (50% drop from peak)
MOONBAG_EXIT_HRS = 24            # Sell moonbag after this many hours
MIN_1MIN_MULT    = 1.20          # Min 1-min mult to enter (1.20x gives ~80% win rate)
MAX_OPEN_TRADES  = 6             # Max concurrent open trades for 0.2 SOL bankroll safety
POLL_SECS        = 3             # How often to check open trade prices (seconds)

# Solana RPC (free public endpoint — replace with paid RPC for better reliability)
SOLANA_RPC       = "https://mainnet.helius-rpc.com/?api-key=87eeee41-3eb7-499f-9fad-0c242d9be5bf"

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
    is_news_match: bool = False
    entry_time:   float = field(default_factory=time.time)
    sol_invested: float = TRADE_AMOUNT_SOL
    half_sold:    bool  = False
    tier2_sold:   bool  = False
    peak_mult:    float = 0.0
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

async def check_rug_risk(mint: str) -> tuple[bool, str]:
    """
    Check if top holders (excluding bonding curve/Raydium pool) hold dangerous supply.
    Detects split-wallet dev sniper bundles and cabal clusters.
    """
    try:
        from solders.pubkey import Pubkey
        from solana.rpc.async_api import AsyncClient
        c = AsyncClient(SOLANA_RPC)
        res = await c.get_token_largest_accounts(Pubkey.from_string(mint))
        await c.close()
        
        if not res or not res.value:
            return False, "Could not fetch holders"
            
        # Pump.fun coins have 1 Billion total supply (1,000,000,000).
        # Top account (amounts[0]) is the bonding curve / pool (~800M+).
        amounts = []
        for acc in res.value:
            ui_amt = acc.amount.ui_amount
            if ui_amt:
                amounts.append(ui_amt)
                
        amounts.sort(reverse=True)
        if len(amounts) < 2:
            return False, "Not enough holders to check"
            
        # Non-curve holders (excluding the liquidity pool / curve at index 0)
        holders = amounts[1:]
        
        # 1. Single largest holder check (max 4.5% of total supply)
        top1 = holders[0] / 1_000_000_000 * 100 if len(holders) > 0 else 0
        if top1 > 4.5:
            return True, f"Single sniper holds {top1:.1f}% (>4.5%)"
            
        # 2. Top 2 holders combined (catches 2-wallet split dev snipes)
        top2 = sum(holders[:2]) / 1_000_000_000 * 100 if len(holders) >= 2 else top1
        if top2 > 6.5:
            return True, f"Top 2 snipers hold {top2:.1f}% (>6.5% dev split risk)"
            
        # 3. Top 3 holders combined (catches 3-wallet coordinated bundle)
        top3 = sum(holders[:3]) / 1_000_000_000 * 100 if len(holders) >= 3 else top2
        if top3 > 9.5:
            return True, f"Top 3 snipers hold {top3:.1f}% (>9.5% bundle risk)"
            
        # 4. Top 5 holders combined (catches multi-wallet dev cluster)
        top5 = sum(holders[:5]) / 1_000_000_000 * 100 if len(holders) >= 5 else top3
        if top5 > 12.0:
            return True, f"Top 5 snipers hold {top5:.1f}% (>12.0% cluster risk)"
            
        # 5. Top ~20 wallets combined (catches cabal-distributed supply)
        top_all = sum(holders) / 1_000_000_000 * 100
        if top_all > 20.0:
            return True, f"Top wallets hold {top_all:.1f}% (>20.0% cabal risk)"
            
        return False, f"Clean supply (Top1={top1:.1f}%, Top5={top5:.1f}%, Total={top_all:.1f}%)"
    except Exception as e:
        log.error(f"Rug check failed: {e}")
        return False, "Check failed"

async def buy_coin(
    session: aiohttp.ClientSession,
    mint: str,
    name: str,
    entry_mc: float,
    wallet_pubkey: str = "",
    private_key_b58: str = "",
    is_news_match: bool = False
):
    if mint in open_trades:
        return  # already in this trade

    # Capital rotation for 0.2 SOL wallet safety: cap concurrent trades
    if len(open_trades) >= MAX_OPEN_TRADES:
        moonbags = [t for t in open_trades.values() if t.half_sold]
        if moonbags:
            weakest = min(moonbags, key=lambda t: t.peak_mult)
            log.info(f"CAPITAL ROTATION | Closing weakest moonbag {weakest.name} to free capital for {name}")
            await sell_coin(session, weakest, 0.5, "capital rotation", wallet_pubkey, private_key_b58)
            weakest.closed = True
            weakest.close_reason = "Rotated out for fresh runner"
            if weakest.mint in open_trades:
                closed_trades.append(open_trades.pop(weakest.mint))
        else:
            log.warning(f"MAX TRADES ({MAX_OPEN_TRADES}) REACHED | Skipping {name} to preserve cashflow.")
            return

    is_rug, rug_msg = await check_rug_risk(mint)
    if is_rug:
        log.warning(f"RUG PREVENTED | {name} | {rug_msg} - Aborting buy.")
        return


    invest_amt = TRADE_AMOUNT_SOL * 2 if is_news_match else TRADE_AMOUNT_SOL
    news_tag = " [NEWS/HYPE]" if is_news_match else ""
    log.info(f"BUY | {name} | MC ${entry_mc/1000:.1f}k | {invest_amt} SOL{news_tag} {'[PAPER]' if PAPER_MODE else '[LIVE]'}")

    tx = await execute_swap(
        session, SOL_MINT, mint,
        invest_amt, wallet_pubkey, private_key_b58
    )
    if tx is None and not PAPER_MODE:
        log.error(f"Buy failed for {name}")
        return

    from paper import open_paper_trade
    open_paper_trade(mint, entry_mc, invest_amt, strategy="V3")

    open_trades[mint] = Trade(
        is_news_match=is_news_match,
        sol_invested=invest_amt,
        mint=mint, name=name,
        entry_mc=entry_mc
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
                trade.peak_mult = max(trade.peak_mult, mult)
                elapsed_secs = time.time() - trade.entry_time

                # Sync paper trading database in real time with actual current_mc
                update_paper_trade(mint, current_mc, elapsed_secs=elapsed_secs)

                # ── INITIAL STOP LOSS (-30%) ──────────────────────────────
                if mult <= STOP_LOSS_AT and not trade.half_sold:
                    sol_back = trade.sol_invested * mult
                    trade.sol_returned += sol_back
                    trade.closed = True
                    trade.close_reason = f"Stop loss at {mult:.2f}x"
                    log.info(f"STOP LOSS | {trade.name} | {mult:.2f}x | PnL: {trade.pnl:+.4f} SOL")
                    await sell_coin(session, trade, 1.0, "stop loss", wallet_pubkey, private_key_b58)
                    to_close.append(mint)

                # ── TIER 1: SELL 50% AT 1.5x ──────────────────────────────
                elif not trade.half_sold and mult >= TIER1_SELL_AT:
                    sol_back = (trade.sol_invested * 0.50) * TIER1_SELL_AT  # locks in exactly 0.015 SOL
                    trade.sol_returned += sol_back
                    trade.half_sold = True
                    log.info(f"TIER 1 SELL (50%) | {trade.name} | {mult:.2f}x | Banked {sol_back:.4f} SOL")
                    await sell_coin(session, trade, 0.50, "Tier 1 (50% at 1.5x)", wallet_pubkey, private_key_b58)

                # ── TIER 2: SELL 25% AT 3.0x ──────────────────────────────
                elif trade.half_sold and not trade.tier2_sold and mult >= TIER2_SELL_AT:
                    sol_back = (trade.sol_invested * 0.25) * TIER2_SELL_AT  # locks in another 0.015 SOL
                    trade.sol_returned += sol_back
                    trade.tier2_sold = True
                    log.info(f"TIER 2 SELL (25%) | {trade.name} | {mult:.2f}x | Banked {sol_back:.4f} SOL (Total Returned: {trade.sol_returned:.4f} SOL)")
                    await sell_coin(session, trade, 0.25, "Tier 2 (25% at 3.0x)", wallet_pubkey, private_key_b58)

                # ── TRAILING STOP LOSS (MOONBAG) ──────────────────────────
                elif trade.half_sold and mult <= max(1.0, trade.peak_mult * (1.0 - TRAILING_STOP_PCT)):
                    # Remaining fraction of original position:
                    rem_fraction = 0.25 if trade.tier2_sold else 0.50
                    # Real market return: sell at current market price (mult)
                    sol_back = (trade.sol_invested * rem_fraction) * mult
                    trade.sol_returned += sol_back
                    trade.closed = True
                    trade.close_reason = f"Trailing stop at {mult:.2f}x (Peak: {trade.peak_mult:.2f}x)"
                    log.info(f"TRAILING STOP ({int(rem_fraction*100)}%) | {trade.name} | {mult:.2f}x | Final PnL: {trade.pnl:+.4f} SOL")
                    await sell_coin(session, trade, rem_fraction, "trailing stop exit", wallet_pubkey, private_key_b58)
                    to_close.append(mint)

                # ── 24HR MOONBAG EXIT ─────────────────────────────────────
                elif trade.elapsed_hrs >= MOONBAG_EXIT_HRS:
                    rem_fraction = 0.25 if trade.tier2_sold else (0.50 if trade.half_sold else 1.0)
                    sol_back = (trade.sol_invested * rem_fraction) * mult
                    trade.sol_returned += sol_back
                    trade.closed = True
                    trade.close_reason = f"24hr exit at {mult:.2f}x"
                    log.info(f"24HR EXIT | {trade.name} | {mult:.2f}x | Final PnL: {trade.pnl:+.4f} SOL")
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

        actual_entry = mc_1min if mc_1min else entry_mc
        await buy_coin(session, mint, name, actual_entry, wallet_pubkey, private_key_b58)
    finally:
        if own_session:
            await session.close()


# ── WHALE COPY TRADE ─────────────────────────────────────────────────────────
async def on_whale_buy(
    session: Optional[aiohttp.ClientSession],
    mint: str,
    wallet_pubkey: str = "",
    private_key_b58: str = ""
):
    """
    Called automatically by whale_tracker.py when a whale buys a coin.
    Bypasses the 1-min momentum check entirely.
    """
    own_session = session is None
    if own_session:
        connector = aiohttp.TCPConnector(ssl=False)
        session = aiohttp.ClientSession(connector=connector)

    try:
        current_mc = await fetch_current_mc(session, mint)
        # We need an entry MC for the stop loss math
        entry_mc = current_mc if current_mc else 20000.0 # fallback

        log.info(f"🚀 INSTANT WHALE COPY | {mint} | MC ${entry_mc/1000:.1f}k")
        
        # We tag is_news_match=True just so it doubles the invest amount (high conviction)
        await buy_coin(session, mint, "Whale Coin", entry_mc, wallet_pubkey, private_key_b58, is_news_match=True)
    finally:
        if own_session:
            await session.close()


# ── STANDALONE TEST ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info(f"AutoTrader starting in {'PAPER' if PAPER_MODE else 'LIVE'} mode")
    log.info(f"Trade size: {TRADE_AMOUNT_SOL} SOL | Stop loss: {STOP_LOSS_AT*100:.0f}% | Half sell: {HALF_SELL_AT}x | Exit: {MOONBAG_EXIT_HRS}hr")
    asyncio.run(monitor_loop())
