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
TIER1_SELL_AT    = 1.35          # Tier 1: Sell 74% at 1.35x (locks 0.0200 SOL = 100.0% principal back at +35%!)
TIER2_SELL_AT    = 2.0           # Tier 2: Sell 10% at 2.0x (locks 0.0040 SOL -> 0.0240 SOL = +20% guaranteed + 16% Moonbag!)
MIN_DISCOVERY_MC = 18_500        # Min discovery MC at min 0 ($18.5k-$26k Golden Zone)
MAX_ENTRY_MC     = 26_000        # Max discovery MC at min 0
MIN_1MIN_MC      = 24_500        # Min 45s-50s staircase buy MC ($24.5k floor)
MAX_1MIN_MC      = 39_500        # Max 45s-50s staircase buy MC ($39.5k cap; skips $30.8k-$33.8k KOTH dump wall)
KOTH_TRAP_LOW    = 30_800        # Lower bound of Pump.fun King-of-the-Hill dump wall ($30.8k-$33.8k)
KOTH_TRAP_HIGH   = 33_800        # Upper bound of KOTH dump wall (coins must either be pre-KOTH <=$30.8k or broken above >=$33.8k)
STOP_LOSS_AT     = 0.78          # Tighter initial stop loss at 78% of entry (-22%)
TRAILING_STOP_PCT= 0.35          # Trailing stop loss for moonbag (35% drop from peak)
MOONBAG_EXIT_HRS = 24            # Sell moonbag after this many hours
MIN_1MIN_MULT    = 1.15          # Min 45s staircase mult from discovery (+15%)
MAX_1MIN_MULT    = 1.95          # Max 45s staircase mult from discovery (+95% cap for KOTH breakouts)
MAX_OPEN_TRADES  = 4             # Max concurrent active (un-banked) trades for 0.2 SOL bankroll safety
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


async def check_buy_pressure(session: aiohttp.ClientSession, mint: str) -> tuple[bool, str]:
    """Verify m5 buys outnumber sells on DexScreener so we don't buy into an active bundle dump."""
    try:
        async with session.get(
            DEXSCREENER_URL.format(mint),
            timeout=aiohttp.ClientTimeout(total=8)
        ) as resp:
            if resp.status != 200:
                return True, "OK"
            data = await resp.json(content_type=None)
            pairs = data.get("pairs") or []
            if not pairs:
                return True, "OK"
            best = max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
            m5 = (best.get("txns") or {}).get("m5") or {}
            buys = int(m5.get("buys") or 0)
            sells = int(m5.get("sells") or 0)
            if buys > 0 and sells >= buys:
                return False, f"Sell pressure dominates (m5 buys={buys} <= sells={sells})"
            return True, f"Orderflow OK (m5 buys={buys}, sells={sells})"
    except Exception:
        return True, "OK"


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
        if top2 > 7.5:
            return True, f"Top 2 snipers hold {top2:.1f}% (>7.5% dev split risk)"
            
        # 3. Top 3 holders combined (catches 3-wallet coordinated bundle)
        top3 = sum(holders[:3]) / 1_000_000_000 * 100 if len(holders) >= 3 else top2
        if top3 > 10.5:
            return True, f"Top 3 snipers hold {top3:.1f}% (>10.5% bundle risk)"
            
        # 4. Top 5 holders combined (catches multi-wallet dev cluster)
        top5 = sum(holders[:5]) / 1_000_000_000 * 100 if len(holders) >= 5 else top3
        if top5 > 16.0:
            return True, f"Top 5 snipers hold {top5:.1f}% (>16.0% cluster risk)"
            
        # 5. Top ~19 wallets combined vs 1B total supply
        top_all = sum(holders) / 1_000_000_000 * 100
        if top_all > 50.0:
            return True, f"Top wallets hold {top_all:.1f}% (>50.0% cabal risk)"

        # 6. Circulating-supply Jito bundle check (catches 12-19 wallet split bundles when 600M-800M is still in curve)
        circulating = max(1.0, 1_000_000_000 - amounts[0])
        if circulating >= 25_000_000:
            top5_circ = sum(holders[:5]) / circulating * 100
            top19_circ = sum(holders) / circulating * 100
            if top5_circ > 38.0:
                return True, f"Top 5 hold {top5_circ:.1f}% of circulating float (>38% Jito bundle risk)"
            if top19_circ > 75.0:
                return True, f"Top 19 hold {top19_circ:.1f}% of circulating float (>75% Jito bundle risk)"

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

    # Cap concurrent active (un-banked) trades for 0.2 SOL bankroll safety (do not liquidate risk-free moonbags!)
    active_at_risk = [t for t in open_trades.values() if not t.half_sold and not t.closed]
    if len(active_at_risk) >= MAX_OPEN_TRADES:
        log.warning(f"MAX ACTIVE TRADES ({MAX_OPEN_TRADES}) REACHED | Skipping {name} to preserve cashflow.")
        return

    is_rug, rug_msg = await check_rug_risk(mint)
    if is_rug:
        log.warning(f"RUG PREVENTED | {name} | {rug_msg} - Aborting buy.")
        return

    bp_ok, bp_msg = await check_buy_pressure(session, mint)
    if not bp_ok:
        log.warning(f"SELL PRESSURE BLOCKED | {name} | {bp_msg} - Aborting buy.")
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
    open_paper_trade(mint, entry_mc, invest_amt, strategy="V4")

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

                # ── INITIAL STOP LOSS (-22% at 0.78x) OR 40s FAILED-BREAKOUT TIME-STOP ──
                is_failed_breakout = (not trade.half_sold and elapsed_secs >= 40 and mult <= 0.95 and trade.peak_mult < 1.18)
                if (mult <= STOP_LOSS_AT or is_failed_breakout) and not trade.half_sold:
                    sol_back = trade.sol_invested * mult
                    trade.sol_returned += sol_back
                    trade.closed = True
                    reason_str = "failed breakout time-stop" if is_failed_breakout else "stop loss"
                    trade.close_reason = f"{reason_str} at {mult:.2f}x"
                    log.info(f"STOP ({reason_str.upper()}) | {trade.name} | {mult:.2f}x | PnL: {trade.pnl:+.4f} SOL")
                    await sell_coin(session, trade, 1.0, reason_str, wallet_pubkey, private_key_b58)
                    to_close.append(mint)

                # ── TIER 1: SELL 74% AT 1.35x KOTH WALL (LOCKS 0.0200 SOL = 100.0% PRINCIPAL BACK!) ──
                elif not trade.half_sold and mult >= TIER1_SELL_AT:
                    sol_back = (trade.sol_invested * 0.74) * TIER1_SELL_AT
                    trade.sol_returned += sol_back
                    trade.half_sold = True
                    log.info(f"TIER 1 SELL (74%) | {trade.name} | {mult:.2f}x | Banked {sol_back:.4f} SOL (100% principal back!)")
                    await sell_coin(session, trade, 0.74, "Tier 1 (74% at 1.35x KOTH)", wallet_pubkey, private_key_b58)

                # ── TIER 2: SELL 10% AT 2.0x (LOCKS 0.0240 SOL = +20% GUARANTEED + 16% MOONBAG!) ──
                elif trade.half_sold and not trade.tier2_sold and mult >= TIER2_SELL_AT:
                    sol_back = (trade.sol_invested * 0.10) * TIER2_SELL_AT
                    trade.sol_returned += sol_back
                    trade.tier2_sold = True
                    log.info(f"TIER 2 SELL (10%) | {trade.name} | {mult:.2f}x | Banked {sol_back:.4f} SOL (Total Returned: {trade.sol_returned:.4f} SOL)")
                    await sell_coin(session, trade, 0.10, "Tier 2 (10% at 2.0x)", wallet_pubkey, private_key_b58)

                # ── TRAILING STOP LOSS (16% MOONBAG) ──────────────────────
                elif trade.half_sold and mult <= max(0.95, trade.peak_mult * (1.0 - TRAILING_STOP_PCT)):
                    rem_fraction = 0.16 if trade.tier2_sold else 0.26
                    sol_back = (trade.sol_invested * rem_fraction) * mult
                    trade.sol_returned += sol_back
                    trade.closed = True
                    trade.close_reason = f"Trailing stop at {mult:.2f}x (Peak: {trade.peak_mult:.2f}x)"
                    log.info(f"TRAILING STOP ({rem_fraction*100:.1f}%) | {trade.name} | {mult:.2f}x | Final PnL: {trade.pnl:+.4f} SOL")
                    await sell_coin(session, trade, rem_fraction, "trailing stop exit", wallet_pubkey, private_key_b58)
                    to_close.append(mint)

                # ── 24HR MOONBAG EXIT ─────────────────────────────────────
                elif trade.elapsed_hrs >= MOONBAG_EXIT_HRS:
                    rem_fraction = 0.16 if trade.tier2_sold else (0.26 if trade.half_sold else 1.0)
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


# ── CALLED BY MAIN BOT WHEN A NEW HIGH-SCORE COIN IS FOUND ───────────────────
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
    Fast two-stage breakout entry:
      1. Wait 15s to filter instant launch rugs.
      2. Poll every 5s (from 15s to 45s) for 2 consecutive rising checks >= 1.12x
         inside the $23k-$28.5k buy window so we enter only confirmed organic breakouts.
    """
    own_session = session is None
    if own_session:
        connector = aiohttp.TCPConnector(ssl=False)
        session = aiohttp.ClientSession(connector=connector)

    try:
        if entry_mc < MIN_DISCOVERY_MC or entry_mc > MAX_ENTRY_MC:
            log.info(f"SKIP (Discovery MC ${entry_mc/1000:.1f}k outside ${MIN_DISCOVERY_MC/1000:.0f}k-${MAX_ENTRY_MC/1000:.0f}k zone) | {name}")
            return

        if mc_1min is None:
            log.info(f"WATCHING 3-STAGE STAIRCASE (0s->20s->45s) | {name} | Discovery MC ${entry_mc/1000:.1f}k")
            # Stage 1: Wait 20s and require +10% gain from discovery (filters instant launch dumps)
            await asyncio.sleep(20)
            mc_20s = await fetch_current_mc(session, mint)
            if mc_20s is None or mc_20s < entry_mc * 1.10:
                log.info(f"SKIP (20s MC ${mc_20s/1000 if mc_20s else 0:.1f}k < 1.10x discovery) | {name}")
                return

            # Stage 2: Wait another 25s (to 45s) so DexScreener cache refreshes & 20s dev-bundle spikes roll over!
            # Across 48 V4 trades, coins where 45s-60s MC <= 20s MC went 5/30 (-0.25 SOL), while >20s MC went 10/12 (+0.11 SOL)!
            await asyncio.sleep(25)
            mc_45s = await fetch_current_mc(session, mint)
            if mc_45s is None or mc_45s <= mc_20s * 1.02:
                log.info(f"SKIP (stalled/rolled over between 20s ${mc_20s/1000:.1f}k and 45s ${mc_45s/1000 if mc_45s else 0:.1f}k) | {name}")
                return

            # Stage 3: KOTH Dump-Wall Filter ($30.8k - $33.8k)
            # If sitting inside the exact $30.8k-$33.8k King-of-the-Hill dump zone at 45s, wait 10s to require a clean breakout > $33.8k
            if KOTH_TRAP_LOW < mc_45s < KOTH_TRAP_HIGH:
                log.info(f"KOTH WALL CHECK ($30.8k-$33.8k) | {name} at ${mc_45s/1000:.1f}k — waiting 10s for breakout > ${KOTH_TRAP_HIGH/1000:.1f}k")
                await asyncio.sleep(10)
                mc_55s = await fetch_current_mc(session, mint)
                if mc_55s is None or mc_55s < KOTH_TRAP_HIGH or mc_55s <= mc_45s:
                    log.info(f"SKIP (rejected at KOTH wall: ${mc_55s/1000 if mc_55s else 0:.1f}k) | {name}")
                    return
                mc_45s = mc_55s

            mc_1min = mc_45s

        if mc_1min is None:
            log.info(f"SKIP (did not pass 3-stage staircase) | {name}")
            return

        mult_1min = mc_1min / entry_mc if entry_mc > 0 else 0
        if mult_1min > MAX_1MIN_MULT:
            log.info(f"SKIP (already pumped {mult_1min:.2f}x > {MAX_1MIN_MULT}x cap — avoiding top) | {name}")
            return

        actual_entry = mc_1min
        if actual_entry < MIN_1MIN_MC:
            log.info(f"SKIP (Buy MC ${actual_entry/1000:.1f}k < ${MIN_1MIN_MC/1000:.0f}k floor) | {name}")
            return
        if actual_entry > MAX_1MIN_MC:
            log.info(f"SKIP (Buy MC ${actual_entry/1000:.1f}k > ${MAX_1MIN_MC/1000:.1f}k cap) | {name}")
            return

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
