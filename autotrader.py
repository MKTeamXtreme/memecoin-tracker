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
from pumpportal import fetch_pump_curve_mc_usd
from creator_guard import check_creator_risk, record_rugged_coin

# ── CONFIG ────────────────────────────────────────────────────────────────────
PAPER_MODE       = True          # Set False to trade real SOL
TRADE_AMOUNT_SOL = 0.02          # SOL per trade
TIER1_SELL_AT    = 1.35          # Tier 1: Sell 62% at 1.35x (banks 0.837x principal & raises floor to 0.95x)
TIER2_SELL_AT    = 1.55          # Tier 2: Sell 23% at 1.55x ($40k-$47k pre-graduation wall -> locks +19.4% cash + 15% Moonbag!)
MIN_COIN_AGE_SECS= 110           # Min token age at discovery (110s filters slot-0 dumps while keeping 159s-167s organic entries)
MIN_DISCOVERY_MC = 18_500        # Min discovery MC at min 0 ($18.5k-$25.5k Golden Zone)
MAX_ENTRY_MC     = 25_500        # Max discovery MC at min 0
MIN_1MIN_MC      = 24_500        # Min 45s staircase buy MC ($24.5k floor)
MAX_1MIN_MC      = 30_800        # Max 45s staircase buy MC ($30.8k cap: enters strictly pre-KOTH so 1.55x is below $50k wall)
KOTH_TRAP_LOW    = 30_800        # Lower bound of Pump.fun King-of-the-Hill dump wall ($30.8k-$33.8k)
KOTH_TRAP_HIGH   = 33_800        # Upper bound of KOTH dump wall
STOP_LOSS_AT     = 0.58          # Structural stop loss at 0.58x (avoids 0.60x-0.75x first-pullback shakeouts on 10x-48x runners)
TRAILING_STOP_PCT= 0.28          # Trailing stop loss for 15% moonbag (28% drop from peak = 0.72x peak)
MOONBAG_EXIT_HRS = 24            # Sell moonbag after this many hours
MIN_1MIN_MULT    = 1.16          # Min 45s staircase mult from discovery (+16%)
MAX_1MIN_MULT    = 1.54          # Max 45s staircase mult from discovery (+54% cap: filters vertical blow-off tops)
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
    # 1. Zero-lag direct on-chain Pump.fun bonding-curve read (falls through to DexScreener once graduated)
    onchain_mc = await fetch_pump_curve_mc_usd(session, mint, SOLANA_RPC)
    if onchain_mc is not None and onchain_mc > 0:
        return onchain_mc

    # 2. DexScreener fallback (for graduated PumpSwap/Raydium AMM pools or RPC hiccups)
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

async def check_rug_risk(mint: str, expected_mc: float = 0.0) -> tuple[bool, str]:
    """
    Check if top holders (excluding bonding curve/Raydium pool) hold dangerous supply.
    Detects split-wallet dev sniper bundles, 500-wallet micro-shard Sybil bundlers,
    and stale DexScreener caches on already-rugged Pump.fun bonding curves.
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
        # Top account (amounts[0]) is the bonding curve / pool (~800M+ when rugged, ~210M-650M at $18.5k-$60k).
        amounts = []
        for acc in res.value:
            ui_amt = acc.amount.ui_amount
            if ui_amt:
                amounts.append(ui_amt)
                
        amounts.sort(reverse=True)
        if len(amounts) < 2:
            return False, "Not enough holders to check"

        is_pump = mint.endswith("pump")
        if is_pump and amounts[0] > 1_000_000_000:
            return True, f"Counterfeit pump supply ({amounts[0]/1e6:.0f}M > 1B)"

        # Stale DexScreener cache blocker: a live Pump.fun coin >= $18.5k MC mathematically has <650M in curve.
        # If >780M tokens are back in amounts[0], the coin has ALREADY rugged on-chain while DexScreener lags!
        if is_pump and expected_mc >= 18_500 and amounts[0] > 780_000_000:
            return True, f"Already rugged on-chain / stale DexScreener cache (curve holds {amounts[0]/1e6:.1f}M)"
            
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

        # 6. Circulating-supply Jito & Micro-Shard Sybil bundle checks
        circulating = max(1.0, 1_000_000_000 - amounts[0])
        if circulating >= 25_000_000:
            top5_circ = sum(holders[:5]) / circulating * 100
            top19_circ = sum(holders) / circulating * 100
            if top5_circ > 38.0:
                return True, f"Top 5 hold {top5_circ:.1f}% of circulating float (>38% Jito bundle risk)"
            if top19_circ > 75.0:
                return True, f"Top 19 hold {top19_circ:.1f}% of circulating float (>75% Jito bundle risk)"
            # 7. 500-Wallet Micro-Shard Sybil Bundler check (while on Pump.fun bonding curve: 200M <= curve <= 780M):
            # Real organic bonding-curve coins have top19_circ in 33%-75%; 500-wallet v1 bundlers shard float to 1%-5%!
            if is_pump and 200_000_000 <= amounts[0] <= 780_000_000 and circulating >= 200_000_000 and top19_circ < 15.0:
                return True, f"Micro-shard Sybil bundler: Top 19 hold only {top19_circ:.1f}% of {circulating/1e6:.0f}M float (<15%)"

        return False, f"Clean supply (Top1={top1:.1f}%, Top5={top5:.1f}%, Total={top_all:.1f}%)"
    except Exception as e:
        log.error(f"Rug check failed: {e}")
        return False, "Check failed"

def _prune_closed_open_trades():
    """Remove any in-memory open_trades that are already CLOSED in SQLite so MAX_OPEN_TRADES can never deadlock."""
    if not open_trades:
        return
    try:
        from database import get_connection
        conn = get_connection()
        rows = conn.execute("SELECT mint FROM paper_trades WHERE status = 'OPEN' AND strategy IN ('V4', 'V5')").fetchall()
        conn.close()
        db_open = {r[0] for r in rows}
        for m in list(open_trades.keys()):
            if m not in db_open:
                closed_trades.append(open_trades.pop(m))
    except Exception as e:
        log.error(f"Could not sync open_trades with DB: {e}")


async def buy_coin(
    session: aiohttp.ClientSession,
    mint: str,
    name: str,
    entry_mc: float,
    wallet_pubkey: str = "",
    private_key_b58: str = "",
    is_news_match: bool = False,
    buy_reason: str = "",
    buy_source: str = "STAIRCASE",
):
    _prune_closed_open_trades()
    if mint in open_trades:
        return  # already in this trade

    # Cap concurrent active (un-banked) trades for 0.2 SOL bankroll safety (do not liquidate risk-free moonbags!)
    active_at_risk = [t for t in open_trades.values() if not t.half_sold and not t.closed]
    if len(active_at_risk) >= MAX_OPEN_TRADES:
        log.warning(f"MAX ACTIVE TRADES ({MAX_OPEN_TRADES}) REACHED | Skipping {name} to preserve cashflow.")
        return

    is_rug, rug_msg = await check_rug_risk(mint, expected_mc=entry_mc)
    if is_rug:
        log.warning(f"RUG PREVENTED | {name} | {rug_msg} - Aborting buy.")
        return

    is_bad_creator, creator_msg = await check_creator_risk(session, mint, SOLANA_RPC)
    if is_bad_creator:
        log.warning(f"SERIAL RUGGER BLOCKED | {name} | {creator_msg} - Aborting buy.")
        return

    bp_ok, bp_msg = await check_buy_pressure(session, mint)
    if not bp_ok:
        log.warning(f"SELL PRESSURE BLOCKED | {name} | {bp_msg} - Aborting buy.")
        return

    details = [p for p in [buy_reason, rug_msg, bp_msg if bp_msg != "OK" else ""] if p]
    full_reason = " | ".join(details)

    invest_amt = TRADE_AMOUNT_SOL
    news_tag = f" [{buy_source}]" if buy_source else ""
    log.info(f"BUY | {name} | MC ${entry_mc/1000:.1f}k | {invest_amt} SOL{news_tag} | Why: {full_reason} {'[PAPER]' if PAPER_MODE else '[LIVE]'}")

    tx = await execute_swap(
        session, SOL_MINT, mint,
        invest_amt, wallet_pubkey, private_key_b58
    )
    if tx is None and not PAPER_MODE:
        log.error(f"Buy failed for {name}")
        return

    from paper import open_paper_trade
    open_paper_trade(mint, entry_mc, invest_amt, strategy="V5", buy_reason=full_reason, buy_source=buy_source)

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


def restore_open_v4_trades():
    """Reload any OPEN V4 trades from SQLite on startup so server restarts never orphan live trades."""
    try:
        from database import get_connection
        conn = get_connection()
        rows = conn.execute("""
            SELECT pt.mint, COALESCE(c.name, pt.mint) as name, pt.entry_mc, pt.invested_sol,
                   pt.sold_half, pt.sold_half_sol, COALESCE(pt.sold_tier2, 0) as sold_tier2,
                   COALESCE(pt.sold_tier2_sol, 0) as sold_tier2_sol, pt.peak_mc, pt.created_at
            FROM paper_trades pt
            LEFT JOIN coins c ON c.mint = pt.mint
            WHERE pt.status = 'OPEN' AND pt.strategy IN ('V4', 'V5')
        """).fetchall()
        conn.close()
        for r in rows:
            mint = r[0]
            if mint not in open_trades:
                entry_mc = float(r[2] or 1.0)
                peak_mc = float(r[8] or entry_mc)
                open_trades[mint] = Trade(
                    mint=mint,
                    name=str(r[1]),
                    entry_mc=entry_mc,
                    entry_time=float(r[9] or time.time()),
                    sol_invested=float(r[3] or TRADE_AMOUNT_SOL),
                    half_sold=bool(r[4]),
                    tier2_sold=bool(r[6]),
                    peak_mult=peak_mc / entry_mc if entry_mc > 0 else 1.0,
                    sol_returned=float(r[5] or 0.0) + float(r[7] or 0.0),
                )
        if rows:
            log.info(f"Restored {len(rows)} OPEN V4 trade(s) from database into 3s monitor loop.")
    except Exception as e:
        log.error(f"Could not restore open V4 trades: {e}")


# ── TRADE MONITOR LOOP ────────────────────────────────────────────────────────
async def monitor_loop(wallet_pubkey: str = "", private_key_b58: str = ""):
    """Fast loop that checks all open trade prices every POLL_SECS seconds."""
    restore_open_v4_trades()
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
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

                    # Sync paper trading database in real time with actual current_mc (monitor is source of truth for V4/V5)
                    update_paper_trade(mint, current_mc, elapsed_secs=elapsed_secs, from_monitor=True)

                    # ── INITIAL STOP LOSS (0.58x) OR 180s STAGNATION TIME-STOP ──
                    is_failed_breakout = (not trade.half_sold and elapsed_secs >= 180 and mult <= 0.95 and trade.peak_mult < 1.18)
                    if (mult <= STOP_LOSS_AT or is_failed_breakout) and not trade.half_sold:
                        sol_back = trade.sol_invested * mult
                        trade.sol_returned += sol_back
                        trade.closed = True
                        reason_str = "180s stagnation time-stop" if is_failed_breakout else "stop loss"
                        trade.close_reason = f"{reason_str} at {mult:.2f}x"
                        log.info(f"STOP ({reason_str.upper()}) | {trade.name} | {mult:.2f}x | PnL: {trade.pnl:+.4f} SOL")
                        await sell_coin(session, trade, 1.0, reason_str, wallet_pubkey, private_key_b58)
                        to_close.append(mint)
                        # Auto-learn: if instant rug (<= $4.5k or <= 0.25x), blacklist creator + log parent funder
                        if current_mc <= 4_500 or mult <= 0.25:
                            asyncio.create_task(record_rugged_coin(mint, SOLANA_RPC))

                    # ── TIER 1: SELL 62% AT 1.35x (BANKS 83.7% PRINCIPAL & RAISES STOP TO 0.95x) ──
                    elif not trade.half_sold and mult >= TIER1_SELL_AT:
                        sol_back = (trade.sol_invested * 0.62) * TIER1_SELL_AT
                        trade.sol_returned += sol_back
                        trade.half_sold = True
                        log.info(f"TIER 1 SELL (62%) | {trade.name} | {mult:.2f}x | Banked {sol_back:.4f} SOL")
                        await sell_coin(session, trade, 0.62, "Tier 1 (62% at 1.35x)", wallet_pubkey, private_key_b58)

                    # ── TIER 2: SELL 23% AT 1.55x PRE-GRADUATION WALL (LOCKS +19.4% CASH + 15% MOONBAG!) ──
                    elif trade.half_sold and not trade.tier2_sold and mult >= TIER2_SELL_AT:
                        sol_back = (trade.sol_invested * 0.23) * TIER2_SELL_AT
                        trade.sol_returned += sol_back
                        trade.tier2_sold = True
                        log.info(f"TIER 2 SELL (23%) | {trade.name} | {mult:.2f}x | Banked {sol_back:.4f} SOL (Total Returned: {trade.sol_returned:.4f} SOL)")
                        await sell_coin(session, trade, 0.23, "Tier 2 (23% at 1.55x pre-grad wall)", wallet_pubkey, private_key_b58)

                    # ── TRAILING STOP LOSS (15% MOONBAG / 38% POST-T1) ───────────────────────
                    elif trade.half_sold and mult <= max(0.95, trade.peak_mult * (1.0 - TRAILING_STOP_PCT)):
                        rem_fraction = 0.15 if trade.tier2_sold else 0.38
                        sol_back = (trade.sol_invested * rem_fraction) * mult
                        trade.sol_returned += sol_back
                        trade.closed = True
                        trade.close_reason = f"Trailing stop at {mult:.2f}x (Peak: {trade.peak_mult:.2f}x)"
                        log.info(f"TRAILING STOP ({rem_fraction*100:.1f}%) | {trade.name} | {mult:.2f}x | Final PnL: {trade.pnl:+.4f} SOL")
                        await sell_coin(session, trade, rem_fraction, "trailing stop exit", wallet_pubkey, private_key_b58)
                        to_close.append(mint)

                    # ── 24HR MOONBAG EXIT ─────────────────────────────────────
                    elif trade.elapsed_hrs >= MOONBAG_EXIT_HRS:
                        rem_fraction = 0.15 if trade.tier2_sold else (0.38 if trade.half_sold else 1.0)
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
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error(f"monitor_loop recovered from error: {e}")

            await asyncio.sleep(POLL_SECS)


# ── CALLED BY MAIN BOT WHEN A NEW HIGH-SCORE COIN IS FOUND ───────────────────
async def on_new_coin(
    session: Optional[aiohttp.ClientSession],
    mint: str,
    name: str,
    entry_mc: float,
    mc_1min: Optional[float] = None,
    wallet_pubkey: str = "",
    private_key_b58: str = "",
    pair_created_at: Optional[float] = None,
    is_breaking_hype: bool = False,
    hype_reason: str = "",
):
    """
    Two-mode breakout confirmation:
      - Normal V5 coins: >=110s age, $18.5k-$25.5k discovery, 0s->20s->45s staircase, $24.5k-$30.8k buy window.
      - Breaking News / Live Meta-Cluster Leaders (is_breaking_hype=True):
        >=35s age, $15.0k-$25.5k discovery, fast 0s->12s->25s staircase, $18.0k-$32.0k buy window.
    """
    own_session = session is None
    if own_session:
        connector = aiohttp.TCPConnector(ssl=False)
        session = aiohttp.ClientSession(connector=connector)

    min_age    = 35 if is_breaking_hype else MIN_COIN_AGE_SECS
    min_disc   = 15_000 if is_breaking_hype else MIN_DISCOVERY_MC
    max_disc   = MAX_ENTRY_MC
    s1_secs    = 12 if is_breaking_hype else 20
    s2_secs    = 13 if is_breaking_hype else 25
    s1_mult    = 1.08 if is_breaking_hype else 1.10
    min_mult   = 1.12 if is_breaking_hype else MIN_1MIN_MULT
    max_mult   = 1.75 if is_breaking_hype else MAX_1MIN_MULT
    min_buy_mc = 18_000 if is_breaking_hype else MIN_1MIN_MC
    max_buy_mc = 32_000 if is_breaking_hype else MAX_1MIN_MC

    try:
        if pair_created_at is not None:
            coin_age = time.time() - pair_created_at
            if 0 <= coin_age < min_age:
                log.info(f"SKIP (Coin age {coin_age:.0f}s < {min_age}s launch-dump filter) | {name}")
                return

        if entry_mc < min_disc or entry_mc > max_disc:
            log.info(f"SKIP (Discovery MC ${entry_mc/1000:.1f}k outside ${min_disc/1000:.0f}k-${max_disc/1000:.0f}k zone) | {name}")
            return

        if mc_1min is None:
            mode_lbl = f"FAST-TRACK HYPE (0s->{s1_secs}s->{s1_secs+s2_secs}s | {hype_reason})" if is_breaking_hype else "3-STAGE STAIRCASE (0s->20s->45s)"
            log.info(f"WATCHING {mode_lbl} | {name} | Discovery MC ${entry_mc/1000:.1f}k")
            # Stage 1: Wait s1_secs and require gain from discovery (filters instant launch dumps)
            await asyncio.sleep(s1_secs)
            mc_s1 = await fetch_current_mc(session, mint)
            if mc_s1 is None or mc_s1 < entry_mc * s1_mult:
                log.info(f"SKIP ({s1_secs}s MC ${mc_s1/1000 if mc_s1 else 0:.1f}k < {s1_mult:.2f}x discovery) | {name}")
                return

            # Stage 2: Wait s2_secs and require continued upward staircase
            await asyncio.sleep(s2_secs)
            mc_s2 = await fetch_current_mc(session, mint)
            if mc_s2 is None or mc_s2 <= mc_s1 * 1.02:
                log.info(f"SKIP (stalled/rolled over between {s1_secs}s ${mc_s1/1000:.1f}k and {s1_secs+s2_secs}s ${mc_s2/1000 if mc_s2 else 0:.1f}k) | {name}")
                return

            mc_1min = mc_s2

        if mc_1min is None:
            log.info(f"SKIP (did not pass confirmation staircase) | {name}")
            return

        mult_1min = mc_1min / entry_mc if entry_mc > 0 else 0
        if mult_1min < min_mult:
            log.info(f"SKIP (staircase mult {mult_1min:.2f}x < {min_mult:.2f}x floor) | {name}")
            return
        if mult_1min > max_mult:
            log.info(f"SKIP (already pumped {mult_1min:.2f}x > {max_mult:.2f}x cap — avoiding blow-off top) | {name}")
            return

        actual_entry = mc_1min
        if actual_entry < min_buy_mc:
            log.info(f"SKIP (Buy MC ${actual_entry/1000:.1f}k < ${min_buy_mc/1000:.0f}k floor) | {name}")
            return
        if actual_entry > max_buy_mc:
            log.info(f"SKIP (Buy MC ${actual_entry/1000:.1f}k > ${max_buy_mc/1000:.1f}k cap) | {name}")
            return

        pct_up = int((mult_1min - 1.0) * 100)
        if is_breaking_hype and hype_reason:
            b_src = "BREAKING_NEWS" if hype_reason.startswith("BREAKING") else "VIRAL_META"
            b_rsn = f"{hype_reason} (+{pct_up}% in {s1_secs+s2_secs}s: ${entry_mc/1000:.1f}k->${actual_entry/1000:.1f}k)"
        else:
            b_src = "STAIRCASE"
            b_rsn = f"V5 Staircase (+{pct_up}% in 45s: ${entry_mc/1000:.1f}k->${actual_entry/1000:.1f}k)"

        await buy_coin(
            session, mint, name, actual_entry, wallet_pubkey, private_key_b58,
            is_news_match=is_breaking_hype, buy_reason=b_rsn, buy_source=b_src,
        )
    finally:
        if own_session:
            await session.close()


# ── SMART-MONEY / WHALE CONFLUENCE ENTRY ─────────────────────────────────────
async def on_whale_buy(
    session: Optional[aiohttp.ClientSession],
    mint: str,
    wallet_pubkey: str = "",
    private_key_b58: str = "",
    buyer_count: int = 1,
    wallet_labels: str = "",
):
    """
    Called by whale_tracker.py when smart-money confluence triggers (e.g., 2+ vetted wallets
    buying the same token within 5m, or 1 vetted wallet on a clean sub-$45k coin).
    Still enforces real live MC bounds, Helius check_rug_risk, check_creator_risk, and check_buy_pressure.
    """
    own_session = session is None
    if own_session:
        connector = aiohttp.TCPConnector(ssl=False)
        session = aiohttp.ClientSession(connector=connector)

    try:
        current_mc = await fetch_current_mc(session, mint)
        if not current_mc or current_mc <= 0:
            log.info(f"SKIP SMART-MONEY (could not read live MC) | {mint[:10]}..")
            return

        max_mc = 85_000 if buyer_count >= 2 else 45_000
        if current_mc < 10_000 or current_mc > max_mc:
            log.info(f"SKIP SMART-MONEY (MC ${current_mc/1000:.1f}k outside $10k-${max_mc/1000:.0f}k window) | {mint[:10]}..")
            return

        # Resolve token name from DexScreener if available so dashboard shows real name
        coin_name = f"SmartMoney-{mint[:6]}"
        try:
            async with session.get(DEXSCREENER_URL.format(mint), timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    pdata = await resp.json(content_type=None)
                    pairs = pdata.get("pairs") or []
                    if pairs:
                        bt = pairs[0].get("baseToken") or {}
                        coin_name = bt.get("name") or bt.get("symbol") or coin_name
        except Exception:
            pass

        tag = f"{buyer_count}-WALLET CONFLUENCE" if buyer_count >= 2 else "SMART-MONEY BUY"
        sm_reason = (
            f"Smart-Money {buyer_count}-wallet confluence ({wallet_labels}) @ ${current_mc/1000:.1f}k"
            if buyer_count >= 2
            else f"Smart-Money buy by {wallet_labels} @ ${current_mc/1000:.1f}k"
        )
        log.info(f"{tag} ({wallet_labels}) | {coin_name} ({mint[:8]}..) | MC ${current_mc/1000:.1f}k")
        await buy_coin(
            session, mint, coin_name, current_mc, wallet_pubkey, private_key_b58,
            is_news_match=True, buy_reason=sm_reason, buy_source="SMART_MONEY",
        )
    finally:
        if own_session:
            await session.close()


# ── STANDALONE TEST ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info(f"AutoTrader starting in {'PAPER' if PAPER_MODE else 'LIVE'} mode")
    log.info(f"Trade size: {TRADE_AMOUNT_SOL} SOL | Stop loss: {STOP_LOSS_AT*100:.0f}% | Tier 1: {TIER1_SELL_AT}x | Tier 2: {TIER2_SELL_AT}x | Exit: {MOONBAG_EXIT_HRS}hr")
    asyncio.run(monitor_loop())
