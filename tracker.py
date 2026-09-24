"""
tracker.py — Polls DexScreener every 5 minutes to record market cap
snapshots (5min, 15min, 1hr, 24hr) and compute peak MC for each coin.
"""
import asyncio
import time
import aiohttp
from database import get_pending_tracking, update_mc_snapshot, mark_tracking_done
from paper import update_paper_trade

DEXSCREENER_URL = "https://api.dexscreener.com/latest/dex/tokens/{}"


async def fetch_mc(session: aiohttp.ClientSession, mint: str) -> float | None:
    """Fetch current USD market cap from DexScreener."""
    try:
        url = DEXSCREENER_URL.format(mint)
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)
            pairs = data.get("pairs") or []
            if not pairs:
                return None
            # Pick pair with highest liquidity
            best = max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
            mc = best.get("marketCap") or best.get("fdv")
            return float(mc) if mc else None
    except Exception:
        return None


async def run_outcome_update():
    """Check all pending coins and update their MC snapshots."""
    pending = get_pending_tracking()
    if not pending:
        return 0

    updated = 0
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        for coin in pending:
            mint     = coin["mint"]
            detected = coin["detected_at"]
            elapsed  = time.time() - detected

            # Skip coins detected less than 50 seconds ago
            if elapsed < 50:
                continue

            mc = await fetch_mc(session, mint)
            if mc is None:
                await asyncio.sleep(0.3)
                continue

            # 1-minute snapshot
            if elapsed >= 60 and coin["mc_1min"] is None:
                update_mc_snapshot(mint, "mc_1min", mc)
                updated += 1
            update_paper_trade(mint, mc, is_final=False)

            # 5-minute snapshot
            if elapsed >= 5 * 60 and coin["mc_5min"] is None:
                update_mc_snapshot(mint, "mc_5min", mc)
                updated += 1

            # 15-minute snapshot
            if elapsed >= 15 * 60 and coin["mc_15min"] is None:
                update_mc_snapshot(mint, "mc_15min", mc)
                updated += 1

            # 1-hour snapshot
            if elapsed >= 60 * 60 and coin["mc_1hr"] is None:
                update_mc_snapshot(mint, "mc_1hr", mc)
                updated += 1

            # 24-hour snapshot — mark done
            if elapsed >= 24 * 60 * 60 and coin["mc_24hr"] is None:
                update_mc_snapshot(mint, "mc_24hr", mc)
                mark_tracking_done(mint)
                update_paper_trade(mint, mc, is_final=True)
                updated += 1

            # Always update peak
            update_mc_snapshot(mint, "peak_mc", mc)

            # Throttle to avoid DexScreener rate limits
            await asyncio.sleep(0.25)

    return updated

