"""
main.py — FastAPI backend for the Solana Memecoin Trial Run Tracker.

Coin Discovery:
  - Rotates through 8 DexScreener search queries to continuously surface
    new low-cap Solana memecoins (no auth required, server-accessible).
  - Also polls the DexScreener token-profiles endpoint every 3rd cycle
    to catch truly brand-new listings.

Outcome Tracking:
  - Every 5 minutes: records market cap snapshots at 5min, 15min, 1hr, 24hr.

Frontend:
  - WebSocket pushes live coin alerts and stat updates to the browser.
"""
import asyncio
import json
import os
import sys
import time
from contextlib import asynccontextmanager
from typing import Set, List, Dict

import aiohttp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import init_db, save_coin, get_recent_coins, get_stats, get_biggest_winners
from paper import init_paper_db, open_paper_trade, get_paper_stats
from autotrader import on_new_coin, monitor_loop, PAPER_MODE, TRADE_AMOUNT_SOL
from scorer import score_coin
from tracker import run_outcome_update
from news import refresh_trends, _trending_symbols, _news_keywords

# ── Constants ─────────────────────────────────────────────────────────────────
# Rotate through these queries to keep finding fresh low-cap Solana tokens
SEARCH_QUERIES = [
    "https://api.dexscreener.com/latest/dex/search?q=solana%20new",
    "https://api.dexscreener.com/latest/dex/search?q=pump%20sol",
    "https://api.dexscreener.com/latest/dex/search?q=meme%20solana",
    "https://api.dexscreener.com/latest/dex/search?q=sol%20token",
    "https://api.dexscreener.com/latest/dex/search?q=raydium%20sol",
    "https://api.dexscreener.com/latest/dex/search?q=pepe%20sol",
    "https://api.dexscreener.com/latest/dex/search?q=inu%20solana",
    "https://api.dexscreener.com/latest/dex/search?q=coin%20sol",
    "https://api.dexscreener.com/latest/dex/search?q=baby%20sol",
    "https://api.dexscreener.com/latest/dex/search?q=floki%20sol",
]

DEXSCREENER_PROFILES  = "https://api.dexscreener.com/token-profiles/latest/v1"
DEXSCREENER_PAIRS_URL = "https://api.dexscreener.com/latest/dex/tokens/{}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; memecoin-tracker/1.0)",
    "Accept": "application/json",
}

MIN_SCORE      = 35
POLL_INTERVAL  = 15      # seconds between polls
TRACK_INTERVAL = 60      # seconds between outcome updates

# Only surface coins in this MC range
MC_MIN = 10_000          # raised from $2k — sub-$10k coins showed 0 wins in trial data
MC_MAX = 1_000_000

FRONTEND_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend")

# ── State ─────────────────────────────────────────────────────────────────────
seen_mints: Set[str]      = set()
ws_clients: Set[WebSocket] = set()
search_idx: int            = 0


# ── WebSocket broadcast ────────────────────────────────────────────────────────
async def broadcast(payload: dict):
    dead = set()
    msg  = json.dumps(payload)
    for ws in ws_clients:
        try:
            await ws.send_text(msg)
        except Exception:
            dead.add(ws)
    ws_clients.difference_update(dead)


# ── Normalize DexScreener pair into scorer-compatible shape ───────────────────
def pair_to_raw(pair: dict) -> dict:
    info    = pair.get("info") or {}
    socials = {s.get("type", "").lower(): s.get("url", "") for s in (info.get("socials") or [])}
    websites= [w.get("url", "") for w in (info.get("websites") or [])]
    return {
        "mint":            pair.get("baseToken", {}).get("address", ""),
        "name":            pair.get("baseToken", {}).get("name", "Unknown"),
        "symbol":          pair.get("baseToken", {}).get("symbol", "???"),
        "image_uri":       info.get("imageUrl", ""),
        "description":     "",
        "twitter":         socials.get("twitter", ""),
        "telegram":        socials.get("telegram", ""),
        "website":         websites[0] if websites else "",
        "reply_count":     0,
        "usd_market_cap":  pair.get("marketCap") or pair.get("fdv") or 0,
        "is_currently_live":          False,
        "king_of_the_hill_timestamp": None,
        "created_timestamp":          pair.get("pairCreatedAt"),
    }


# ── Process a batch of DexScreener pairs ──────────────────────────────────────
async def process_pairs(pairs: List[dict]) -> List[dict]:
    new_found = []
    for pair in pairs:
        if pair.get("chainId") != "solana":
            continue

        mint = (pair.get("baseToken") or {}).get("address", "")
        if not mint or mint in seen_mints:
            continue

        mc = float(pair.get("marketCap") or pair.get("fdv") or 0)
        if mc < MC_MIN or mc > MC_MAX:
            continue

        # Only tokens created in the last 24 hours
        created_ms = pair.get("pairCreatedAt")
        if created_ms:
            age_h = (time.time() * 1000 - created_ms) / 3_600_000
            if age_h > 24:
                continue

        seen_mints.add(mint)
        raw   = pair_to_raw(pair)
        score, breakdown = score_coin(raw)

        if score < MIN_SCORE:
            continue

        record = {
            "mint":         mint,
            "name":         raw["name"],
            "symbol":       raw["symbol"],
            "image_uri":    raw["image_uri"],
            "description":  raw["description"],
            "twitter_url":  raw["twitter"],
            "telegram_url": raw["telegram"],
            "website_url":  raw["website"],
            "has_twitter":  bool(raw["twitter"]),
            "has_telegram": bool(raw["telegram"]),
            "has_website":  bool(raw["website"]),
            "reply_count":  0,
            "created_at":   (created_ms / 1000) if created_ms else time.time(),
            "detected_at":  time.time(),
            "initial_mc":   mc,
            "score":        score,
            "score_breakdown": breakdown,
        }

        if save_coin(record):
            new_found.append(record)
            if score == 100:
                open_paper_trade(mint, mc, 0.02)
                asyncio.create_task(on_new_coin(
                    session=None, mint=mint,
                    name=raw["name"], entry_mc=mc,
                ))

    return new_found


# ── Source A: rotating keyword search ─────────────────────────────────────────
async def poll_search(session: aiohttp.ClientSession, url: str) -> List[dict]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                data  = await resp.json(content_type=None)
                pairs = data.get("pairs") or []
                return await process_pairs(pairs)
            elif resp.status == 429:
                print("[Poll] Rate limited -- waiting 20s")
                await asyncio.sleep(20)
            else:
                print(f"[Poll] Search HTTP {resp.status}")
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"[Poll] Search error: {e}")
    return []


# ── Source B: token profiles (newest listings) ─────────────────────────────────
async def poll_profiles(session: aiohttp.ClientSession) -> List[dict]:
    found = []
    try:
        async with session.get(
            DEXSCREENER_PROFILES, timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status == 200:
                profiles = await resp.json(content_type=None)
                sol_profiles = [p for p in (profiles or []) if p.get("chainId") == "solana"]
                for profile in sol_profiles[:20]:
                    addr = profile.get("tokenAddress", "")
                    if addr and addr not in seen_mints:
                        try:
                            async with session.get(
                                DEXSCREENER_PAIRS_URL.format(addr),
                                timeout=aiohttp.ClientTimeout(total=10)
                            ) as pr:
                                if pr.status == 200:
                                    pdata = await pr.json(content_type=None)
                                    pairs = pdata.get("pairs") or []
                                    new = await process_pairs(pairs)
                                    found.extend(new)
                        except Exception:
                            pass
                        await asyncio.sleep(0.15)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"[Poll] Profiles error: {e}")
    return found


# ── Main poll loop ─────────────────────────────────────────────────────────────
async def poll_loop():
    global search_idx
    connector  = aiohttp.TCPConnector(ssl=False)
    poll_count = 0

    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        while True:
            found = []
            try:
                if poll_count % 3 == 0:
                    # Every 3rd poll: profiles endpoint (newest token listings)
                    found = await poll_profiles(session)
                else:
                    # Other polls: rotate through search keywords
                    url = SEARCH_QUERIES[search_idx % len(SEARCH_QUERIES)]
                    search_idx += 1
                    found = await poll_search(session, url)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[Poll] Unexpected error: {e}")

            for coin in found:
                await broadcast({"type": "new_coin", "coin": coin})

            if found:
                print(f"[Poll] #{poll_count} -- {len(found)} saved | {len(seen_mints)} seen total")
            elif poll_count % 5 == 0:
                print(f"[Poll] #{poll_count} -- scanning... | {len(seen_mints)} seen, {len(seen_mints) - len(found)} rejected by score")

            poll_count += 1
            await asyncio.sleep(POLL_INTERVAL)


# ── Outcome tracking loop ──────────────────────────────────────────────────────
async def track_loop():
    # Run immediately on startup to catch up on any missed snapshots
    await asyncio.sleep(5)  # short delay to let server fully boot first
    while True:
        try:
            n = await run_outcome_update()
            if n:
                stats = get_stats()
                await broadcast({"type": "stats_update", "stats": stats})
                print(f"[Track] Updated {n} MC snapshot(s)")
            else:
                print("[Track] Checked outcomes -- no new snapshots due yet")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[Track] Error: {e}")
        await asyncio.sleep(TRACK_INTERVAL)  # then wait 5 minutes between each run


async def news_loop():
    """Refresh trending coins and news keywords every 5 minutes."""
    while True:
        try:
            await refresh_trends()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[News] Error: {e}")
        await asyncio.sleep(5 * 60)




# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    init_paper_db()
    for coin in get_recent_coins(limit=5000):
        seen_mints.add(coin["mint"])
    print(f"[Boot] {len(seen_mints)} existing coins pre-loaded")

    # Fetch trending data immediately on boot
    await refresh_trends()

    poll_task   = asyncio.create_task(poll_loop())
    track_task  = asyncio.create_task(track_loop())
    news_task   = asyncio.create_task(news_loop())
    trader_task = asyncio.create_task(monitor_loop())
    mode = 'PAPER' if PAPER_MODE else 'LIVE'
    print(f"[Boot] AutoTrader started in {mode} mode ({TRADE_AMOUNT_SOL} SOL per trade)")
    print("[Boot] Background tasks started")
    print("[Boot] Open http://localhost:8000 in your browser")

    yield

    poll_task.cancel()
    track_task.cancel()
    news_task.cancel()
    trader_task.cancel()


# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(title="Memecoin Tracker", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


@app.get("/")
async def root():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


@app.get("/api/coins")
async def api_coins(limit: int = 150, offset: int = 0):
    return JSONResponse(get_recent_coins(limit, offset))


@app.get("/api/biggest_winners")
async def api_biggest_winners(limit: int = 50):
    return JSONResponse(get_biggest_winners(limit))


@app.get("/api/stats")
async def api_stats():
    stats = get_stats()
    stats["paper"] = get_paper_stats()
    return JSONResponse(stats)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.add(ws)
    try:
        coins = get_recent_coins(limit=50)
        stats = get_stats()
        stats["paper"] = get_paper_stats()
        await ws.send_text(json.dumps({"type": "initial", "coins": coins, "stats": stats}))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        ws_clients.discard(ws)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
