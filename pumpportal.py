"""
pumpportal.py — Real-time Pump.fun WebSocket discovery & direct on-chain bonding-curve pricing.

How it works:
  1. Connects to wss://pumpportal.fun/api/data (free, no API key) and subscribes to
     100% of new Pump.fun token creations ('subscribeNewToken').
  2. Holds newly created tokens in a bounded in-memory incubator until they reach
     >= 110s age (filtering early launch-spike traps).
  3. Batch-reads live on-chain Pump.fun bonding-curve accounts via Solana RPC
     getMultipleAccounts (up to 100 curves per single RPC call) with zero DexScreener lag.
  4. Immediately surfaces any token entering the $18.5k-$26k Golden Discovery Zone to
     main.py for scoring, Helius bundle/Sybil checks, dashboard broadcast, and V4 trading.
"""

import asyncio
import base64
import json
import struct
import time
from dataclasses import dataclass
from typing import Callable, Awaitable, Dict, List, Optional, Set, Tuple

import aiohttp
from solders.pubkey import Pubkey

PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"
PUMPFUN_PROGRAM   = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
WSOL_MINT         = "So11111111111111111111111111111111111111112"

# Incubator bounds
MIN_INCUBATE_SECS = 110     # 110s incubation + 45s staircase = ~155s-165s buy age (0/10 rugs in Nov 10-13 out-of-sample)
MAX_INCUBATE_SECS = 900     # Evict tokens that haven't reached $18.5k within 15 minutes
DEAD_FLOOR_SECS   = 160     # After 160s, evict dead tokens still sitting < $4.5k floor
DEAD_FLOOR_USD    = 4_500.0
TARGET_MIN_MC_USD = 18_500.0
TARGET_MAX_MC_USD = 26_000.0
MAX_INIT_DEV_BUY  = 80_000_000.0  # Instant t=0 discard if creator sniped >8% in creation tx
MAX_WATCHLIST     = 800
SCAN_INTERVAL_SECS= 6       # 1 batch RPC call every 6s keeps Helius usage well within free tier

# Cached SOL/USD price
_sol_price_usd: float = 0.0
_sol_price_ts:  float = 0.0


@dataclass
class IncubatedToken:
    mint:       str
    name:       str
    symbol:     str
    curve_key:  str
    created_at: float
    uri:        str = ""


watchlist: Dict[str, IncubatedToken] = {}
graduated_mints: Set[str] = set()


def derive_bonding_curve_pda(mint: str) -> Optional[str]:
    """Deterministically derive the Pump.fun bonding curve PDA for any mint."""
    try:
        mint_pk = Pubkey.from_string(mint)
        pda, _ = Pubkey.find_program_address([b"bonding-curve", bytes(mint_pk)], PUMPFUN_PROGRAM)
        return str(pda)
    except Exception:
        return None


def decode_bonding_curve_mc_sol(b64_data: str) -> Tuple[Optional[float], bool]:
    """
    Decode raw Pump.fun BondingCurve Anchor account data.
    Layout after 8-byte Anchor discriminator:
      v_token_reserves (u64), v_sol_reserves (u64),
      r_token_reserves (u64), r_sol_reserves (u64),
      token_total_supply (u64), complete (bool/u8)
    Returns (market_cap_in_sol, is_complete_graduated).
    """
    try:
        raw = base64.b64decode(b64_data)
        if len(raw) < 49:
            return None, False
        v_tok, v_sol, r_tok, _r_sol, tot_sup, complete = struct.unpack_from("<QQQQQB", raw, 8)
        if complete or v_tok == 0 or r_tok == 0:
            return None, True
        if tot_sup <= 0:
            return None, False
        mc_sol = (tot_sup / 1e6) * ((v_sol / 1e9) / (v_tok / 1e6))
        return mc_sol, False
    except Exception:
        return None, False


async def get_sol_price_usd(session: aiohttp.ClientSession) -> float:
    """Return cached SOL/USD price, refreshing every 45 seconds."""
    global _sol_price_usd, _sol_price_ts
    now = time.time()
    if _sol_price_usd > 10.0 and (now - _sol_price_ts) < 45:
        return _sol_price_usd

    # Primary: DexScreener WSOL/USDC highest-liquidity Solana pool
    try:
        async with session.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{WSOL_MINT}",
            timeout=aiohttp.ClientTimeout(total=6),
        ) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                pairs = [
                    p for p in (data.get("pairs") or [])
                    if p.get("chainId") == "solana"
                    and (p.get("quoteToken") or {}).get("symbol") in ("USDC", "USDT")
                ]
                if pairs:
                    best = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0))
                    price = float(best.get("priceUsd") or 0)
                    if price > 10.0:
                        _sol_price_usd = price
                        _sol_price_ts = now
                        return _sol_price_usd
    except Exception:
        pass

    # Fallback: CoinGecko simple price
    try:
        async with session.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
            timeout=aiohttp.ClientTimeout(total=6),
        ) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                price = float((data.get("solana") or {}).get("usd") or 0)
                if price > 10.0:
                    _sol_price_usd = price
                    _sol_price_ts = now
                    return _sol_price_usd
    except Exception:
        pass

    return _sol_price_usd if _sol_price_usd > 10.0 else 95.0


async def fetch_pump_curve_mc_usd(
    session: aiohttp.ClientSession,
    mint: str,
    rpc_url: str,
) -> Optional[float]:
    """
    Zero-lag on-chain USD Market Cap reader for active Pump.fun bonding-curve tokens.
    Returns None once the token graduates to PumpSwap/Raydium so callers fall back to DexScreener.
    """
    if mint in graduated_mints:
        return None

    curve_key = derive_bonding_curve_pda(mint)
    if not curve_key:
        return None

    try:
        sol_usd = await get_sol_price_usd(session)
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getMultipleAccounts",
            "params": [[curve_key], {"encoding": "base64"}],
        }
        async with session.post(rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status != 200:
                return None
            data = await r.json(content_type=None)
            vals = ((data.get("result") or {}).get("value")) or []
            if not vals or vals[0] is None:
                graduated_mints.add(mint)
                return None
            acc = vals[0]
            if acc.get("owner") != str(PUMPFUN_PROGRAM):
                graduated_mints.add(mint)
                return None
            b64_list = acc.get("data") or []
            if not b64_list:
                return None
            mc_sol, is_complete = decode_bonding_curve_mc_sol(b64_list[0])
            if is_complete:
                graduated_mints.add(mint)
                if len(graduated_mints) > 2000:
                    graduated_mints.clear()
                return None
            if mc_sol and mc_sol > 0:
                return mc_sol * sol_usd
    except Exception:
        return None
    return None


async def _fetch_batch_curves(
    session: aiohttp.ClientSession,
    tokens: List[IncubatedToken],
    rpc_url: str,
    sol_usd: float,
) -> Dict[str, Tuple[Optional[float], bool]]:
    """Batch-fetch up to 100 bonding curves per single RPC call. Returns {mint: (mc_usd, is_complete)}."""
    results: Dict[str, Tuple[Optional[float], bool]] = {}
    for i in range(0, len(tokens), 100):
        chunk = tokens[i : i + 100]
        keys = [t.curve_key for t in chunk]
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getMultipleAccounts",
            "params": [keys, {"encoding": "base64"}],
        }
        try:
            async with session.post(rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200:
                    continue
                data = await r.json(content_type=None)
                vals = ((data.get("result") or {}).get("value")) or []
                for tok, acc in zip(chunk, vals):
                    if acc is None:
                        results[tok.mint] = (None, False)
                        continue
                    b64_list = acc.get("data") or []
                    if not b64_list:
                        results[tok.mint] = (None, False)
                        continue
                    mc_sol, is_complete = decode_bonding_curve_mc_sol(b64_list[0])
                    mc_usd = (mc_sol * sol_usd) if (mc_sol and mc_sol > 0) else None
                    results[tok.mint] = (mc_usd, is_complete)
        except Exception:
            continue
    return results


async def _ws_listener(seen_mints: Set[str]):
    """Maintain persistent connection to PumpPortal WebSocket and ingest new tokens."""
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                async with session.ws_connect(PUMPPORTAL_WS_URL, heartbeat=20, timeout=15) as ws:
                    await ws.send_json({"method": "subscribeNewToken"})
                    print("[PumpPortal] Connected live to wss://pumpportal.fun/api/data (100% Pump.fun stream)")
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        try:
                            payload = json.loads(msg.data)
                        except Exception:
                            continue

                        if payload.get("txType") != "create":
                            continue

                        mint = payload.get("mint") or ""
                        if not mint or mint in seen_mints or mint in watchlist:
                            continue

                        init_buy = float(payload.get("initialBuy") or 0.0)
                        if init_buy > MAX_INIT_DEV_BUY:
                            continue

                        curve_key = payload.get("bondingCurveKey") or derive_bonding_curve_pda(mint)
                        if not curve_key:
                            continue

                        # Enforce strict upper memory cap
                        if len(watchlist) >= MAX_WATCHLIST:
                            oldest_mint = next(iter(watchlist))
                            watchlist.pop(oldest_mint, None)

                        watchlist[mint] = IncubatedToken(
                            mint=mint,
                            name=(payload.get("name") or "Unknown").strip() or "Unknown",
                            symbol=(payload.get("symbol") or "???").strip() or "???",
                            curve_key=curve_key,
                            created_at=time.time(),
                            uri=payload.get("uri") or "",
                        )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[PumpPortal] WS reconnecting after error: {e}")
                await asyncio.sleep(4)


async def _incubator_scanner(
    rpc_url: str,
    seen_mints: Set[str],
    on_candidate: Callable[[IncubatedToken, float], Awaitable[None]],
):
    """Every SCAN_INTERVAL_SECS, batch-check incubated tokens aged >= 110s for $18.5k-$26k breakouts."""
    connector = aiohttp.TCPConnector(ssl=False)
    scan_count = 0
    async with aiohttp.ClientSession(connector=connector) as session:
        await get_sol_price_usd(session)
        while True:
            try:
                now = time.time()
                to_evict: List[str] = []
                due_tokens: List[IncubatedToken] = []

                for mint, tok in list(watchlist.items()):
                    if mint in seen_mints:
                        to_evict.append(mint)
                        continue
                    age = now - tok.created_at
                    if age > MAX_INCUBATE_SECS:
                        to_evict.append(mint)
                    elif age >= MIN_INCUBATE_SECS:
                        due_tokens.append(tok)

                for m in to_evict:
                    watchlist.pop(m, None)

                if due_tokens:
                    sol_usd = await get_sol_price_usd(session)
                    curve_map = await _fetch_batch_curves(session, due_tokens, rpc_url, sol_usd)

                    for tok in due_tokens:
                        if tok.mint not in watchlist:
                            continue
                        mc_usd, is_complete = curve_map.get(tok.mint, (None, False))
                        age = now - tok.created_at

                        # Evict graduated or over-pumped tokens that already left the $18.5k-$26k entry zone
                        if is_complete or (mc_usd is not None and mc_usd > 42_000):
                            watchlist.pop(tok.mint, None)
                            continue

                        # Evict dead floor flatliners after 160s to keep RPC batch tiny
                        if mc_usd is not None and age >= DEAD_FLOOR_SECS and mc_usd < DEAD_FLOOR_USD:
                            watchlist.pop(tok.mint, None)
                            continue

                        # Golden Zone live on-chain breakout!
                        if mc_usd is not None and TARGET_MIN_MC_USD <= mc_usd <= TARGET_MAX_MC_USD:
                            watchlist.pop(tok.mint, None)
                            asyncio.create_task(on_candidate(tok, mc_usd))

                scan_count += 1
                if scan_count % 20 == 0:
                    print(
                        f"[PumpPortal] Incubating {len(watchlist)} live tokens "
                        f"({len(due_tokens)} aged >=110s checked | SOL=${_sol_price_usd:.2f})"
                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[PumpPortal] Scanner error: {e}")

            await asyncio.sleep(SCAN_INTERVAL_SECS)


async def run_pumpportal_feed(
    rpc_url: str,
    seen_mints: Set[str],
    on_candidate: Callable[[IncubatedToken, float], Awaitable[None]],
):
    """Run both the live WebSocket token stream and the batched on-chain curve scanner."""
    await asyncio.gather(
        _ws_listener(seen_mints),
        _incubator_scanner(rpc_url, seen_mints, on_candidate),
    )
