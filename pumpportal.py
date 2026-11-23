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
import re
import struct
import time
from dataclasses import dataclass
from typing import Callable, Awaitable, Dict, List, Optional, Set, Tuple

import aiohttp
from solders.pubkey import Pubkey
from news import STOPWORDS, match_breaking_catalyst
from creator_guard import record_creator_launch, get_creator_launch_count, REPEAT_DEPLOY_LIMIT

PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"
PUMPFUN_PROGRAM   = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
WSOL_MINT         = "So11111111111111111111111111111111111111112"

# Incubator bounds
MIN_INCUBATE_SECS     = 110     # Normal tokens: 110s incubation + 45s staircase
FAST_TRACK_AGE_SECS   = 35      # Breaking news / viral meta leaders: fast-track at 35s (skips 110s delay!)
FAST_TRACK_MIN_MC_USD = 15_000.0
MAX_INCUBATE_SECS     = 900     # Evict tokens that haven't reached target MC within 15 minutes
DEAD_FLOOR_SECS       = 160     # After 160s, evict dead tokens still sitting < $4.5k floor
DEAD_FLOOR_USD        = 4_500.0
TARGET_MIN_MC_USD     = 18_500.0
TARGET_MAX_MC_USD     = 26_000.0
MAX_INIT_DEV_BUY      = 80_000_000.0  # Instant t=0 discard if creator sniped >8% in creation tx
MAX_WATCHLIST         = 800
SCAN_INTERVAL_SECS    = 6       # 1 batch RPC call every 6s keeps Helius usage well within free tier

# Meta-Cluster Radar & Copycat Blocker config
CLUSTER_WINDOW_SECS   = 600     # 10-minute rolling surge window
HISTORIC_WINDOW_SECS  = 14400   # 4-hour background window to ensure term is genuinely novel
CLUSTER_MIN_LAUNCHES  = 4       # >=4 fresh launches in 10m = live viral Pump.fun meta
CLUSTER_MAX_PRIOR     = 2       # <=2 mentions in prior 3h50m = brand-new catalyst (not recycled spam)
META_TTL_SECS         = 15 * 60 # Live meta stays active for 15 minutes
COPYCAT_TTL_SECS      = 60 * 60 # Block duplicate copycat names/catalysts for 60 minutes

PUMP_NOISE_WORDS: Set[str] = STOPWORDS | {
    "FUCK", "SHIT", "DICK", "COCK", "BOOB", "ASS", "SEXY", "PORN", "GAY", "CHAD", "BASED",
    "FART", "POOP", "TURD", "WIFE", "HUSBAND", "MAMA", "PAPA", "DADDY", "MOMMY", "GOAT",
    "LION", "BEAR", "WOLF", "DUCK", "BIRD", "FISH", "SHARK", "WHALE", "MONKEY", "APES",
    "TIGER", "PANDA", "BUNNY", "MOUSE", "HORSE", "SNAKE", "DRAGON", "DEVIL", "ANGEL",
    "GHOST", "ALIEN", "ROBOT", "CYBER", "QUANTUM", "MAGIC", "SUPER", "HYPER", "ULTRA",
    "MEGA", "GIGA", "MICRO", "NANO", "ALPHA", "OMEGA", "SIGMA", "BETA", "DELTA", "GAMMA",
    "RICH", "POOR", "MILLIONAIRE", "BILLIONAIRE", "LAMBO", "TESLA", "SPACE", "MARS",
    "SANTA", "XMAS", "CHRISTMAS", "HALLOWEEN", "EASTER", "SUMMER", "WINTER", "SPRING",
    "AUTUMN", "TRENCH", "TRENCHES", "DEGEN", "DEGENS", "JEET", "JEETS", "HODL", "FOMO",
    "WAGMI", "NGMI", "GM", "GN", "COPE", "SEETHE", "CHILL", "VIBE", "VIBES", "CULT",
    "ARMY", "GANG", "CLUB", "SQUAD", "NATION", "EMPIRE", "KINGDOM", "REPUBLIC", "INU",
}

# Cached SOL/USD price
_sol_price_usd: float = 0.0
_sol_price_ts:  float = 0.0

# Meta-cluster & copycat tracking state
_term_launch_ts: Dict[str, List[float]] = {}
_active_metas: Dict[str, dict] = {}
_surfaced_leaders: Dict[str, Tuple[str, float]] = {}  # key -> (mint, ts)


@dataclass
class IncubatedToken:
    mint:             str
    name:             str
    symbol:           str
    curve_key:        str
    created_at:       float
    uri:              str  = ""
    is_breaking_hype: bool = False
    hype_reason:      str  = ""
    catalyst_term:    str  = ""


def _extract_launch_terms(name: str, symbol: str) -> List[str]:
    """Extract clean distinctive keywords from a newly launched Pump.fun token."""
    raw = f"{name or ''} {symbol or ''}"
    words = re.findall(r"[A-Za-z]{4,15}", raw)
    out = []
    seen = set()
    for w in words:
        u = w.upper()
        if u not in PUMP_NOISE_WORDS and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _record_launch_for_meta(name: str, symbol: str, now: float):
    """Update rolling Meta-Cluster Radar on every new Pump.fun token creation."""
    terms = _extract_launch_terms(name, symbol)
    if not terms:
        return
    # Prune expired active metas
    for k in list(_active_metas.keys()):
        if now - _active_metas[k]["ts"] > META_TTL_SECS:
            _active_metas.pop(k, None)

    for u in terms:
        hist = _term_launch_ts.setdefault(u, [])
        # Keep only last 4 hours
        cutoff = now - HISTORIC_WINDOW_SECS
        if hist and hist[0] < cutoff:
            hist[:] = [t for t in hist if t >= cutoff]
        hist.append(now)

        recent_cutoff = now - CLUSTER_WINDOW_SECS
        recent_cnt = sum(1 for t in hist if t >= recent_cutoff)
        prior_cnt  = len(hist) - recent_cnt

        if recent_cnt >= CLUSTER_MIN_LAUNCHES and prior_cnt <= CLUSTER_MAX_PRIOR:
            is_new = u not in _active_metas
            _active_metas[u] = {"term": u, "count": recent_cnt, "ts": now}
            if is_new:
                print(f"[MetaCluster] LIVE VIRAL META DETECTED: {u} ({recent_cnt} launches in 10m, {prior_cnt} prior)")

    # Bound dictionary size
    if len(_term_launch_ts) > 5000:
        cutoff = now - CLUSTER_WINDOW_SECS
        for k in list(_term_launch_ts.keys()):
            if not _term_launch_ts[k] or _term_launch_ts[k][-1] < cutoff:
                _term_launch_ts.pop(k, None)


def match_meta_cluster(symbol: str, name: str) -> Optional[dict]:
    """Return active live Pump.fun Meta-Cluster info if token matches a surging viral meta."""
    now = time.time()
    for k in list(_active_metas.keys()):
        if now - _active_metas[k]["ts"] > META_TTL_SECS:
            _active_metas.pop(k, None)
    if not _active_metas:
        return None
    for term in _extract_launch_terms(name, symbol):
        if term in _active_metas:
            return _active_metas[term]
    return None


def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def is_duplicate_copycat(name: str, symbol: str, mint: str, catalyst_term: str = "") -> Tuple[bool, str]:
    """
    Block copycat tokens #2-#30 that share the exact same name/symbol or breaking catalyst
    as a leader token that already entered our discovery zone within the last 60 minutes.
    """
    now = time.time()
    for k in list(_surfaced_leaders.keys()):
        if now - _surfaced_leaders[k][1] > COPYCAT_TTL_SECS:
            _surfaced_leaders.pop(k, None)

    n_key = _norm_key(name)
    s_key = _norm_key(symbol)
    c_key = f"cat:{catalyst_term.upper()}" if catalyst_term else ""

    if c_key and c_key in _surfaced_leaders and _surfaced_leaders[c_key][0] != mint:
        return True, f"Copycat of active catalyst '{catalyst_term}' (leader already surfaced)"
    if len(n_key) >= 4 and f"name:{n_key}" in _surfaced_leaders and _surfaced_leaders[f"name:{n_key}"][0] != mint:
        return True, f"Duplicate copycat name '{name}' (already seen <60m)"
    if len(s_key) >= 4 and f"sym:{s_key}" in _surfaced_leaders and _surfaced_leaders[f"sym:{s_key}"][0] != mint:
        return True, f"Duplicate copycat symbol '${symbol}' (already seen <60m)"
    return False, ""


def register_surfaced_leader(name: str, symbol: str, mint: str, catalyst_term: str = ""):
    """Register the #1 leader coin for a name/symbol/catalyst so subsequent copycats are blocked."""
    now = time.time()
    n_key = _norm_key(name)
    s_key = _norm_key(symbol)
    if len(n_key) >= 4:
        _surfaced_leaders.setdefault(f"name:{n_key}", (mint, now))
    if len(s_key) >= 4:
        _surfaced_leaders.setdefault(f"sym:{s_key}", (mint, now))
    if catalyst_term:
        _surfaced_leaders.setdefault(f"cat:{catalyst_term.upper()}", (mint, now))


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

                        tok_name = (payload.get("name") or "Unknown").strip() or "Unknown"
                        tok_sym  = (payload.get("symbol") or "???").strip() or "???"
                        creator  = (payload.get("traderPublicKey") or "").strip()
                        now_ts   = time.time()

                        # Feed 100% of Pump.fun launches into the rolling Meta-Cluster Radar
                        _record_launch_for_meta(tok_name, tok_sym, now_ts)

                        # Track creator launch frequency (0 RPC cost) & drop repeat spam deployers immediately
                        if creator:
                            record_creator_launch(creator, mint, now_ts)
                            if get_creator_launch_count(creator) >= REPEAT_DEPLOY_LIMIT:
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
                            name=tok_name,
                            symbol=tok_sym,
                            curve_key=curve_key,
                            created_at=now_ts,
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
                        continue

                    # Check live Breaking News (<1s TreeNews WS) or live Viral Meta-Cluster
                    if not tok.is_breaking_hype:
                        b_cat = match_breaking_catalyst(tok.symbol, tok.name)
                        if b_cat:
                            tok.is_breaking_hype = True
                            tok.catalyst_term    = b_cat["term"]
                            tok.hype_reason      = f"BREAKING ({b_cat['term']}): {b_cat['headline'][:70]}"
                        else:
                            m_cat = match_meta_cluster(tok.symbol, tok.name)
                            if m_cat:
                                tok.is_breaking_hype = True
                                tok.catalyst_term    = m_cat["term"]
                                tok.hype_reason      = f"VIRAL META ({m_cat['term']}: {m_cat['count']} launches in 10m)"

                    min_age = FAST_TRACK_AGE_SECS if tok.is_breaking_hype else MIN_INCUBATE_SECS
                    if age >= min_age:
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

                        # Evict graduated or over-pumped tokens that already left the entry zone
                        if is_complete or (mc_usd is not None and mc_usd > 42_000):
                            watchlist.pop(tok.mint, None)
                            continue

                        # Evict dead floor flatliners after 160s to keep RPC batch tiny
                        if mc_usd is not None and age >= DEAD_FLOOR_SECS and mc_usd < DEAD_FLOOR_USD:
                            watchlist.pop(tok.mint, None)
                            continue

                        min_mc = FAST_TRACK_MIN_MC_USD if tok.is_breaking_hype else TARGET_MIN_MC_USD
                        # Golden Zone live on-chain breakout!
                        if mc_usd is not None and min_mc <= mc_usd <= TARGET_MAX_MC_USD:
                            watchlist.pop(tok.mint, None)
                            asyncio.create_task(on_candidate(tok, mc_usd))

                scan_count += 1
                if scan_count % 20 == 0:
                    print(
                        f"[PumpPortal] Incubating {len(watchlist)} live tokens "
                        f"({len(due_tokens)} checked | {len(_active_metas)} live metas | SOL=${_sol_price_usd:.2f})"
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
