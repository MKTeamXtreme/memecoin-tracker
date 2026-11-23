"""
creator_guard.py — Serial Rugger Deployer & Parent-Funder Blacklist.

Catches two classes of Pump.fun serial ruggers without false-blocking normal fresh-wallet launches:
  1. Repeat Deployers (0 RPC cost): Any creator wallet that launches >= 2 tokens in 24h on PumpPortal,
     or previously launched a rugged coin in our DB.
  2. Burner-Rotating Ruggers (Parent-Funder Graph): Traces the 1st incoming SOL transfer (>= 0.05 SOL)
     that funded a fresh creator wallet (skipping known CEX/exchange hot wallets & high-velocity routers).
     If that parent funder previously funded >= 2 rugged deployers, blocks the buy automatically.
"""
import asyncio
import base64
import time
from typing import Dict, List, Optional, Set, Tuple

import aiohttp
from solders.pubkey import Pubkey
from database import get_connection

PUMPFUN_PROGRAM = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")

# Major CEX hot wallets, bridges, and Solana system programs (NEVER blacklist as a rugger funder)
KNOWN_CEX_AND_SYSTEM: Set[str] = {
    "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9",  # Binance 2
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",  # Binance 3
    "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S",  # Binance
    "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS",  # Coinbase 1
    "GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE",  # Coinbase 2
    "2AQdpHJ2JpcEgPiATUXjQxA8QmafFegfQwSLWSprPicm",  # Coinbase 3
    "5VCwKtCXgCJ6kit5FybXjvriW3xELsFDhYrPSqtJNmcD",  # OKX
    "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2",  # Bybit
    "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5",  # Kraken
    "BmFdpraQhkiDQE6SnfG5omcA1VwzqfXrwtNYBwWTymy6",  # KuCoin
    "u6PJ8DtQuPFnfmwHbGFULQ4u4EgjDiyYKjVEsynXq2w",   # Gate.io
    "ASTyfSima4LLAdDgoFGkgqoKowG1LZFDr9fAQrg7iaJZ",  # MEXC
    "3yFwqXBfZY4jBVUafQ1YEXw189y2dN3V5KQq9uzBDy1E",  # FixedFloat
    "G2YxRa6wt1qePMwfJzdXZG62ej4qaTC7YURzuh2Lwd3t",  # ChangeNOW
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",   # Pump.fun Program
    "11111111111111111111111111111111",
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
}

REPEAT_WINDOW_SECS  = 24 * 3600
REPEAT_DEPLOY_LIMIT = 2   # >= 2 Pump.fun launches in 24h = serial spam deployer
FUNDER_RUG_LIMIT    = 2   # >= 2 previous rugged deployers funded by same parent wallet = blocked

# In-memory 24h rolling tracker: creator_wallet -> list of (timestamp, mint)
_creator_launches: Dict[str, List[Tuple[float, str]]] = {}
# Cache mint -> creator_wallet (populated from PumpPortal create events or PDA decode)
_mint_to_creator: Dict[str, str] = {}
# Cache creator -> funder_wallet so we don't re-query Helius
_creator_to_funder: Dict[str, Optional[str]] = {}


def init_creator_db():
    conn = get_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bad_deployers (
                wallet      TEXT PRIMARY KEY,
                role        TEXT NOT NULL,
                rug_count   INTEGER DEFAULT 1,
                last_mint   TEXT,
                last_seen   REAL NOT NULL
            )
        """)
        conn.commit()
    finally:
        conn.close()


def record_creator_launch(creator: str, mint: str, now: Optional[float] = None):
    """Record a new token creation from PumpPortal's live stream (0 RPC cost)."""
    if not creator or not mint:
        return
    now = now or time.time()
    _mint_to_creator[mint] = creator
    if len(_mint_to_creator) > 10000:
        # Evict oldest quarter
        for k in list(_mint_to_creator.keys())[:2500]:
            _mint_to_creator.pop(k, None)

    hist = _creator_launches.setdefault(creator, [])
    cutoff = now - REPEAT_WINDOW_SECS
    if hist and hist[0][0] < cutoff:
        hist[:] = [(ts, m) for ts, m in hist if ts >= cutoff]
    if not any(m == mint for _, m in hist):
        hist.append((now, mint))

    if len(_creator_launches) > 10000:
        for k in list(_creator_launches.keys())[:2500]:
            if not _creator_launches[k] or _creator_launches[k][-1][0] < cutoff:
                _creator_launches.pop(k, None)


def get_creator_launch_count(creator: str) -> int:
    if not creator or creator not in _creator_launches:
        return 0
    cutoff = time.time() - REPEAT_WINDOW_SECS
    hist = [x for x in _creator_launches[creator] if x[0] >= cutoff]
    _creator_launches[creator] = hist
    return len(hist)


def get_known_creator(mint: str) -> Optional[str]:
    return _mint_to_creator.get(mint)


def _upsert_bad_deployer(wallet: str, role: str, mint: str):
    if not wallet or wallet in KNOWN_CEX_AND_SYSTEM:
        return
    conn = get_connection()
    try:
        row = conn.execute("SELECT rug_count, last_mint FROM bad_deployers WHERE wallet = ?", (wallet,)).fetchone()
        if row:
            if row["last_mint"] == mint:
                return  # Already counted this rug
            conn.execute(
                "UPDATE bad_deployers SET rug_count = rug_count + 1, last_mint = ?, last_seen = ? WHERE wallet = ?",
                (mint, time.time(), wallet),
            )
        else:
            conn.execute(
                "INSERT INTO bad_deployers (wallet, role, rug_count, last_mint, last_seen) VALUES (?, ?, 1, ?, ?)",
                (wallet, role, mint, time.time()),
            )
        conn.commit()
    except Exception as e:
        print(f"[CreatorGuard] DB upsert error: {e}")
    finally:
        conn.close()


def _lookup_bad_deployer(wallet: str) -> Optional[dict]:
    if not wallet:
        return None
    conn = get_connection()
    try:
        row = conn.execute("SELECT wallet, role, rug_count FROM bad_deployers WHERE wallet = ?", (wallet,)).fetchone()
        return dict(row) if row else None
    except Exception:
        return None
    finally:
        conn.close()


async def resolve_pump_creator(session: aiohttp.ClientSession, mint: str, rpc_url: str) -> Optional[str]:
    """Decode the 32-byte creator pubkey directly from Pump.fun BondingCurve PDA bytes [49:81]."""
    if mint in _mint_to_creator:
        return _mint_to_creator[mint]
    if not mint.endswith("pump"):
        return None
    try:
        pda, _ = Pubkey.find_program_address([b"bonding-curve", bytes(Pubkey.from_string(mint))], PUMPFUN_PROGRAM)
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [str(pda), {"encoding": "base64"}],
        }
        async with session.post(rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)
        val = (data.get("result") or {}).get("value")
        if val and val.get("data"):
            raw = base64.b64decode(val["data"][0])
            if len(raw) >= 81:
                creator = str(Pubkey.from_bytes(raw[49:81]))
                _mint_to_creator[mint] = creator
                return creator
    except Exception:
        return None
    return None


async def find_parent_funder(session: aiohttp.ClientSession, creator: str, rpc_url: str) -> Optional[str]:
    """
    Find the first non-CEX parent wallet that sent >= 0.05 SOL to fund `creator`.
    Uses at most 2 fast RPC calls (oldest signatures + oldest funding tx).
    """
    if creator in _creator_to_funder:
        return _creator_to_funder[creator]
    try:
        p1 = {"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress", "params": [creator, {"limit": 40}]}
        async with session.post(rpc_url, json=p1, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status != 200:
                return None
            sigs = (await resp.json(content_type=None)).get("result") or []
        if not sigs:
            _creator_to_funder[creator] = None
            return None

        # Only trace parent funder on relatively fresh wallets (< 40 txs) — avoids paging huge aged wallets
        if len(sigs) >= 40:
            _creator_to_funder[creator] = None
            return None

        oldest_sig = sigs[-1]["signature"]
        p2 = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "getTransaction",
            "params": [oldest_sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
        }
        async with session.post(rpc_url, json=p2, timeout=aiohttp.ClientTimeout(total=6)) as r2:
            if r2.status != 200:
                return None
            tx = (await r2.json(content_type=None)).get("result")
        if not tx:
            return None

        # Look for native SOL transfer >= 0.05 SOL INTO creator
        instructions = ((tx.get("transaction") or {}).get("message") or {}).get("instructions") or []
        funder = None
        for ix in instructions:
            parsed = ix.get("parsed")
            if isinstance(parsed, dict) and parsed.get("type") == "transfer":
                info = parsed.get("info") or {}
                if info.get("destination") == creator and int(info.get("lamports") or 0) >= 50_000_000:
                    cand = info.get("source")
                    if cand and cand != creator:
                        funder = cand
                        break

        # Fallback: fee payer if it sent SOL to creator and isn't creator
        if not funder:
            acct_keys = ((tx.get("transaction") or {}).get("message") or {}).get("accountKeys") or []
            if acct_keys:
                first_key = acct_keys[0]["pubkey"] if isinstance(acct_keys[0], dict) else acct_keys[0]
                if first_key and first_key != creator:
                    funder = first_key

        if not funder or funder in KNOWN_CEX_AND_SYSTEM:
            _creator_to_funder[creator] = None
            return None

        # Velocity CEX safety guard: if funder made 50 txs in < 5 minutes (>600/hr), it's a CEX/router hot wallet
        p3 = {"jsonrpc": "2.0", "id": 3, "method": "getSignaturesForAddress", "params": [funder, {"limit": 50}]}
        async with session.post(rpc_url, json=p3, timeout=aiohttp.ClientTimeout(total=6)) as r3:
            if r3.status == 200:
                fsigs = (await r3.json(content_type=None)).get("result") or []
                if len(fsigs) >= 50:
                    dt = (fsigs[0].get("blockTime") or 0) - (fsigs[-1].get("blockTime") or 0)
                    if 0 <= dt < 300:
                        _creator_to_funder[creator] = None
                        return None

        _creator_to_funder[creator] = funder
        return funder
    except Exception:
        return None


async def check_creator_risk(session: aiohttp.ClientSession, mint: str, rpc_url: str) -> Tuple[bool, str]:
    """
    Check if a token's creator or parent funder is a known serial rugger / spam deployer.
    Returns (is_bad, reason).
    """
    creator = await resolve_pump_creator(session, mint, rpc_url)
    if not creator:
        return False, "No creator resolved"

    # 1. Repeat serial deployer check (0 RPC cost — live PumpPortal 24h counter)
    launches_24h = get_creator_launch_count(creator)
    if launches_24h >= REPEAT_DEPLOY_LIMIT:
        return True, f"Serial deployer {creator[:8]}.. launched {launches_24h} tokens in 24h"

    # 2. Persistent creator rug blacklist
    bad_c = _lookup_bad_deployer(creator)
    if bad_c and bad_c["rug_count"] >= 1:
        return True, f"Blacklisted rugger creator {creator[:8]}.. ({bad_c['rug_count']} prior rug(s))"

    # 3. Parent-funder blacklist (catches burner-rotating serial ruggers!)
    funder = await find_parent_funder(session, creator, rpc_url)
    if funder:
        bad_f = _lookup_bad_deployer(funder)
        if bad_f and bad_f["rug_count"] >= FUNDER_RUG_LIMIT:
            return True, f"Burner funded by serial rugger parent {funder[:8]}.. ({bad_f['rug_count']} prior rugs)"

    return False, f"Creator {creator[:8]}.. clean"


async def record_rugged_coin(mint: str, rpc_url: str):
    """Auto-learn from any instant rug: blacklist the creator & increment its non-CEX parent funder."""
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            creator = await resolve_pump_creator(session, mint, rpc_url)
            if not creator:
                return
            _upsert_bad_deployer(creator, "creator", mint)
            funder = await find_parent_funder(session, creator, rpc_url)
            if funder:
                _upsert_bad_deployer(funder, "funder", mint)
                print(f"[CreatorGuard] BLACKLISTED rugger creator {creator[:8]}.. & logged parent funder {funder[:8]}..")
            else:
                print(f"[CreatorGuard] BLACKLISTED rugger creator {creator[:8]}..")
    except Exception as e:
        print(f"[CreatorGuard] record_rugged_coin error: {e}")
