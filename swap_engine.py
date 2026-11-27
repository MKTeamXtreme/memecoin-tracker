"""
swap_engine.py — Production Solana Mainnet Swap & ATA Rent-Reclamation Engine for APEX.

Architecture:
  - Private keys are loaded ONLY from local environment / .env (never sent to any third-party API).
  - Primary Router (Pump.fun / PumpSwap / Raydium): PumpPortal Local Transaction API
    (`https://pumpportal.fun/api/trade-local`, `pool="auto"`), signed locally via `solders`
    (`to_bytes_versioned` + `VersionedTransaction.populate`) and broadcast directly to Helius RPC.
  - Fallback Router: Jupiter Swap API v1 (`https://api.jup.ag/swap/v1/quote` + `/swap`).
  - Reads ACTUAL on-chain SPL Token / Token-2022 balances before selling (supports tiered partial
    exits calculated as exact fraction of remaining tokens + 100% final closes).
  - Confirms transactions on-chain (`getSignatureStatuses`) with automatic retry + escalating
    slippage on emergency exits so stop-losses actually land during violent dumps.
  - Measures actual SOL balance delta from confirmed transactions for accurate realized PnL.
  - Automatically closes empty SPL Token / Token-2022 ATAs after 100% exits to reclaim ~0.00204 SOL rent.
  - Built-in safety fuses: hard max SOL/trade cap, minimum gas-reserve floor, daily max-loss breaker.

CLI utilities (safe — never trades):
  python3 swap_engine.py --check         # Verify .env keypair, RPC, SOL balance & empty ATAs
  python3 swap_engine.py --reclaim-all   # Close all zero-balance token accounts & reclaim SOL rent
"""

import asyncio
import base64
import logging
import os
import time
from dataclasses import dataclass
from typing import Optional, Tuple, List

import aiohttp

log = logging.getLogger("swap_engine")

# ── ENDPOINTS & PROGRAM CONSTANTS ─────────────────────────────────────────────
PUMPPORTAL_LOCAL_URL = "https://pumpportal.fun/api/trade-local"
JUPITER_QUOTE_V1     = "https://api.jup.ag/swap/v1/quote"
JUPITER_SWAP_V1      = "https://api.jup.ag/swap/v1/swap"

SOL_MINT             = "So11111111111111111111111111111111111111112"
TOKEN_PROGRAM_ID     = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM   = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

# Hard safety limits (can be tightened via .env, never exceeded above MAX_TRADE_SOL_HARD_CAP)
MAX_TRADE_SOL_HARD_CAP = 0.50
LAMPORTS_PER_SOL       = 1_000_000_000


@dataclass
class SwapConfig:
    live_mode: bool              = False
    private_key_b58: str         = ""
    wallet_pubkey: str           = ""
    rpc_url: str                 = "https://mainnet.helius-rpc.com/?api-key=87eeee41-3eb7-499f-9fad-0c242d9be5bf"
    jupiter_api_key: str         = ""
    live_trade_sol: float        = 0.10
    live_max_open_trades: int    = 2
    live_smoke_max_trades: int   = 0        # 0 = unlimited; >0 stops opening new live buys after N trades this session
    min_wallet_reserve_sol: float = 0.05
    daily_max_loss_sol: float    = 0.40
    buy_slippage_pct: float      = 8.0
    sell_slippage_pct: float     = 12.0
    emergency_slippage_pct: float = 25.0
    priority_fee_sol: float      = 0.0004
    emergency_priority_sol: float = 0.0008
    router_mode: str             = "auto"   # "auto" (PumpPortal->Jupiter), "pumpportal", "jupiter"


@dataclass
class SwapResult:
    ok: bool
    signature: Optional[str] = None
    router: str = ""
    sol_delta: Optional[float] = None   # Actual wallet SOL delta (+ on sell, - on buy), None if unavailable
    tokens_sold_raw: int = 0
    error: str = ""


# ── ZERO-DEPENDENCY .ENV LOADER ───────────────────────────────────────────────
def load_dotenv_file(env_path: Optional[str] = None) -> None:
    """Load KEY=VALUE pairs from .env into os.environ without overwriting existing env vars."""
    if env_path is None:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except Exception as e:
        log.error(f"Failed to read {env_path}: {e}")


def derive_pubkey(private_key_b58: str) -> str:
    """Derive base58 public key from a base58 Solana secret key using solders."""
    from solders.keypair import Keypair
    kp = Keypair.from_base58_string(private_key_b58.strip())
    return str(kp.pubkey())


def load_swap_config(default_rpc: str = "") -> SwapConfig:
    """Build SwapConfig from environment / .env with strict safety bounds."""
    load_dotenv_file()

    def _f(name: str, default: float) -> float:
        try:
            return float(os.getenv(name, str(default)))
        except ValueError:
            return default

    def _i(name: str, default: int) -> int:
        try:
            return int(os.getenv(name, str(default)))
        except ValueError:
            return default

    rpc = os.getenv("SOLANA_RPC_URL", "").strip() or default_rpc or SwapConfig.rpc_url
    priv = os.getenv("WALLET_PRIVATE_KEY", "").strip()
    pub = ""
    if priv:
        try:
            pub = derive_pubkey(priv)
        except Exception as e:
            log.error(f"Invalid WALLET_PRIVATE_KEY in environment: {e}")
            priv = ""

    want_live = os.getenv("APEX_LIVE_MODE", "false").strip().lower() in ("1", "true", "yes")
    if want_live and not priv:
        log.error("APEX_LIVE_MODE=true requested, but WALLET_PRIVATE_KEY is missing or invalid! Staying in PAPER mode.")
        want_live = False

    trade_sol = min(MAX_TRADE_SOL_HARD_CAP, max(0.01, _f("LIVE_TRADE_SOL", 0.10)))
    max_open  = min(4, max(1, _i("LIVE_MAX_OPEN_TRADES", 2)))
    smoke_cap = max(0, _i("LIVE_SMOKE_MAX_TRADES", 0))

    return SwapConfig(
        live_mode=want_live,
        private_key_b58=priv,
        wallet_pubkey=pub,
        rpc_url=rpc,
        jupiter_api_key=os.getenv("JUPITER_API_KEY", "").strip(),
        live_trade_sol=trade_sol,
        live_max_open_trades=max_open,
        live_smoke_max_trades=smoke_cap,
        min_wallet_reserve_sol=max(0.02, _f("MIN_WALLET_RESERVE_SOL", 0.05)),
        daily_max_loss_sol=max(0.05, _f("DAILY_MAX_LOSS_SOL", 0.40)),
        buy_slippage_pct=min(50.0, max(1.0, _f("BUY_SLIPPAGE_PCT", 8.0))),
        sell_slippage_pct=min(50.0, max(1.0, _f("SELL_SLIPPAGE_PCT", 12.0))),
        emergency_slippage_pct=min(75.0, max(10.0, _f("EMERGENCY_SLIPPAGE_PCT", 25.0))),
        priority_fee_sol=min(0.01, max(0.00005, _f("PRIORITY_FEE_SOL", 0.0004))),
        emergency_priority_sol=min(0.02, max(0.0001, _f("EMERGENCY_PRIORITY_SOL", 0.0008))),
        router_mode=os.getenv("SWAP_ROUTER", "auto").strip().lower(),
    )


# ── LOCAL VERSIONED TRANSACTION SIGNING ───────────────────────────────────────
def sign_versioned_tx_bytes(raw_unsigned_tx: bytes, private_key_b58: str) -> bytes:
    """
    Deserialize a Solana VersionedTransaction (legacy or v0), sign its exact versioned
    message bytes locally using solders, populate signatures at the payer slot, and
    return signed transaction bytes ready for RPC broadcast.
    """
    from solders.keypair import Keypair
    from solders.message import to_bytes_versioned
    from solders.transaction import VersionedTransaction

    kp = Keypair.from_base58_string(private_key_b58.strip())
    tx = VersionedTransaction.from_bytes(raw_unsigned_tx)
    msg = tx.message
    sig = kp.sign_message(to_bytes_versioned(msg))

    # Preserve any additional pre-signed signatures (e.g. if a router attached a signer)
    existing_sigs = list(tx.signatures)
    account_keys = list(msg.account_keys)
    payer_idx = 0
    for idx, ak in enumerate(account_keys):
        if ak == kp.pubkey():
            payer_idx = idx
            break

    if existing_sigs and payer_idx < len(existing_sigs):
        existing_sigs[payer_idx] = sig
        signed = VersionedTransaction.populate(msg, existing_sigs)
    else:
        signed = VersionedTransaction.populate(msg, [sig])

    return bytes(signed)


# ── RPC HELPERS (JSON-RPC OVER AIOHTTP) ───────────────────────────────────────
async def _rpc_call(session: aiohttp.ClientSession, rpc_url: str, method: str, params: list, timeout_s: float = 12.0) -> dict:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    async with session.post(rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
        data = await resp.json(content_type=None)
        if "error" in data and data["error"] is not None:
            raise RuntimeError(f"RPC {method} error: {data['error']}")
        return data.get("result")


async def get_sol_balance(session: aiohttp.ClientSession, rpc_url: str, pubkey: str) -> float:
    """Return wallet native SOL balance."""
    res = await _rpc_call(session, rpc_url, "getBalance", [pubkey, {"commitment": "confirmed"}])
    lamports = int((res or {}).get("value") or 0)
    return lamports / LAMPORTS_PER_SOL


async def get_token_accounts_for_mint(
    session: aiohttp.ClientSession,
    rpc_url: str,
    owner_pubkey: str,
    mint: str,
) -> List[dict]:
    """
    Fetch all token accounts (both SPL Token and Token-2022) owned by `owner_pubkey` for `mint`.
    Returns list of dicts: {pubkey, program_id, raw_amount, decimals, ui_amount}.
    """
    res = await _rpc_call(
        session,
        rpc_url,
        "getTokenAccountsByOwner",
        [owner_pubkey, {"mint": mint}, {"encoding": "jsonParsed", "commitment": "confirmed"}],
    )
    out = []
    for item in (res or {}).get("value") or []:
        acct = item.get("account") or {}
        parsed = ((acct.get("data") or {}).get("parsed") or {}).get("info") or {}
        tok_amt = parsed.get("tokenAmount") or {}
        out.append({
            "pubkey":     item.get("pubkey", ""),
            "program_id": acct.get("owner", TOKEN_PROGRAM_ID),
            "raw_amount": int(tok_amt.get("amount") or 0),
            "decimals":   int(tok_amt.get("decimals") or 6),
            "ui_amount":  float(tok_amt.get("uiAmount") or 0.0),
        })
    return out


async def get_all_empty_token_accounts(
    session: aiohttp.ClientSession,
    rpc_url: str,
    owner_pubkey: str,
) -> List[dict]:
    """Find all zero-balance SPL Token & Token-2022 accounts owned by wallet (candidates for rent reclaim)."""
    empty = []
    for prog in (TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM):
        try:
            res = await _rpc_call(
                session,
                rpc_url,
                "getTokenAccountsByOwner",
                [owner_pubkey, {"programId": prog}, {"encoding": "jsonParsed", "commitment": "confirmed"}],
            )
            for item in (res or {}).get("value") or []:
                acct = item.get("account") or {}
                parsed = ((acct.get("data") or {}).get("parsed") or {}).get("info") or {}
                tok_amt = parsed.get("tokenAmount") or {}
                if int(tok_amt.get("amount") or 0) == 0:
                    empty.append({
                        "pubkey":     item.get("pubkey", ""),
                        "mint":       parsed.get("mint", ""),
                        "program_id": acct.get("owner", prog),
                    })
        except Exception as e:
            log.warning(f"Could not scan empty accounts for {prog[:8]}: {e}")
    return empty


async def send_and_confirm_tx(
    session: aiohttp.ClientSession,
    rpc_url: str,
    signed_tx_bytes: bytes,
    confirm_timeout_s: float = 28.0,
) -> Tuple[bool, Optional[str], str]:
    """
    Broadcast signed transaction to Solana RPC and poll until confirmed/finalized.
    Returns (confirmed_ok, signature, error_msg).
    """
    b64_tx = base64.b64encode(signed_tx_bytes).decode("ascii")
    try:
        sig = await _rpc_call(
            session,
            rpc_url,
            "sendTransaction",
            [b64_tx, {"encoding": "base64", "skipPreflight": True, "preflightCommitment": "processed", "maxRetries": 3}],
            timeout_s=12.0,
        )
    except Exception as e:
        return False, None, f"sendTransaction failed: {e}"

    if not sig:
        return False, None, "RPC returned empty signature"

    deadline = time.time() + confirm_timeout_s
    while time.time() < deadline:
        await asyncio.sleep(0.9)
        try:
            st = await _rpc_call(
                session,
                rpc_url,
                "getSignatureStatuses",
                [[sig], {"searchTransactionHistory": True}],
                timeout_s=8.0,
            )
            val = ((st or {}).get("value") or [None])[0]
            if val is not None:
                if val.get("err") is not None:
                    return False, sig, f"On-chain tx error: {val['err']}"
                conf = val.get("confirmationStatus")
                if conf in ("confirmed", "finalized"):
                    return True, sig, ""
        except Exception:
            pass

    return False, sig, f"Confirmation timed out after {confirm_timeout_s:.0f}s"


async def fetch_tx_sol_delta(
    session: aiohttp.ClientSession,
    rpc_url: str,
    signature: str,
    wallet_pubkey: str,
) -> Optional[float]:
    """Read actual wallet SOL change (in SOL, net of fees) from a confirmed transaction."""
    for _ in range(4):
        try:
            tx = await _rpc_call(
                session,
                rpc_url,
                "getTransaction",
                [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}],
                timeout_s=8.0,
            )
            if tx and tx.get("meta"):
                meta = tx["meta"]
                keys = ((tx.get("transaction") or {}).get("message") or {}).get("accountKeys") or []
                for idx, k in enumerate(keys):
                    pk = k.get("pubkey") if isinstance(k, dict) else k
                    if pk == wallet_pubkey:
                        pre = (meta.get("preBalances") or [0])[idx]
                        post = (meta.get("postBalances") or [0])[idx]
                        return (post - pre) / LAMPORTS_PER_SOL
        except Exception:
            pass
        await asyncio.sleep(0.7)
    return None


# ── ROUTER 1: PUMPPORTAL LOCAL TRANSACTION API ────────────────────────────────
async def _build_pumpportal_tx(
    session: aiohttp.ClientSession,
    wallet_pubkey: str,
    action: str,
    mint: str,
    amount,
    denominated_in_sol: bool,
    slippage_pct: float,
    priority_fee_sol: float,
    pool: str = "auto",
) -> Tuple[Optional[bytes], str]:
    payload = {
        "publicKey":        wallet_pubkey,
        "action":           action,
        "mint":             mint,
        "amount":           amount,
        "denominatedInSol": "true" if denominated_in_sol else "false",
        "slippage":         round(slippage_pct, 2),
        "priorityFee":      round(priority_fee_sol, 6),
        "pool":             pool,
    }
    try:
        async with session.post(
            PUMPPORTAL_LOCAL_URL,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                txt = (await resp.text())[:200]
                return None, f"PumpPortal HTTP {resp.status}: {txt}"
            raw = await resp.read()
            if not raw or len(raw) < 64:
                return None, "PumpPortal returned empty/invalid tx bytes"
            return raw, ""
    except Exception as e:
        return None, f"PumpPortal request error: {e}"


# ── ROUTER 2: JUPITER SWAP API V1 ─────────────────────────────────────────────
async def _build_jupiter_tx(
    session: aiohttp.ClientSession,
    wallet_pubkey: str,
    input_mint: str,
    output_mint: str,
    amount_atomic: int,
    slippage_bps: int,
    priority_fee_sol: float,
    jupiter_api_key: str = "",
) -> Tuple[Optional[bytes], str]:
    headers = {"Accept": "application/json"}
    if jupiter_api_key:
        headers["x-api-key"] = jupiter_api_key

    try:
        async with session.get(
            JUPITER_QUOTE_V1,
            headers=headers,
            params={
                "inputMint":   input_mint,
                "outputMint":  output_mint,
                "amount":      str(int(amount_atomic)),
                "slippageBps": str(int(slippage_bps)),
            },
            timeout=aiohttp.ClientTimeout(total=10),
        ) as qresp:
            if qresp.status != 200:
                txt = (await qresp.text())[:200]
                return None, f"Jupiter quote HTTP {qresp.status}: {txt}"
            quote = await qresp.json(content_type=None)

        priority_lamports = max(10_000, int(priority_fee_sol * LAMPORTS_PER_SOL))
        async with session.post(
            JUPITER_SWAP_V1,
            headers=headers,
            json={
                "quoteResponse":         quote,
                "userPublicKey":         wallet_pubkey,
                "wrapAndUnwrapSol":      True,
                "dynamicComputeUnitLimit": True,
                "prioritizationFeeLamports": priority_lamports,
            },
            timeout=aiohttp.ClientTimeout(total=10),
        ) as sresp:
            if sresp.status != 200:
                txt = (await sresp.text())[:200]
                return None, f"Jupiter swap HTTP {sresp.status}: {txt}"
            sdata = await sresp.json(content_type=None)
            tx_b64 = sdata.get("swapTransaction")
            if not tx_b64:
                return None, f"Jupiter swap missing swapTransaction: {str(sdata)[:160]}"
            return base64.b64decode(tx_b64), ""
    except Exception as e:
        return None, f"Jupiter request error: {e}"


# ── PUBLIC BUY / SELL EXECUTION ───────────────────────────────────────────────
async def buy_token_live(
    session: aiohttp.ClientSession,
    cfg: SwapConfig,
    mint: str,
    amount_sol: float,
    max_attempts: int = 2,
) -> SwapResult:
    """
    Execute a live SOL -> Token buy with safety caps, SOL balance reserve check,
    local signing, RPC confirmation, and automatic router fallback.
    """
    if not cfg.private_key_b58 or not cfg.wallet_pubkey:
        return SwapResult(ok=False, error="Missing wallet keypair")
    if amount_sol <= 0 or amount_sol > MAX_TRADE_SOL_HARD_CAP:
        return SwapResult(ok=False, error=f"Buy size {amount_sol} SOL violates hard safety cap (0 < x <= {MAX_TRADE_SOL_HARD_CAP})")

    # Safety check: ensure wallet retains at least MIN_WALLET_RESERVE_SOL for exit gas
    try:
        bal = await get_sol_balance(session, cfg.rpc_url, cfg.wallet_pubkey)
        needed = amount_sol + cfg.min_wallet_reserve_sol
        if bal < needed:
            return SwapResult(
                ok=False,
                error=f"Insufficient SOL balance ({bal:.4f} SOL < {needed:.4f} SOL incl. {cfg.min_wallet_reserve_sol:.2f} gas reserve)",
            )
    except Exception as e:
        return SwapResult(ok=False, error=f"Could not verify wallet SOL balance before buy: {e}")

    routers = _router_order(cfg.router_mode, mint)
    last_err = ""

    for attempt in range(max_attempts):
        slip = cfg.buy_slippage_pct + (attempt * 4.0)
        prio = cfg.priority_fee_sol * (1.5 if attempt > 0 else 1.0)
        for router in routers:
            if router == "pumpportal":
                raw_tx, err = await _build_pumpportal_tx(
                    session, cfg.wallet_pubkey, "buy", mint,
                    round(amount_sol, 6), True, slip, prio, pool="auto",
                )
            else:
                raw_tx, err = await _build_jupiter_tx(
                    session, cfg.wallet_pubkey, SOL_MINT, mint,
                    int(amount_sol * LAMPORTS_PER_SOL), int(slip * 100), prio, cfg.jupiter_api_key,
                )
            if not raw_tx:
                last_err = f"{router}: {err}"
                log.warning(f"[LIVE BUY] {mint[:8]}.. attempt {attempt+1} {last_err}")
                continue

            try:
                signed = sign_versioned_tx_bytes(raw_tx, cfg.private_key_b58)
            except Exception as e:
                last_err = f"Local sign error: {e}"
                log.error(f"[LIVE BUY] {last_err}")
                return SwapResult(ok=False, error=last_err)

            ok, sig, cerr = await send_and_confirm_tx(session, cfg.rpc_url, signed)
            if ok and sig:
                sol_delta = await fetch_tx_sol_delta(session, cfg.rpc_url, sig, cfg.wallet_pubkey)
                log.info(f"[LIVE BUY CONFIRMED] {mint[:8]}.. via {router} | SOL delta: {sol_delta if sol_delta is not None else -amount_sol:+.4f} | https://solscan.io/tx/{sig}")
                return SwapResult(ok=True, signature=sig, router=router, sol_delta=sol_delta)

            last_err = f"{router} tx failed ({sig}): {cerr}"
            log.warning(f"[LIVE BUY] {mint[:8]}.. attempt {attempt+1} {last_err}")

    return SwapResult(ok=False, error=last_err or "All buy attempts failed")


async def sell_token_live(
    session: aiohttp.ClientSession,
    cfg: SwapConfig,
    mint: str,
    pct_of_remaining: float,
    is_final_exit: bool = False,
    is_emergency: bool = False,
    max_attempts: int = 3,
) -> SwapResult:
    """
    Execute a live Token -> SOL sell based on ACTUAL on-chain token balance.
      - `pct_of_remaining`: fraction of CURRENT wallet token balance to sell (0.0 < x <= 1.0).
      - `is_final_exit`: sells 100% of balance and automatically reclaims the empty ATA rent (~0.00204 SOL).
      - `is_emergency`: uses higher priority fee & aggressive escalating slippage (for stop-losses / rugs).
    """
    if not cfg.private_key_b58 or not cfg.wallet_pubkey:
        return SwapResult(ok=False, error="Missing wallet keypair")

    # Fetch actual on-chain token balance (works for both SPL Token & Token-2022)
    try:
        accts = await get_token_accounts_for_mint(session, cfg.rpc_url, cfg.wallet_pubkey, mint)
    except Exception as e:
        return SwapResult(ok=False, error=f"Failed to read token balance for {mint[:8]}..: {e}")

    total_raw = sum(a["raw_amount"] for a in accts)
    decimals = accts[0]["decimals"] if accts else 6
    if total_raw <= 0:
        log.warning(f"[LIVE SELL] {mint[:8]}.. wallet holds 0 tokens (already sold?).")
        if is_final_exit and accts:
            asyncio.create_task(_safe_reclaim_mint(cfg, mint))
        return SwapResult(ok=True, router="noop_zero_bal", sol_delta=0.0, tokens_sold_raw=0)

    sell_frac = 1.0 if is_final_exit else min(1.0, max(0.01, pct_of_remaining))
    raw_to_sell = total_raw if sell_frac >= 0.995 else max(1, int(total_raw * sell_frac))
    ui_to_sell = raw_to_sell / (10 ** decimals)

    routers = _router_order(cfg.router_mode, mint)
    base_slip = cfg.emergency_slippage_pct if is_emergency else cfg.sell_slippage_pct
    base_prio = cfg.emergency_priority_sol if is_emergency else cfg.priority_fee_sol
    last_err = ""

    for attempt in range(max_attempts):
        # Escalate slippage aggressively on retries so exits never get stranded on dumping charts
        slip = min(75.0, base_slip + (attempt * (12.0 if is_emergency else 8.0)))
        prio = min(0.01, base_prio * (1.0 + 0.5 * attempt))

        for router in routers:
            if router == "pumpportal":
                pp_amount = "100%" if sell_frac >= 0.995 else round(ui_to_sell, decimals)
                raw_tx, err = await _build_pumpportal_tx(
                    session, cfg.wallet_pubkey, "sell", mint,
                    pp_amount, False, slip, prio, pool="auto",
                )
            else:
                raw_tx, err = await _build_jupiter_tx(
                    session, cfg.wallet_pubkey, mint, SOL_MINT,
                    raw_to_sell, int(slip * 100), prio, cfg.jupiter_api_key,
                )
            if not raw_tx:
                last_err = f"{router}: {err}"
                log.warning(f"[LIVE SELL] {mint[:8]}.. attempt {attempt+1} {last_err}")
                continue

            try:
                signed = sign_versioned_tx_bytes(raw_tx, cfg.private_key_b58)
            except Exception as e:
                last_err = f"Local sign error: {e}"
                log.error(f"[LIVE SELL] {last_err}")
                return SwapResult(ok=False, error=last_err)

            ok, sig, cerr = await send_and_confirm_tx(session, cfg.rpc_url, signed)
            if ok and sig:
                sol_delta = await fetch_tx_sol_delta(session, cfg.rpc_url, sig, cfg.wallet_pubkey)
                log.info(
                    f"[LIVE SELL CONFIRMED] {mint[:8]}.. ({sell_frac*100:.0f}% of bal) via {router} "
                    f"| SOL delta: {sol_delta if sol_delta is not None else 0.0:+.4f} | https://solscan.io/tx/{sig}"
                )
                if is_final_exit:
                    asyncio.create_task(_safe_reclaim_mint(cfg, mint))
                return SwapResult(
                    ok=True,
                    signature=sig,
                    router=router,
                    sol_delta=sol_delta,
                    tokens_sold_raw=raw_to_sell,
                )

            last_err = f"{router} tx failed ({sig}): {cerr}"
            log.warning(f"[LIVE SELL] {mint[:8]}.. attempt {attempt+1} {last_err}")

    return SwapResult(ok=False, error=last_err or "All sell attempts failed")


def _router_order(router_mode: str, mint: str) -> List[str]:
    if router_mode == "pumpportal":
        return ["pumpportal"]
    if router_mode == "jupiter":
        return ["jupiter"]
    # Auto: PumpPortal primary for Pump.fun mints (instant bonding-curve + PumpSwap support), Jupiter fallback
    if mint.endswith("pump"):
        return ["pumpportal", "jupiter"]
    return ["jupiter", "pumpportal"]


# ── AUTOMATIC EMPTY ATA RENT RECLAMATION (~0.00204 SOL PER CLOSED TRADE) ─────
def build_close_ata_tx_bytes(
    ata_pubkey_str: str,
    token_program_id_str: str,
    recent_blockhash_str: str,
    private_key_b58: str,
) -> bytes:
    """
    Build & sign a VersionedTransaction containing an SPL Token / Token-2022 CloseAccount
    instruction (opcode 9 — identical in both Tokenkeg and Token-2022) to reclaim ~0.00204 SOL rent.
    """
    from solders.hash import Hash
    from solders.instruction import AccountMeta, Instruction
    from solders.keypair import Keypair
    from solders.message import MessageV0, to_bytes_versioned
    from solders.pubkey import Pubkey
    from solders.transaction import VersionedTransaction

    kp = Keypair.from_base58_string(private_key_b58.strip())
    owner_pk = kp.pubkey()
    ata_pk = Pubkey.from_string(ata_pubkey_str)
    prog_pk = Pubkey.from_string(token_program_id_str)
    bh = Hash.from_string(recent_blockhash_str)

    # SPL Token / Token-2022 CloseAccount instruction index = 9
    close_ix = Instruction(
        program_id=prog_pk,
        accounts=[
            AccountMeta(pubkey=ata_pk,   is_signer=False, is_writable=True),
            AccountMeta(pubkey=owner_pk, is_signer=False, is_writable=True),
            AccountMeta(pubkey=owner_pk, is_signer=True,  is_writable=False),
        ],
        data=bytes([9]),
    )
    msg = MessageV0.try_compile(
        payer=owner_pk,
        instructions=[close_ix],
        address_lookup_table_accounts=[],
        recent_blockhash=bh,
    )
    sig = kp.sign_message(to_bytes_versioned(msg))
    signed = VersionedTransaction.populate(msg, [sig])
    return bytes(signed)


async def reclaim_empty_ata(
    session: aiohttp.ClientSession,
    cfg: SwapConfig,
    mint: str,
) -> int:
    """Close any zero-balance token accounts for `mint` and return number of ATAs reclaimed."""
    if not cfg.private_key_b58 or not cfg.wallet_pubkey:
        return 0
    accts = await get_token_accounts_for_mint(session, cfg.rpc_url, cfg.wallet_pubkey, mint)
    reclaimed = 0
    for a in accts:
        if a["raw_amount"] != 0:
            continue
        try:
            bh_res = await _rpc_call(session, cfg.rpc_url, "getLatestBlockhash", [{"commitment": "confirmed"}])
            bh_str = ((bh_res or {}).get("value") or {}).get("blockhash")
            if not bh_str:
                continue
            signed = build_close_ata_tx_bytes(a["pubkey"], a["program_id"], bh_str, cfg.private_key_b58)
            ok, sig, err = await send_and_confirm_tx(session, cfg.rpc_url, signed, confirm_timeout_s=20.0)
            if ok:
                reclaimed += 1
                log.info(f"[ATA RECLAIMED] +0.00204 SOL rent recovered for {mint[:8]}.. | https://solscan.io/tx/{sig}")
            else:
                log.warning(f"[ATA RECLAIM] Could not close {a['pubkey'][:8]}..: {err}")
        except Exception as e:
            log.warning(f"[ATA RECLAIM] Error closing {a['pubkey'][:8]}..: {e}")
    return reclaimed


async def _safe_reclaim_mint(cfg: SwapConfig, mint: str) -> None:
    """Background helper to reclaim ATA rent 2s after a confirmed 100% sell."""
    await asyncio.sleep(2.0)
    connector = aiohttp.TCPConnector(ssl=False)
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            await reclaim_empty_ata(session, cfg, mint)
    except Exception as e:
        log.warning(f"[ATA RECLAIM] Background task error for {mint[:8]}..: {e}")


# ── SAFE CLI DIAGNOSTICS (--check / --reclaim-all) ───────────────────────────
async def _cli_main(reclaim_all: bool = False):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [SWAP] %(message)s", datefmt="%H:%M:%S")
    cfg = load_swap_config()
    print(f"Live mode enabled : {cfg.live_mode} (APEX_LIVE_MODE)")
    print(f"Router mode       : {cfg.router_mode}")
    print(f"Trade size (live) : {cfg.live_trade_sol:.4f} SOL (hard cap {MAX_TRADE_SOL_HARD_CAP} SOL)")
    print(f"Max open trades   : {cfg.live_max_open_trades} | Smoke-test cap: {cfg.live_smoke_max_trades or 'unlimited'}")
    print(f"Min gas reserve   : {cfg.min_wallet_reserve_sol:.4f} SOL")
    print(f"Daily loss limit  : {cfg.daily_max_loss_sol:.4f} SOL")
    if not cfg.wallet_pubkey:
        print("Wallet            : NOT CONFIGURED (add WALLET_PRIVATE_KEY to .env when ready for live trading)")
        return
    print(f"Wallet pubkey     : {cfg.wallet_pubkey}")
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        bal = await get_sol_balance(session, cfg.rpc_url, cfg.wallet_pubkey)
        empty = await get_all_empty_token_accounts(session, cfg.rpc_url, cfg.wallet_pubkey)
        print(f"SOL balance       : {bal:.5f} SOL")
        print(f"Empty ATAs found  : {len(empty)} (~{len(empty)*0.002039:.5f} SOL reclaimable rent)")
        if reclaim_all and empty:
            closed = 0
            for a in empty:
                bh_res = await _rpc_call(session, cfg.rpc_url, "getLatestBlockhash", [{"commitment": "confirmed"}])
                bh_str = ((bh_res or {}).get("value") or {}).get("blockhash")
                signed = build_close_ata_tx_bytes(a["pubkey"], a["program_id"], bh_str, cfg.private_key_b58)
                ok, sig, err = await send_and_confirm_tx(session, cfg.rpc_url, signed, confirm_timeout_s=20.0)
                if ok:
                    closed += 1
                    print(f"  Reclaimed {a['pubkey']} ({a['mint'][:8]}..) -> {sig}")
                else:
                    print(f"  Failed {a['pubkey']}: {err}")
            print(f"Total reclaimed: {closed}/{len(empty)} accounts (~{closed*0.002039:.5f} SOL)")


if __name__ == "__main__":
    import sys
    asyncio.run(_cli_main(reclaim_all=("--reclaim-all" in sys.argv)))
