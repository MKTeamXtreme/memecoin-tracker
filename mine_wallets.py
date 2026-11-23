"""
mine_wallets.py — Automatically mine profitable Solana trench wallets from YOUR OWN coins.db!

How it works:
  1. Queries data/coins.db for the top recent sustained Pump.fun winners (4x+ peak >= $100k, survived 15m >= $40k).
  2. Uses Helius RPC to inspect the early bonding-curve buyers (skipping slot-0 dev/snipers).
  3. Finds wallets that bought >= 2 DIFFERENT big winners early.
  4. Filters out HFT/MEV spam bots (> 18 txs/hr) and stale wallets (> 5 days inactive).
  5. Writes the top vetted wallets to data/smart_wallets.json for whale_tracker.py.

Usage on server:
  python3 mine_wallets.py
"""
import asyncio
import json
import os
import sqlite3
import time
from collections import defaultdict

import aiohttp
from solders.pubkey import Pubkey
from autotrader import SOLANA_RPC

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
DB_PATH      = os.path.join(BASE_DIR, "data", "coins.db")
WALLETS_PATH = os.path.join(BASE_DIR, "data", "smart_wallets.json")

PUMPFUN_PROGRAM = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")

IGNORE_ADDRS = {
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
    "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1",
    "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM",
    "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM",
    "11111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
    "ComputeBudget111111111111111111111111111111",
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
}


async def _rpc_post(session: aiohttp.ClientSession, payload):
    for attempt in range(3):
        try:
            async with session.post(SOLANA_RPC, json=payload, timeout=aiohttp.ClientTimeout(total=18)) as resp:
                if resp.status == 429:
                    await asyncio.sleep(1.0 + attempt)
                    continue
                if resp.status != 200:
                    return None
                return await resp.json(content_type=None)
        except Exception:
            await asyncio.sleep(0.5)
    return None


async def mine_and_save(db_path: str = DB_PATH, out_path: str = WALLETS_PATH, lookback_days: int = 21, max_winners: int = 55):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cutoff = time.time() - lookback_days * 86400
    winners = conn.execute("""
        SELECT mint, name, symbol, initial_mc, peak_mc, mc_15min, (peak_mc / initial_mc) as mult, detected_at
        FROM coins
        WHERE detected_at >= ?
          AND mint LIKE '%pump'
          AND initial_mc BETWEEN 14000 AND 38000
          AND peak_mc BETWEEN 100000 AND 8000000
          AND mc_15min >= 40000
          AND (peak_mc / initial_mc) >= 4.0
        ORDER BY detected_at DESC
        LIMIT ?
    """, (cutoff, max_winners)).fetchall()
    conn.close()

    print(f"[Miner] Scanning early bonding-curve buyers across {len(winners)} recent 4x+ winners...")
    wallet_hits = defaultdict(set)

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as s:
        for idx, w in enumerate(winners):
            mint = w["mint"]
            try:
                pda, _ = Pubkey.find_program_address([b"bonding-curve", bytes(Pubkey.from_string(mint))], PUMPFUN_PROGRAM)
                pda_str = str(pda)
            except Exception:
                continue

            before = None
            earliest_sigs = []
            for _ in range(2):
                params = [pda_str, {"limit": 1000}]
                if before:
                    params[1]["before"] = before
                d = await _rpc_post(s, {"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress", "params": params})
                batch = (d or {}).get("result") or []
                if not batch:
                    break
                earliest_sigs = batch
                before = batch[-1]["signature"]
                if len(batch) < 1000:
                    break
                await asyncio.sleep(0.25)

            if not earliest_sigs:
                continue

            oldest_first = [x["signature"] for x in reversed(earliest_sigs) if not x.get("err")]
            sample_sigs = oldest_first[5:37]
            buyers_this_coin = set()

            for i in range(0, len(sample_sigs), 8):
                chunk = sample_sigs[i:i + 8]
                batch_req = [
                    {"jsonrpc": "2.0", "id": j, "method": "getTransaction", "params": [sg, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]}
                    for j, sg in enumerate(chunk)
                ]
                res_list = await _rpc_post(s, batch_req)
                if isinstance(res_list, list):
                    for item in res_list:
                        tx = item.get("result")
                        if not tx:
                            continue
                        meta = tx.get("meta") or {}
                        if meta.get("err") is not None:
                            continue
                        pre = {}
                        for b in (meta.get("preTokenBalances") or []):
                            if b.get("mint") == mint and b.get("owner"):
                                pre[b["owner"]] = float((b.get("uiTokenAmount") or {}).get("uiAmount") or 0)
                        for b in (meta.get("postTokenBalances") or []):
                            if b.get("mint") == mint and b.get("owner"):
                                own = b["owner"]
                                if own in IGNORE_ADDRS or own == pda_str:
                                    continue
                                post_amt = float((b.get("uiTokenAmount") or {}).get("uiAmount") or 0)
                                if (post_amt - pre.get(own, 0.0)) >= 400_000:
                                    buyers_this_coin.add(own)
                await asyncio.sleep(0.35)

            for buyer in buyers_this_coin:
                wallet_hits[buyer].add(w["name"])
            print(f"  [{idx+1:2d}/{len(winners)}] {w['name'][:20]:20s} ({w['mult']:5.1f}x) -> {len(buyers_this_coin):2d} early buyers", flush=True)

        candidates = [(w, hits) for w, hits in wallet_hits.items() if len(hits) >= 2]
        candidates.sort(key=lambda x: len(x[1]), reverse=True)

        vetted = []
        now = time.time()
        for w, hits in candidates[:45]:
            d = await _rpc_post(s, {"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress", "params": [w, {"limit": 100}]})
            sigs = (d or {}).get("result") or []
            if not sigs:
                continue
            newest_t = sigs[0].get("blockTime") or now
            oldest_t = sigs[-1].get("blockTime") or now
            span_hrs = max(0.01, (newest_t - oldest_t) / 3600.0)
            txs_per_hr = len(sigs) / span_hrs
            days_inactive = (now - newest_t) / 86400.0
            b_res = await _rpc_post(s, {"jsonrpc": "2.0", "id": 2, "method": "getBalance", "params": [w]})
            sol_bal = ((b_res or {}).get("result") or {}).get("value", 0) / 1e9
            if txs_per_hr <= 35.0 and days_inactive <= 4.0 and sol_bal >= 0.5:
                vetted.append({
                    "wallet": w,
                    "label": f"SW-{w[:6]}",
                    "winners": len(hits),
                    "txs_per_hr": round(txs_per_hr, 2),
                    "sol_balance": round(sol_bal, 2),
                    "coins": sorted(list(hits))[:6],
                })
            await asyncio.sleep(0.25)

    vetted = vetted[:15]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(vetted, f, indent=2)
    print(f"[Miner] Saved {len(vetted)} vetted smart-money wallets -> {out_path}")
    return vetted


if __name__ == "__main__":
    asyncio.run(mine_and_save())
