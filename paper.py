import sqlite3
import time
from database import get_connection

# Strategy: Option A Ladder
PAPER_INVEST      = 0.02
PAPER_HALF_AT     = 1.5   # Tier 1: sell 50% when coin reaches 1.5x (returns 0.015 SOL)
PAPER_TIER2_AT    = 3.0   # Tier 2: sell 25% when coin reaches 3.0x (returns 0.015 SOL)
PAPER_EXIT_HRS    = 24    # close moonbag after 24 hours
TRAILING_STOP_PCT = 0.50  # Trailing stop loss (50%)

def init_paper_db():
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS paper_trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            mint            TEXT    UNIQUE NOT NULL,
            entry_mc        REAL    NOT NULL,
            invested_sol    REAL    NOT NULL,
            sold_half       INTEGER DEFAULT 0,
            sold_half_sol   REAL    DEFAULT 0,
            sold_tier2      INTEGER DEFAULT 0,
            sold_tier2_sol  REAL    DEFAULT 0,
            exit_mc         REAL,
            exit_sol        REAL    DEFAULT 0,
            profit_sol      REAL    DEFAULT 0,
            status          TEXT    DEFAULT 'OPEN',
            created_at      REAL    NOT NULL
        )
    """)
    conn.commit()

    # Safely upgrade existing database with new columns
    try:
        conn.execute("ALTER TABLE paper_trades ADD COLUMN strategy TEXT DEFAULT 'V1'")
        conn.commit()
    except sqlite3.OperationalError:
        pass

    try:
        conn.execute("ALTER TABLE paper_trades ADD COLUMN peak_mc REAL")
        conn.commit()
    except sqlite3.OperationalError:
        pass

    try:
        conn.execute("ALTER TABLE paper_trades ADD COLUMN sold_tier2 INTEGER DEFAULT 0")
        conn.commit()
    except sqlite3.OperationalError:
        pass

    try:
        conn.execute("ALTER TABLE paper_trades ADD COLUMN sold_tier2_sol REAL DEFAULT 0")
        conn.commit()
    except sqlite3.OperationalError:
        pass
    
    conn.close()

def open_paper_trade(mint: str, entry_mc: float, amount_sol: float = PAPER_INVEST):
    if entry_mc <= 0: return
    conn = get_connection()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO paper_trades 
               (mint, entry_mc, invested_sol, created_at, strategy) VALUES (?, ?, ?, ?, 'V2')""",
            (mint, entry_mc, amount_sol, time.time())
        )
        conn.commit()
    finally:
        conn.close()

def update_paper_trade(mint: str, current_mc: float, elapsed_secs: float):
    """Call on every tracking tick. Closes the trade automatically at 1hr."""
    if current_mc is None or current_mc <= 0: return
    conn = get_connection()
    try:
        trade = conn.execute("SELECT * FROM paper_trades WHERE mint = ? AND status = 'OPEN'", (mint,)).fetchone()
        if not trade:
            return

        trade   = dict(trade)
        updates = {}

        # Tier 1: Sell 50% at 1.5x
        if not trade["sold_half"] and current_mc >= trade["entry_mc"] * PAPER_HALF_AT:
            sold_half_sol = (trade["invested_sol"] * 0.50) * PAPER_HALF_AT
            updates["sold_half"]     = 1
            updates["sold_half_sol"] = sold_half_sol
            trade["sold_half"]     = 1
            trade["sold_half_sol"] = sold_half_sol

        # Tier 2: Sell 25% at 3.0x
        if trade["sold_half"] and not trade.get("sold_tier2") and current_mc >= trade["entry_mc"] * PAPER_TIER2_AT:
            sold_tier2_sol = (trade["invested_sol"] * 0.25) * PAPER_TIER2_AT
            updates["sold_tier2"]     = 1
            updates["sold_tier2_sol"] = sold_tier2_sol
            trade["sold_tier2"]     = 1
            trade["sold_tier2_sol"] = sold_tier2_sol

        # Track peak MC
        peak_mc = max(trade.get("peak_mc") or current_mc, current_mc)
        if peak_mc > (trade.get("peak_mc") or 0):
            updates["peak_mc"] = peak_mc
            trade["peak_mc"] = peak_mc

        mult = current_mc / trade["entry_mc"]

        # Determine remaining position fraction
        if trade.get("sold_tier2") or updates.get("sold_tier2"):
            rem_frac = 0.25
        elif trade.get("sold_half") or updates.get("sold_half"):
            rem_frac = 0.50
        else:
            rem_frac = 1.0

        # Stop Loss at -30% (Initial, before Tier 1)
        is_stop_loss = not trade["sold_half"] and mult <= 0.70

        # Trailing Stop Loss on moonbag/remainder with 1.0x Break-Even Floor
        peak_mult = peak_mc / trade["entry_mc"]
        trailing_threshold = max(1.0, peak_mult * (1.0 - TRAILING_STOP_PCT))
        is_trailing_stop = trade["sold_half"] and mult <= trailing_threshold

        # Close moonbag at 24hr
        is_final = elapsed_secs >= PAPER_EXIT_HRS * 3600
        
        # Already realized returned SOL from previous tier sales
        already_locked = (
            (trade.get("sold_half_sol") or 0) + (updates.get("sold_half_sol") or 0) +
            (trade.get("sold_tier2_sol") or 0) + (updates.get("sold_tier2_sol") or 0)
        )

        # Real-time mark-to-market
        rem_sol = trade["invested_sol"] * rem_frac
        exit_sol = rem_sol * mult
        updates["profit_sol"] = already_locked + exit_sol - trade["invested_sol"]

        if is_final or is_stop_loss or is_trailing_stop:
            if is_stop_loss:
                # Stop loss triggers at 0.70x (-30%)
                exit_mult = min(0.70, mult) if mult > 0 else 0.70
                updates["exit_mc"]    = trade["entry_mc"] * exit_mult
                updates["exit_sol"]   = trade["invested_sol"] * exit_mult
                updates["profit_sol"] = updates["exit_sol"] - trade["invested_sol"]
            elif is_trailing_stop:
                # Trailing stop locked at drop from peak, with guaranteed 1.0x break-even floor
                exit_mult = max(1.0, mult)
                updates["exit_mc"]    = trade["entry_mc"] * exit_mult
                updates["exit_sol"]   = rem_sol * exit_mult
                updates["profit_sol"] = already_locked + updates["exit_sol"] - trade["invested_sol"]
            else:
                updates["exit_mc"]    = current_mc
                updates["exit_sol"]   = exit_sol
                updates["profit_sol"] = already_locked + exit_sol - trade["invested_sol"]
            updates["status"] = 'CLOSED'

        if updates:
            set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
            conn.execute(f"UPDATE paper_trades SET {set_clause} WHERE mint = ?", list(updates.values()) + [mint])
            conn.commit()
    finally:
        conn.close()

def get_paper_stats():
    conn = get_connection()
    try:
        try:
            total = conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0]
        except sqlite3.OperationalError:
            return {"total_trades": 0, "open_trades": 0, "closed_trades": 0, "net_profit_sol": 0.0}

        open_trades   = conn.execute("SELECT COUNT(*) FROM paper_trades WHERE status = 'OPEN'").fetchone()[0]
        closed_trades = conn.execute("SELECT COUNT(*) FROM paper_trades WHERE status = 'CLOSED'").fetchone()[0]
        
        try:
            v1_profit = conn.execute("SELECT SUM(profit_sol) FROM paper_trades WHERE strategy = 'V1'").fetchone()[0] or 0.0
            v2_profit = conn.execute("SELECT SUM(profit_sol) FROM paper_trades WHERE strategy = 'V2'").fetchone()[0] or 0.0
            profit = conn.execute("SELECT SUM(profit_sol) FROM paper_trades").fetchone()[0] or 0.0
        except sqlite3.OperationalError:
            v1_profit = 0.0
            v2_profit = 0.0
            profit = 0.0

        return {
            "total_trades": total,
            "open_trades":  open_trades,
            "closed_trades": closed_trades,
            "net_profit_sol": round(profit, 4),
            "v1_profit_sol": round(v1_profit, 4),
            "v2_profit_sol": round(v2_profit, 4)
        }
    finally:
        conn.close()

def get_recent_paper_trades(limit: int = 50):
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT pt.mint, c.name, pt.entry_mc, pt.invested_sol,
                   pt.sold_half, pt.sold_half_sol, pt.profit_sol,
                   pt.status, pt.created_at, pt.exit_mc, c.score_breakdown, pt.strategy,
                   COALESCE(pt.sold_tier2, 0), COALESCE(pt.sold_tier2_sol, 0)
            FROM paper_trades pt
            LEFT JOIN coins c ON c.mint = pt.mint
            ORDER BY pt.created_at DESC LIMIT ?
        """, (limit,)).fetchall()
        
        trades = []
        for r in rows:
            # Check if this was a news match by looking at the breakdown
            is_news = False
            if r[10]:
                is_news = "trend_bonus" in r[10] or "NEWS/HYPE MATCH" in r[10]

            trades.append({
                "mint": r[0],
                "name": r[1],
                "entry_mc": r[2],
                "invested_sol": r[3],
                "sold_half": bool(r[4]),
                "sold_half_sol": r[5],
                "profit_sol": r[6] or 0.0,
                "status": r[7],
                "created_at": r[8],
                "exit_mc": r[9],
                "is_news": is_news,
                "strategy": r[11] if len(r) > 11 else "V1",
                "sold_tier2": bool(r[12]),
                "sold_tier2_sol": r[13]
            })
        return trades
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()

def backfill_paper_trades():
    init_paper_db()
    conn = get_connection()

    # Wipe old paper trades so backfill is clean
    conn.execute("DELETE FROM paper_trades")
    conn.commit()

    rows = conn.execute(
        "SELECT mint, initial_mc, peak_mc, mc_24hr, mc_1min, mc_5min, mc_15min, mc_1hr FROM coins WHERE score = 100 AND initial_mc > 0"
    ).fetchall()
    conn.close()

    for mint, initial_mc, peak_mc, mc_24hr, mc_1min, mc_5min, mc_15min, mc_1hr in rows:
        # 1-min momentum filter: 1.20x gives ~80% win rate
        if mc_1min is None or (mc_1min / initial_mc) < 1.20:
            continue

        open_paper_trade(mint, initial_mc, PAPER_INVEST)

        # Tier 1 (50% at 1.5x)
        if peak_mc and peak_mc >= initial_mc * PAPER_HALF_AT:
            update_paper_trade(mint, initial_mc * PAPER_HALF_AT, elapsed_secs=60)

        # Tier 2 (25% at 3.0x)
        if peak_mc and peak_mc >= initial_mc * PAPER_TIER2_AT:
            update_paper_trade(mint, initial_mc * PAPER_TIER2_AT, elapsed_secs=120)

        # Stop-loss at 0.70x if didn't reach 1.5x
        if peak_mc is None or peak_mc < initial_mc * PAPER_HALF_AT:
            update_paper_trade(mint, initial_mc * 0.70, elapsed_secs=180)
            continue

        # Trailing stop loss on moonbag (50% from peak, min 1.0x break-even floor)
        trailing_stop_mc = max(initial_mc, peak_mc * (1.0 - TRAILING_STOP_PCT))
        # Trigger trailing stop
        update_paper_trade(mint, trailing_stop_mc, elapsed_secs=300)

if __name__ == "__main__":
    print("Running Option A paper trade backfill (50% at 1.5x, 25% at 3x, 25% Moonbag)...")
    backfill_paper_trades()
    print("Done!", get_paper_stats())
