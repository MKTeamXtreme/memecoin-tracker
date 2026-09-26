import sqlite3
import time
from database import get_connection

# Strategy: sell HALF at 2x, exit MOONBAG at 24hr
PAPER_INVEST   = 0.02
PAPER_HALF_AT  = 2.0   # sell half when coin 2x's
PAPER_EXIT_HRS = 24    # close moonbag after 24 hours

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
            exit_mc         REAL,
            exit_sol        REAL    DEFAULT 0,
            profit_sol      REAL    DEFAULT 0,
            status          TEXT    DEFAULT 'OPEN',
            created_at      REAL    NOT NULL
        )
    """)
    conn.commit()
    conn.close()

def open_paper_trade(mint: str, entry_mc: float, amount_sol: float = PAPER_INVEST):
    if entry_mc <= 0: return
    conn = get_connection()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO paper_trades 
               (mint, entry_mc, invested_sol, created_at) VALUES (?, ?, ?, ?)""",
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

        # Sell half at 2x
        if not trade["sold_half"] and current_mc >= trade["entry_mc"] * PAPER_HALF_AT:
            sold_half_sol = (trade["invested_sol"] / 2.0) * PAPER_HALF_AT  # always lock in exactly 2x on the half
            updates["sold_half"]     = 1
            updates["sold_half_sol"] = sold_half_sol
            trade["sold_half"]     = 1
            trade["sold_half_sol"] = sold_half_sol

        # Stop Loss at -50%
        mult = current_mc / trade["entry_mc"]
        is_stop_loss = not trade["sold_half"] and mult <= 0.50

        # Close moonbag at 24hr
        is_final = elapsed_secs >= PAPER_EXIT_HRS * 3600
        
        if is_final or is_stop_loss:
            rem_sol  = (trade["invested_sol"] / 2.0) if trade["sold_half"] else trade["invested_sol"]
            exit_sol = rem_sol * mult
            total_returned = (trade.get("sold_half_sol") or 0) + (updates.get("sold_half_sol") or 0) + exit_sol
            updates["exit_mc"]    = current_mc
            updates["exit_sol"]   = exit_sol
            updates["profit_sol"] = total_returned - trade["invested_sol"]
            updates["status"]     = 'CLOSED'

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
        profit        = conn.execute("SELECT SUM(profit_sol) FROM paper_trades WHERE status = 'CLOSED'").fetchone()[0] or 0.0
        return {
            "total_trades": total,
            "open_trades":  open_trades,
            "closed_trades": closed_trades,
            "net_profit_sol": round(profit, 4)
        }
    finally:
        conn.close()

def get_recent_paper_trades(limit: int = 50):
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT pt.mint, c.name, pt.entry_mc, pt.invested_sol,
                   pt.sold_half, pt.sold_half_sol, pt.profit_sol,
                   pt.status, pt.created_at, pt.exit_mc, c.score_breakdown
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
                "is_news": is_news
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

    # Use mc_24hr as the moonbag exit
    rows = conn.execute(
        "SELECT mint, initial_mc, peak_mc, mc_24hr, mc_1min, mc_5min, mc_15min, mc_1hr FROM coins WHERE score = 100 AND initial_mc > 0"
    ).fetchall()
    conn.close()

    for mint, initial_mc, peak_mc, mc_24hr, mc_1min, mc_5min, mc_15min, mc_1hr in rows:
        # 1-min momentum filter (same as autotrader MIN_1MIN_MULT = 1.0)
        # If it doesn't hold its entry value at 1 minute, autotrader skips it.
        if mc_1min is not None and (mc_1min / initial_mc) < 1.0:
            continue  # Skipped by autotrader due to no momentum

        open_paper_trade(mint, initial_mc, PAPER_INVEST)

        # Simulate 2x half-sell
        if peak_mc and peak_mc >= initial_mc * PAPER_HALF_AT:
            update_paper_trade(mint, initial_mc * PAPER_HALF_AT, elapsed_secs=0)

        # Simulate stop-loss using snapshots
        stop_triggered = False
        for snap in [mc_1min, mc_5min, mc_15min, mc_1hr]:
            if snap is not None and snap <= initial_mc * 0.50:
                update_paper_trade(mint, snap, elapsed_secs=0)  # Trigger stop loss
                stop_triggered = True
                break
        
        if not stop_triggered:
            if mc_24hr is not None:
                # Exit moonbag at 24hr price
                update_paper_trade(mint, mc_24hr, elapsed_secs=PAPER_EXIT_HRS * 3600)

if __name__ == "__main__":
    print("Running paper trade backfill (1hr exit strategy)...")
    backfill_paper_trades()
    print("Done!", get_paper_stats())
