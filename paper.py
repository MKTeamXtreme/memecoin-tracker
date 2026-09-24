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

        # Close moonbag at 1hr
        is_final = elapsed_secs >= PAPER_EXIT_HRS * 3600
        if is_final:
            rem_sol  = (trade["invested_sol"] / 2.0) if trade["sold_half"] else trade["invested_sol"]
            exit_sol = rem_sol * (current_mc / trade["entry_mc"])
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

def backfill_paper_trades():
    init_paper_db()
    conn = get_connection()

    # Wipe old paper trades so backfill is clean
    conn.execute("DELETE FROM paper_trades")
    conn.commit()

    # Use mc_24hr as the moonbag exit — proven best exit (+51% ROI vs -32% for 1hr)
    rows = conn.execute(
        "SELECT mint, initial_mc, peak_mc, mc_24hr FROM coins WHERE score = 100 AND initial_mc > 0 AND mc_24hr IS NOT NULL"
    ).fetchall()
    conn.close()

    for mint, initial_mc, peak_mc, mc_24hr in rows:
        open_paper_trade(mint, initial_mc, PAPER_INVEST)

        # Simulate 2x half-sell
        if peak_mc and peak_mc >= initial_mc * PAPER_HALF_AT:
            update_paper_trade(mint, initial_mc * PAPER_HALF_AT, elapsed_secs=0)

        # Exit moonbag at 24hr price
        update_paper_trade(mint, mc_24hr, elapsed_secs=PAPER_EXIT_HRS * 3600)

if __name__ == "__main__":
    print("Running paper trade backfill (1hr exit strategy)...")
    backfill_paper_trades()
    print("Done!", get_paper_stats())
