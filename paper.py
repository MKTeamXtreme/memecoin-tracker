import sqlite3
import time
from database import get_connection

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

def open_paper_trade(mint: str, entry_mc: float, amount_sol: float = 0.02):
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

def update_paper_trade(mint: str, current_mc: float, is_final: bool = False):
    if current_mc is None: current_mc = 0
    conn = get_connection()
    try:
        trade = conn.execute("SELECT * FROM paper_trades WHERE mint = ? AND status = 'OPEN'", (mint,)).fetchone()
        if not trade:
            return
        
        trade = dict(trade)
        updates = {}
        
        if not trade["sold_half"] and current_mc >= trade["entry_mc"] * 2:
            sold_half_sol = (trade["invested_sol"] / 2.0) * (current_mc / trade["entry_mc"])
            updates["sold_half"] = 1
            updates["sold_half_sol"] = sold_half_sol
            trade["sold_half"] = 1
            trade["sold_half_sol"] = sold_half_sol
            
        if is_final:
            rem_sol = (trade["invested_sol"] / 2.0) if trade["sold_half"] else trade["invested_sol"]
            exit_sol = rem_sol * (current_mc / trade["entry_mc"])
            
            total_returned = trade.get("sold_half_sol", 0) + updates.get("sold_half_sol", 0) + exit_sol
            updates["exit_mc"] = current_mc
            updates["exit_sol"] = exit_sol
            updates["profit_sol"] = total_returned - trade["invested_sol"]
            updates["status"] = 'CLOSED'
            
        if updates:
            set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
            values = list(updates.values()) + [mint]
            conn.execute(f"UPDATE paper_trades SET {set_clause} WHERE mint = ?", values)
            conn.commit()
    finally:
        conn.close()

def get_paper_stats():
    conn = get_connection()
    try:
        # Ensure table exists first in case it's called early
        try:
            total = conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0]
        except sqlite3.OperationalError:
            return {"total_trades": 0, "open_trades": 0, "closed_trades": 0, "net_profit_sol": 0.0}
            
        open_trades = conn.execute("SELECT COUNT(*) FROM paper_trades WHERE status = 'OPEN'").fetchone()[0]
        closed_trades = conn.execute("SELECT COUNT(*) FROM paper_trades WHERE status = 'CLOSED'").fetchone()[0]
        profit = conn.execute("SELECT SUM(profit_sol) FROM paper_trades WHERE status = 'CLOSED'").fetchone()[0] or 0.0
        return {
            "total_trades": total,
            "open_trades": open_trades,
            "closed_trades": closed_trades,
            "net_profit_sol": round(profit, 4)
        }
    finally:
        conn.close()

def backfill_paper_trades():
    init_paper_db()
    conn = get_connection()
    rows = conn.execute("SELECT mint, initial_mc, peak_mc, mc_24hr FROM coins WHERE score = 100 AND initial_mc > 0").fetchall()
    for r in rows:
        mint, initial_mc, peak_mc, mc_24hr = r
        if mc_24hr is None: 
            # If tracking isn't done, skip backfill for this coin (it will be caught live if we just let it run)
            continue
        
        open_paper_trade(mint, initial_mc, 0.02)
        
        if peak_mc and peak_mc >= initial_mc * 2:
            update_paper_trade(mint, initial_mc * 2, is_final=False)
            
        update_paper_trade(mint, mc_24hr, is_final=True)
        
    conn.close()

if __name__ == "__main__":
    print("Running paper trade backfill...")
    backfill_paper_trades()
    print("Done!", get_paper_stats())
