import sqlite3
import json
import os
import time
from typing import Dict, Any, List, Optional

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "coins.db")


def get_connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS coins (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            mint            TEXT    UNIQUE NOT NULL,
            name            TEXT,
            symbol          TEXT,
            image_uri       TEXT,
            description     TEXT,
            twitter_url     TEXT,
            telegram_url    TEXT,
            website_url     TEXT,
            has_twitter     INTEGER DEFAULT 0,
            has_telegram    INTEGER DEFAULT 0,
            has_website     INTEGER DEFAULT 0,
            reply_count     INTEGER DEFAULT 0,
            created_at      REAL,
            detected_at     REAL    NOT NULL,
            initial_mc      REAL    DEFAULT 0,
            score           INTEGER DEFAULT 0,
            score_breakdown TEXT,
            mc_1min         REAL,
            mc_5min         REAL,
            mc_15min        REAL,
            mc_1hr          REAL,
            mc_24hr         REAL,
            peak_mc         REAL,
            last_tracked_at REAL,
            tracking_done   INTEGER DEFAULT 0
        )
    """)
    # Add mc_1min column to existing databases that don't have it yet
    try:
        conn.execute("ALTER TABLE coins ADD COLUMN mc_1min REAL")
    except Exception:
        pass  # column already exists
    conn.commit()
    conn.close()


def save_coin(coin: Dict[str, Any]) -> bool:
    """Insert a new coin. Returns True if inserted, False if already exists."""
    conn = get_connection()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO coins
               (mint, name, symbol, image_uri, description, twitter_url, telegram_url, website_url,
                has_twitter, has_telegram, has_website, reply_count,
                created_at, detected_at, initial_mc, score, score_breakdown)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                coin["mint"], coin["name"], coin["symbol"],
                coin.get("image_uri", ""), coin.get("description", ""),
                coin.get("twitter_url", ""), coin.get("telegram_url", ""), coin.get("website_url", ""),
                int(coin.get("has_twitter", False)), int(coin.get("has_telegram", False)),
                int(coin.get("has_website", False)), coin.get("reply_count", 0),
                coin.get("created_at"), coin["detected_at"],
                coin.get("initial_mc", 0), coin["score"],
                json.dumps(coin.get("score_breakdown", {})),
            ),
        )
        inserted = conn.execute("SELECT changes()").fetchone()[0] > 0
        conn.commit()
        return inserted
    except Exception as e:
        print(f"[DB] save_coin error: {e}")
        return False
    finally:
        conn.close()


def update_mc_snapshot(mint: str, field: str, value: float):
    conn = get_connection()
    try:
        conn.execute(f"UPDATE coins SET {field} = ? WHERE mint = ? AND {field} IS NULL", (value, mint))
        conn.execute(
            "UPDATE coins SET peak_mc = MAX(COALESCE(peak_mc, 0), ?), last_tracked_at = ? WHERE mint = ?",
            (value, time.time(), mint),
        )
        conn.commit()
    finally:
        conn.close()


def mark_tracking_done(mint: str):
    conn = get_connection()
    try:
        conn.execute("UPDATE coins SET tracking_done = 1 WHERE mint = ?", (mint,))
        conn.commit()
    finally:
        conn.close()


def get_pending_tracking() -> List[Dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT mint, detected_at, initial_mc, mc_1min, mc_5min, mc_15min, mc_1hr, mc_24hr "
            "FROM coins WHERE tracking_done = 0 ORDER BY detected_at DESC LIMIT 200"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_recent_coins(limit: int = 100, offset: int = 0) -> List[Dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM coins ORDER BY detected_at DESC LIMIT ? OFFSET ?", (limit, offset)
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("score_breakdown"):
                try:
                    d["score_breakdown"] = json.loads(d["score_breakdown"])
                except Exception:
                    pass
            result.append(d)
        return result
    finally:
        conn.close()


def get_biggest_winners(limit: int = 50) -> List[Dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT * FROM coins 
               WHERE peak_mc IS NOT NULL AND initial_mc > 0 
               ORDER BY (peak_mc / initial_mc) DESC 
               LIMIT ?""",
            (limit,)
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("score_breakdown"):
                try:
                    d["score_breakdown"] = json.loads(d["score_breakdown"])
                except Exception:
                    pass
            result.append(d)
        return result
    finally:
        conn.close()


def get_stats() -> Dict:
    conn = get_connection()
    try:
        total = conn.execute("SELECT COUNT(*) FROM coins").fetchone()[0]
        today_ts = time.time() - 86400
        today = conn.execute("SELECT COUNT(*) FROM coins WHERE detected_at >= ?", (today_ts,)).fetchone()[0]

        brackets = []
        for lo, hi, label in [(70, 101, "70–100 🔥"), (50, 70, "50–69 ⚡"), (0, 50, "0–49 🔻")]:
            row = conn.execute(
                """SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN peak_mc >= initial_mc * 2 AND initial_mc > 0 THEN 1 ELSE 0 END) as wins_2x,
                    SUM(CASE WHEN peak_mc >= initial_mc * 3 AND initial_mc > 0 THEN 1 ELSE 0 END) as wins_3x,
                    AVG(CASE WHEN peak_mc IS NOT NULL AND initial_mc > 0
                        THEN CAST(peak_mc AS REAL) / initial_mc ELSE NULL END) as avg_mult
                   FROM coins WHERE score >= ? AND score < ? AND peak_mc IS NOT NULL""",
                (lo, hi),
            ).fetchone()
            brackets.append({
                "label": label, "total": row[0],
                "wins_2x": row[1] or 0, "wins_3x": row[2] or 0,
                "avg_mult": round(row[3], 2) if row[3] else None,
            })

        return {"total": total, "today": today, "brackets": brackets}
    finally:
        conn.close()
