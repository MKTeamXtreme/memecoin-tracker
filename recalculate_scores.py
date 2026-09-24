import sqlite3
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scorer import score_coin

def run():
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'coins.db')
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    coins = cursor.execute("SELECT * FROM coins").fetchall()
    
    for c in coins:
        c_dict = dict(c)
        raw = {
            "usd_market_cap": c_dict["initial_mc"],
            "symbol": c_dict["symbol"],
            "name": c_dict["name"],
            "image_uri": c_dict["image_uri"],
            "description": c_dict["description"],
            "twitter": c_dict["has_twitter"],
            "telegram": c_dict["has_telegram"],
            "website": c_dict["has_website"]
        }
        score, breakdown = score_coin(raw)
        cursor.execute("UPDATE coins SET score = ?, score_breakdown = ? WHERE mint = ?", 
                       (score, json.dumps(breakdown), c_dict["mint"]))
    
    conn.commit()
    conn.close()
    print(f"Recalculated scores for {len(coins)} coins.")

if __name__ == "__main__":
    run()
