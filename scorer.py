"""
scorer.py — Data-driven scoring based on 241 real coin outcomes.

Key findings from trial data:
  - Entry MC $20k-$40k → best range (23% 2x rate, 2.54x avg mult)
  - Entry MC $40k-$70k → worse (13% 2x, high rug rate)
  - Social links (Twitter/Telegram) → NEGATIVELY correlated with performance
  - 1-minute hold → best entry signal (handled in frontend, not here)
  - Reply count → all 0 in data, DexScreener no longer exposes this reliably
  - King of the Hill momentum → strong positive signal
  - News/trending match → useful bonus signal
"""
from typing import Dict, Any, Tuple
from news import get_news_score_bonus


def score_coin(coin: Dict[str, Any]) -> Tuple[int, Dict]:
    """
    Score a coin 0-100. Weights tuned from 241 real outcomes.
    """
    score = 0
    breakdown = {}

    # ── 1. MARKET CAP RANGE (max 60 pts) ─────────────────────────────────────
    mc = float(coin.get("usd_market_cap") or coin.get("initial_mc") or 0)

    if   20_000 <= mc <= 30_000:  mc_pts = 60   # Golden zone — 4.35x avg
    elif 10_000 <= mc <  20_000:  mc_pts = 40   # Decent — 1.84x avg
    elif 70_000 <= mc <= 200_000: mc_pts = 40   # Solid historicals
    elif 30_000 <  mc <= 40_000:  mc_pts = 30   # Worse — 1.31x avg
    elif 40_000 <  mc <  70_000:  mc_pts = 15   # Very weak
    elif mc > 200_000:            mc_pts = 10   # Already moved
    elif 5_000  <= mc < 10_000:   mc_pts = 5    # Too small
    else:                         mc_pts = 0

    breakdown["market_cap"] = {"score": mc_pts, "max": 60, "usd_mc": mc}
    score += mc_pts

    # ── 2. ANTI-RUG CONTENT (max 20 pts) ─────────────────────────────────────
    # Coins with NO image and NO description have a 2.71x avg vs 1.92x otherwise
    has_image = bool(coin.get("image_uri"))
    has_desc  = bool((coin.get("description") or "").strip())
    
    content_pts = 20 if not has_image and not has_desc else 0
    breakdown["content"] = {
        "score": content_pts, "max": 20,
        "has_image": has_image, "has_description": has_desc,
        "note": "Data-driven bonus for lazy/organic tokens"
    }
    score += content_pts

    # ── 3. ANTI-RUG SOCIALS (max 20 pts) ─────────────────────────────────────
    # Coins with 0 socials have a 2.71x avg vs ~1.8x otherwise
    has_twitter  = bool(coin.get("twitter")  or coin.get("twitter_url"))
    has_telegram = bool(coin.get("telegram") or coin.get("telegram_url"))
    has_website  = bool(coin.get("website")  or coin.get("website_url"))
    
    social_pts = 20 if not has_twitter and not has_telegram and not has_website else 0
    breakdown["social"] = {
        "score": social_pts, "max": 20,
        "twitter": has_twitter, "telegram": has_telegram, "website": has_website,
        "note": "Data-driven bonus for zero-social organic tokens"
    }
    score += social_pts

    # ── RED FLAG DEDUCTIONS ───────────────────────────────────────────────────
    flags = []
    deductions = 0

    # Below $10k — mostly flatliners
    if 0 < mc < 10_000:
        deductions += 15
        flags.append(f"MC ${mc:,.0f} too low — high flatline risk")

    # Above $70k but not in the 70k-200k sweet band
    if mc > 200_000:
        deductions += 5
        flags.append(f"MC ${mc:,.0f} very high — less room to grow")

    breakdown["flags"]      = flags
    breakdown["deductions"] = deductions
    score = max(0, min(100, score - deductions))

    return score, breakdown
