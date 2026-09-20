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

    # ── 1. MARKET CAP RANGE (max 50 pts) — strongest predictor ───────────────
    # $20k-$30k is definitively the golden zone from the data (27% 2x rate, 7.24x avg)
    # $30k-$40k underperforms compared to the 20k-30k bracket
    mc = float(coin.get("usd_market_cap") or coin.get("initial_mc") or 0)

    if   20_000 <= mc <= 30_000:  mc_pts = 50   # Golden zone — 27% 2x, 7.24x avg
    elif 30_000 <  mc <= 40_000:  mc_pts = 35   # Okay, but average returns are much lower (1.27x)
    elif 10_000 <= mc <  20_000:  mc_pts = 30   # Decent — 19% 2x, low rug rate
    elif 70_000 <= mc <= 200_000: mc_pts = 35   # Small sample but good historicals
    elif 40_000 <  mc <  70_000:  mc_pts = 18   # Worse — high rug rate
    elif mc > 200_000:            mc_pts = 10   # Already moved, less upside
    elif 5_000  <= mc < 10_000:   mc_pts = 8    # Too small, mostly flatlines
    else:                         mc_pts = 0

    breakdown["market_cap"] = {"score": mc_pts, "max": 50, "usd_mc": mc}
    score += mc_pts

    # ── 2. MOMENTUM SIGNALS (max 30 pts) ─────────────────────────────────────
    # King of the Hill = already proven to attract buyers
    is_koth = bool(coin.get("king_of_the_hill_timestamp"))
    is_live  = bool(coin.get("is_currently_live"))
    momentum = (25 if is_koth else 0) + (5 if is_live else 0)
    breakdown["momentum"] = {
        "score": momentum, "max": 30,
        "king_of_the_hill": is_koth, "dev_streaming": is_live,
    }
    score += momentum

    # ── 3. NEWS & TRENDING BONUS (max 15 pts) ─────────────────────────────────
    sym  = coin.get("symbol", "")
    name = coin.get("name", "")
    news_bonus, news_reasons = get_news_score_bonus(sym, name)
    news_bonus = min(news_bonus, 15)
    breakdown["news_trending"] = {
        "score": news_bonus, "max": 15,
        "reasons": news_reasons,
    }
    score += news_bonus

    # ── 4. CONTENT QUALITY (max 8 pts) ───────────────────────────────────────
    has_image = bool(coin.get("image_uri"))
    has_desc  = bool((coin.get("description") or "").strip())
    content = (5 if has_image else 0) + (3 if has_desc else 0)
    breakdown["content"] = {
        "score": content, "max": 8,
        "has_image": has_image, "has_description": has_desc,
    }
    score += content

    # ── 5. SOCIAL PRESENCE (max 5 pts) — data shows NEGATIVE correlation ──────
    # Slashed from 15pts to 5pts — coins with no socials outperform those with them
    # Keeping a tiny bonus since having socials = at least some dev effort
    has_twitter  = bool(coin.get("twitter")  or coin.get("twitter_url"))
    has_telegram = bool(coin.get("telegram") or coin.get("telegram_url"))
    has_website  = bool(coin.get("website")  or coin.get("website_url"))
    social = (2 if has_twitter else 0) + (2 if has_telegram else 0) + (1 if has_website else 0)
    breakdown["social"] = {
        "score": social, "max": 5,
        "twitter": has_twitter, "telegram": has_telegram, "website": has_website,
        "note": "Low weight — data shows no-social coins outperform",
    }
    score += social

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

    # No image at all — laziest possible token
    if not has_image:
        deductions += 5
        flags.append("No token image — minimal effort token")

    breakdown["flags"]      = flags
    breakdown["deductions"] = deductions
    score = max(0, min(100, score - deductions))

    return score, breakdown
