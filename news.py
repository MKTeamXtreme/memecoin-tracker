"""
news.py — Free crypto news & trending data sources.
- CryptoPanic public feed (no API key needed for public posts)
- CoinGecko trending tokens (completely free, no key)

Used to boost scores for coins whose symbol/name matches current hype.
"""
import asyncio
import aiohttp
import time
from typing import Set

# Cache so we don't hammer APIs on every poll
_trending_symbols: Set[str] = set()
_news_keywords = {}
_last_refresh = 0
REFRESH_INTERVAL = 30  # refresh every 30 seconds

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}


async def _fetch_coingecko_trending(session: aiohttp.ClientSession) -> Set[str]:
    """Top trending coins on CoinGecko right now."""
    try:
        url = "https://api.coingecko.com/api/v3/search/trending"
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return set()
            data = await r.json(content_type=None)
            symbols = set()
            for item in data.get("coins", []):
                coin = item.get("item", {})
                sym  = (coin.get("symbol") or "").upper()
                name = (coin.get("name")   or "").upper()
                if sym:  symbols.add(sym)
                if name: symbols.add(name)
            return symbols
    except Exception:
        return set()


async def _fetch_rss_keywords(session: aiohttp.ClientSession) -> dict:
    """Pull hot keywords from free RSS feeds."""
    feeds = [
        "https://cointelegraph.com/rss",
        "https://coindesk.com/arc/outboundfeeds/rss/",
        # Global macro/politics news
        "https://news.google.com/rss",
        "https://news.google.com/rss/headlines/section/topic/POLITICS",
        "https://news.google.com/rss/headlines/section/topic/BUSINESS",
        "https://news.google.com/rss/headlines/section/topic/TECHNOLOGY"
    ]
    words = set()
    for url in feeds:
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200:
                    continue
                resp_text = await r.text()
                import re
                titles = re.findall(r"<title>(.*?)</title>", resp_text, re.DOTALL)
                for title in titles[:30]:
                    for w in title.upper().split():
                        w = re.sub(r"[^A-Z0-9]", "", w)
                        if 4 <= len(w) <= 12 and w.isalpha():
                            if w not in ["THIS", "THAT", "WITH", "FROM", "THEIR", "ABOUT", "WHAT", "NEWS", "LATEST", "GOOGLE", "YOUR", "HAVE", "BEEN", "WILL"]:
                                words.add(w)
        except Exception:
            continue
    
    
        
    return words


async def refresh_trends():
    """Pull fresh trending data from all sources. Call this every 5 minutes."""
    global _trending_symbols, _news_keywords, _last_refresh

    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        trending, news = await asyncio.gather(
            _fetch_coingecko_trending(session),
            _fetch_rss_keywords(session),
        )

    _trending_symbols = trending
    _news_keywords    = news
    _last_refresh     = time.time()

    total = len(trending | news)
    if total:
        print(f"[News] Refreshed — {len(trending)} CoinGecko trending, {len(news)} CryptoPanic keywords")
    else:
        print("[News] Refresh returned no data (APIs may be rate-limited)")


def get_news_score_bonus(symbol: str, name: str) -> tuple[int, list[str]]:
    """
    Return (bonus_points, reasons) for a coin based on current trending data.
    Call this from scorer.py.
    """
    sym   = (symbol or "").upper().strip("$")
    name_up = (name or "").upper()

    bonus   = 0
    reasons = []

    # CoinGecko trending match
    if sym in _trending_symbols or name_up in _trending_symbols:
        bonus += 15
        reasons.append(f"📈 Trending on CoinGecko")

    # CryptoPanic news mention
    if sym in _news_keywords or any(w in name_up for w in _news_keywords if len(w) >= 3):
        bonus += 10
        reasons.append(f"📰 Mentioned in crypto news")

    return bonus, reasons


def is_stale() -> bool:
    return time.time() - _last_refresh > REFRESH_INTERVAL
