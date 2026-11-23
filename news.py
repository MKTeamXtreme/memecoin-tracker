"""
news.py — Sub-second Breaking Catalyst & Trending Engine.
  1. Connects live to TreeNews WebSocket (wss://news.treeofalpha.com/ws — free, <1s latency)
     aggregating live X/Twitter crypto wires, OpenAI, Bloomberg, Reuters, Binance, SEC, etc.
  2. Extracts ONLY novel, rare proper nouns & 2-word breaking phrases (zero generic English words)
     and arms a 15-minute live catalyst trigger with whole-word boundary matching.
  3. Also refreshes CoinGecko top trending symbols every 5 minutes as a secondary signal.
"""
import asyncio
import json
import re
import time
from typing import Dict, List, Optional, Set, Tuple

import aiohttp

TREENEWS_WS_URL   = "wss://news.treeofalpha.com/ws"
TREENEWS_REST_URL = "https://news.treeofalpha.com/api/news?limit=100"
COINGECKO_URL     = "https://api.coingecko.com/api/v3/search/trending"

BREAKING_TTL_SECS = 15 * 60   # Breaking catalyst stays armed for 15 minutes
REFRESH_INTERVAL  = 300       # CoinGecko + REST sync every 5 minutes

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# Comprehensive stopword & evergreen crypto/news word list (NEVER arm as breaking catalysts)
STOPWORDS: Set[str] = {
    # Common English words
    "ABOUT", "ABOVE", "ACROSS", "AFTER", "AGAIN", "AGAINST", "AHEAD", "ALMOST", "ALONE", "ALONG",
    "ALREADY", "ALSO", "ALTHOUGH", "ALWAYS", "AMONG", "ANOTHER", "ANYONE", "ANYTHING", "AROUND",
    "ASKING", "BACK", "BECAUSE", "BECOME", "BEFORE", "BEGAN", "BEGIN", "BEHIND", "BEING", "BELOW",
    "BETWEEN", "BEYOND", "BIGGER", "BILLION", "BOTH", "BRING", "BUILD", "CALL", "CALLED", "CAME",
    "CANNOT", "CASE", "CHANGE", "CHECK", "CLEAR", "CLOSE", "COME", "COMES", "COMING", "COMPANY",
    "COMPANIES", "CONTROL", "COULD", "COUNTRY", "COURSE", "CREATE", "DAILY", "DATA", "DAYS", "DEAL",
    "DOES", "DOING", "DOLLAR", "DOLLARS", "DONE", "DOWN", "DURING", "EACH", "EARLY", "ELSE", "END",
    "ENDS", "EVEN", "EVER", "EVERY", "EXPERTS", "FACES", "FACT", "FALL", "FAST", "FEEL", "FEWER",
    "FILE", "FIND", "FIRST", "FIVE", "FOLLOW", "FORM", "FOUND", "FOUR", "FREE", "FROM", "FULL",
    "FUTURE", "GAIN", "GAINS", "GAME", "GAMES", "GAVE", "GENERAL", "GETS", "GIVEN", "GIVES",
    "GLOBAL", "GOING", "GOOD", "GREAT", "GROUP", "GROW", "GROWTH", "HALF", "HAND", "HARD", "HAVE",
    "HEAD", "HEAR", "HELP", "HELPS", "HERE", "HIGH", "HIGHER", "HOLD", "HOME", "HOPE", "HOUR",
    "HOURS", "HOUSE", "HUGE", "HUMAN", "HUMANS", "IDEA", "IMPACT", "INCLUDING", "INDEX", "INFO",
    "INSIDE", "INTO", "INTRODUCE", "INTRODUCED", "ISSUE", "ISSUES", "ITEM", "JOIN", "JUST", "KEEP",
    "KEYS", "KIND", "KNOW", "KNOWN", "LARGE", "LAST", "LATE", "LATER", "LATEST", "LAUNCH",
    "LAUNCHES", "LAUNCHED", "LEAD", "LEADER", "LEADING", "LEAST", "LEAVE", "LEFT", "LESS", "LEVEL",
    "LIFE", "LIKE", "LINE", "LINK", "LINKED", "LIST", "LITTLE", "LIVE", "LOCAL", "LONG", "LOOK",
    "LOOKS", "LOSE", "LOSES", "LOSS", "LOST", "LOTS", "LOWER", "MADE", "MAIN", "MAJOR", "MAKE",
    "MAKES", "MAKING", "MANY", "MARK", "MARKET", "MARKETS", "MASS", "MATH", "MATTER", "MEAN",
    "MEANS", "MEDIA", "MEET", "MEMBER", "MIGHT", "MILLION", "MILLIONS", "MIND", "MINE", "MINUTES",
    "MISS", "MODE", "MODEL", "MODELS", "MONEY", "MONTH", "MONTHS", "MORE", "MOST", "MOVE", "MOVES",
    "MUCH", "MUST", "NAME", "NAMED", "NATION", "NATIVE", "NEAR", "NEED", "NEEDS", "NETWORK", "NEVER",
    "NEWS", "NEXT", "NICE", "NINE", "NONE", "NOTE", "NOTHING", "NUMBER", "OFFER", "OFFICE",
    "OFFICIAL", "OFTEN", "ONCE", "ONLY", "OPEN", "ORDER", "OTHER", "OTHERS", "OURS", "OUTSIDE",
    "OVER", "OWNED", "OWNER", "PAGE", "PAID", "PAIR", "PART", "PARTS", "PASS", "PAST", "PASTE",
    "PAYS", "PEOPLE", "PERCENT", "PERIOD", "PERSON", "PHONE", "PICK", "PLACE", "PLAN", "PLANS",
    "PLAY", "PLAYER", "PLUS", "POINT", "POINTS", "POLICY", "POST", "POWER", "PRESS", "PRICE",
    "PRICES", "PRIVATE", "PROBLEM", "PRODUCT", "PROFIT", "PROGRAM", "PROJECT", "PROOF", "PROTECT",
    "PROTOCOL", "PUBLIC", "PULL", "PUSH", "PUSHBACK", "PUTS", "QUICK", "QUITE", "RAISE", "RATE",
    "RATES", "REACH", "READ", "READY", "REAL", "REALLY", "REASON", "RECENT", "RECORD", "RECOVER",
    "RELEASE", "REMAIN", "REPORT", "REPORTS", "RESEARCH", "RESULT", "RETAIL", "RETAILER", "RETURN",
    "REVEAL", "REVEALED", "RIGHT", "RISE", "RISES", "RISING", "RISK", "ROAD", "ROLE", "ROOM",
    "ROUND", "RULE", "RULES", "RUNS", "SAFE", "SAID", "SALE", "SALES", "SAME", "SAVE", "SAYS",
    "SCALE", "SCALES", "SECOND", "SECONDS", "SECTOR", "SECURED", "SECURES", "SECURITY", "SEEING",
    "SEEK", "SEEKS", "SEEM", "SEEN", "SELL", "SEND", "SENSE", "SENT", "SERIES", "SERVE", "SERVICE",
    "SERVICES", "SESSION", "SETS", "SEVEN", "SEVERAL", "SHARE", "SHARES", "SHIFT", "SHORT", "SHOT",
    "SHOULD", "SHOW", "SHOWS", "SIDE", "SIGN", "SIGNAL", "SIGNS", "SIMPLE", "SINCE", "SINGLE",
    "SITE", "SITES", "SIZE", "SMALL", "SMART", "SOCIAL", "SOME", "SOON", "SORT", "SOURCE", "SPACE",
    "SPEAK", "SPECIAL", "SPEED", "SPEND", "SPOT", "SPREAD", "STAGE", "STAKE", "STAND", "STANDARD",
    "START", "STARTS", "STATE", "STATES", "STATUS", "STAY", "STEP", "STEPS", "STILL", "STOCK",
    "STOCKS", "STOLEN", "STOP", "STORE", "STORY", "STRATEGY", "STREAM", "STREET", "STRONG", "STUDY",
    "STUFF", "STYLE", "SUCH", "SUDDEN", "SUPER", "SUPPLIER", "SUPPLY", "SUPPORT", "SUPPORTS", "SURE",
    "SURGE", "SWAP", "SWAPS", "SYSTEM", "TABLE", "TAKE", "TAKES", "TAKING", "TALK", "TARGET",
    "TEAM", "TECH", "TELL", "TERM", "TERMS", "TEST", "TEXT", "THAN", "THANK", "THAT", "THEIR",
    "THEM", "THEN", "THERE", "THESE", "THEY", "THING", "THINGS", "THINK", "THIRD", "THIS", "THOSE",
    "THOUGH", "THREE", "THROUGH", "TIME", "TIMES", "TODAY", "TOGETHER", "TOLD", "TONIGHT", "TOOK",
    "TOOL", "TOOLS", "TOTAL", "TOUCH", "TOWARD", "TRACK", "TRADE", "TRADER", "TRADERS", "TRADES",
    "TRADING", "TREND", "TRIAL", "TRUE", "TRUST", "TRUTH", "TRYING", "TURN", "TURNING", "TURNS",
    "TWEET", "TWICE", "TYPE", "UNDER", "UNIT", "UNITED", "UNITS", "UNTIL", "UPDATE", "UPDATES",
    "UPGRADE", "UPON", "USED", "USER", "USERS", "USING", "USUAL", "VALUE", "VALUES", "VERSION",
    "VERY", "VIDEO", "VIEW", "VIEWS", "VIRAL", "VISIT", "VOICE", "VOLUME", "VOTE", "WAIT", "WALL",
    "WANT", "WANTS", "WARN", "WARNING", "WATCH", "WATER", "WAVE", "WAYS", "WEAK", "WEEK", "WEEKS",
    "WELL", "WENT", "WERE", "WEST", "WHAT", "WHEN", "WHERE", "WHICH", "WHILE", "WHITE", "WHOLE",
    "WHOM", "WHOSE", "WIDE", "WILL", "WINS", "WITH", "WITHIN", "WITHOUT", "WORD", "WORDS", "WORK",
    "WORKING", "WORKS", "WORLD", "WORTH", "WOULD", "WRITE", "WRONG", "YEAR", "YEARS", "YORK",
    "YOUNG", "YOUR", "ZERO", "ZONE", "TEMPORARY", "DISPUTE", "AUTOMATE", "CONFERENCE", "FLOOR",
    "BEST", "PRACTICES", "ETIQUETTE", "SELECT", "COMMITTEE", "TREASURY", "COMMONS", "GARDEN",
    "PREACH", "GOSPEL", "EXPERT", "APPEAR", "ASSETS", "ASSET", "BRIDGING", "CROSS", "CHAIN",
    "BUSINESS", "BUSINESSES", "PAY", "YOU", "TOP", "NEW", "OLD", "BIG", "ALL", "OUT", "NOW", "HOW",
    "WHY", "WHO", "HAS", "ARE", "WAS", "FOR", "NOT", "BUT", "CAN", "GET", "SET", "RUN", "BUY",
    "GAMBLING", "DISPUTES", "LAWSUIT", "COURT", "JUDGE", "RULING", "GOVERNOR", "PRESIDENT",
    # Evergreen crypto/finance/news brands & words (too common on Pump.fun to be a breaking catalyst alone)
    "CRYPTO", "BITCOIN", "ETHEREUM", "SOLANA", "TETHER", "STABLECOIN", "BLOCKCHAIN", "TOKEN",
    "TOKENS", "COIN", "COINS", "MEME", "MEMECOIN", "DEFI", "WEB3", "WALLET", "WALLETS", "MINT",
    "MINTS", "EXCHANGE", "BINANCE", "COINBASE", "KRAKEN", "BYBIT", "OKX", "UNISWAP", "RAYDIUM",
    "JUPITER", "PUMPFUN", "PUMP", "DEXSCREENER", "COINGECKO", "COINTELEGRAPH", "COINDESK",
    "DECRYPT", "BLOOMBERG", "REUTERS", "UNCHAINED", "THEBLOCK", "BLOCK", "CNBC", "FORBES", "FOX",
    "TWITTER", "TELEGRAM", "DISCORD", "REDDIT", "YOUTUBE", "TIKTOK", "GOOGLE", "APPLE", "META",
    "AMAZON", "MICROSOFT", "TESLA", "NVIDIA", "OPENAI", "ANTHROPIC", "CLAUDE", "CHATGPT", "GROK",
    "ELON", "MUSK", "TRUMP", "BIDEN", "DONALD", "VITALIK", "SATOSHI", "NAKAMOTO", "GRAYSCALE",
    "BLACKROCK", "FIDELITY", "VANGUARD", "JPMORGAN", "GOLDMAN", "BANK", "BANKS", "CASH", "GOLD",
    "SILVER", "OIL", "HACK", "HACKER", "HACKERS", "HEIST", "SCAM", "RUG", "BULL", "BEAR", "MOON",
    "KING", "DOGE", "PEPE", "SHIB", "BONK", "WIF", "BABY", "MINI", "CAT", "DOG", "FROG", "AGENT",
    "AGENTS", "BOTS", "ACCOUNTS", "ACCOUNT", "SENATE", "SENATOR", "CONGRESS", "FEDERAL", "RESERVE",
    "SEC", "CFTC", "ETF", "ETFS", "IPO", "CEO", "CFO", "CTO", "AI", "USD", "USDT", "USDC", "BTC",
    "ETH", "SOL", "XRP", "BNB", "ADA", "AVAX", "LINK", "SUI", "TON", "APT", "ARB", "OP", "BASE",
    "BLOGS", "PART", "WEEKEND", "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY",
    "SUNDAY", "JANUARY", "FEBRUARY", "MARCH", "APRIL", "JUNE", "JULY", "AUGUST", "SEPTEMBER",
    "OCTOBER", "NOVEMBER", "DECEMBER",
}

# ── State ─────────────────────────────────────────────────────────────────────
_trending_symbols: Set[str] = set()
# Active breaking catalysts: term (UPPER) -> {"headline": str, "source": str, "ts": float}
_breaking_catalysts: Dict[str, dict] = {}
# Baseline terms seen in older news so stale/recurring words don't re-trigger
_baseline_terms: Set[str] = set()
_seen_news_ids: Set[str] = set()
_last_refresh: float = 0.0
_news_keywords: Set[str] = set()  # Backwards-compat export for main.py


def _clean_headline(raw_title: str) -> str:
    """Strip source prefixes like 'DECRYPT:', 'The Block (@TheBlockCo):', URLs, and @handles."""
    txt = (raw_title or "").strip()
    # Strip leading 'SOURCE: ' or 'Name (@handle): ' prefixes (up to 2 nested prefixes)
    for _ in range(2):
        txt = re.sub(r"^[A-Za-z0-9 ._@-]{2,42}\s*(\(@[A-Za-z0-9_]+\))?\s*:\s*", "", txt)
    txt = re.sub(r"https?://\S+", " ", txt)
    txt = re.sub(r"@[A-Za-z0-9_]+", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def extract_catalyst_terms(raw_title: str, raw_body: str = "") -> List[str]:
    """
    Extract ONLY distinctive, rare proper nouns & 2-word Title-Case phrases
    from a breaking headline. Returns uppercase terms (zero generic English words).
    """
    clean = _clean_headline(raw_title)
    if not clean and raw_body:
        clean = _clean_headline(raw_body[:160])
    if not clean:
        return []

    terms: List[str] = []
    seen_local: Set[str] = set()

    # 1. Explicit $CASHTAGS in tweets/headlines (e.g. $SHIRUDO, $KALSHI)
    for m in re.findall(r"\$([A-Za-z]{3,12})\b", raw_title or ""):
        u = m.upper()
        if u not in STOPWORDS and u not in seen_local:
            seen_local.add(u)
            terms.append(u)

    # 2. Split words preserving original casing to detect proper nouns (Capitalized mid-sentence or ALLCAPS)
    tokens = re.findall(r"[A-Za-z][A-Za-z0-9'-]{1,18}", clean)
    for idx, tok in enumerate(tokens):
        alpha = re.sub(r"[^A-Za-z0-9]", "", tok)
        u = alpha.upper()
        if len(u) < 4 or len(u) > 15 or not u.isalpha():
            continue
        if u in STOPWORDS:
            continue
        # Require capitalized word (proper noun / brand / novel product name) or unusual ALLCAPS
        is_cap = tok[0].isupper()
        if is_cap and u not in seen_local:
            seen_local.add(u)
            terms.append(u)

    # 3. 2-word proper noun phrases where BOTH words are genuine non-stopwords (e.g. "CYNTHIA LUMMIS")
    for i in range(len(tokens) - 1):
        t1, t2 = tokens[i], tokens[i + 1]
        u1 = re.sub(r"[^A-Za-z]", "", t1).upper()
        u2 = re.sub(r"[^A-Za-z]", "", t2).upper()
        if (
            t1[0].isupper() and t2[0].isupper()
            and 4 <= len(u1) <= 12 and 4 <= len(u2) <= 12
            and u1 not in STOPWORDS and u2 not in STOPWORDS
        ):
            phrase = f"{u1} {u2}"
            if phrase not in seen_local:
                seen_local.add(phrase)
                terms.append(phrase)

    return terms


def _prune_expired_catalysts(now: Optional[float] = None):
    now = now or time.time()
    expired = [k for k, v in _breaking_catalysts.items() if (now - v["ts"]) > BREAKING_TTL_SECS]
    for k in expired:
        _breaking_catalysts.pop(k, None)
    _news_keywords.clear()
    _news_keywords.update(_breaking_catalysts.keys())


def _ingest_news_item(item: dict, is_seed: bool = False):
    """Process one TreeNews item (either historical seed or live WebSocket breaking alert)."""
    if not isinstance(item, dict):
        return
    nid = str(item.get("_id") or item.get("url") or item.get("title") or "")
    if nid and nid in _seen_news_ids:
        return
    if nid:
        _seen_news_ids.add(nid)
        if len(_seen_news_ids) > 2000:
            _seen_news_ids.clear()

    title = item.get("title") or item.get("body") or ""
    body  = item.get("body") or ""
    source = item.get("sourceName") or item.get("source") or "TreeNews"

    raw_ts = item.get("time") or 0
    item_ts = (float(raw_ts) / 1000.0) if raw_ts > 1e12 else (float(raw_ts) if raw_ts > 1e9 else time.time())
    now = time.time()
    age_secs = now - item_ts

    terms = extract_catalyst_terms(title, body)
    if not terms:
        return

    # Older seed items (> 15m old) populate baseline only so stale stories don't fire
    if is_seed and age_secs > BREAKING_TTL_SECS:
        for t in terms:
            _baseline_terms.add(t)
        return

    # Only arm truly fresh breaking items (< 15m old)
    if age_secs > BREAKING_TTL_SECS:
        return

    armed = []
    clean_hl = _clean_headline(title)[:110]
    for t in terms:
        # Skip terms that already appeared heavily in baseline older stories (unless it's a 2-word phrase)
        if not is_seed and t in _baseline_terms and " " not in t:
            continue
        _breaking_catalysts[t] = {
            "term": t,
            "headline": clean_hl,
            "source": str(source),
            "ts": item_ts,
        }
        armed.append(t)

    _prune_expired_catalysts(now)
    if armed and not is_seed:
        print(f"[BreakingNews] ARMED {armed[:5]} | {source}: {clean_hl}")


async def _fetch_coingecko_trending(session: aiohttp.ClientSession) -> Set[str]:
    """Top trending coins on CoinGecko right now."""
    try:
        async with session.get(COINGECKO_URL, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return set()
            data = await r.json(content_type=None)
            symbols = set()
            for item in data.get("coins", []):
                coin = item.get("item", {})
                sym  = (coin.get("symbol") or "").upper()
                name = (coin.get("name") or "").upper()
                if sym and len(sym) >= 3 and sym not in STOPWORDS:
                    symbols.add(sym)
                if name and len(name) >= 4 and name not in STOPWORDS:
                    symbols.add(name)
            return symbols
    except Exception:
        return set()


async def _seed_treenews_rest(session: aiohttp.ClientSession):
    """Fetch recent TreeNews items to seed baseline words & arm any breaking news from the last 15m."""
    try:
        async with session.get(TREENEWS_REST_URL, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return
            items = await r.json(content_type=None)
            if not isinstance(items, list):
                return
            # Process oldest -> newest so baseline populates before newest breaking items
            for item in reversed(items):
                _ingest_news_item(item, is_seed=True)
    except Exception as e:
        print(f"[BreakingNews] REST seed error: {e}")


async def refresh_trends():
    """Refresh CoinGecko trending + sync TreeNews REST baseline/fallback."""
    global _trending_symbols, _last_refresh

    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        trending, _ = await asyncio.gather(
            _fetch_coingecko_trending(session),
            _seed_treenews_rest(session),
        )

    _trending_symbols = trending
    _prune_expired_catalysts()
    _last_refresh = time.time()
    print(
        f"[News] Refreshed — {len(_trending_symbols)} CoinGecko trending, "
        f"{len(_breaking_catalysts)} live breaking catalysts (<15m) | {len(_baseline_terms)} baseline terms"
    )


async def run_news_ws_loop():
    """Persistent sub-second TreeNews WebSocket listener."""
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                async with session.ws_connect(
                    TREENEWS_WS_URL,
                    headers=HEADERS,
                    heartbeat=25,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as ws:
                    print("[BreakingNews] Connected live to wss://news.treeofalpha.com/ws (<1s feed)")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                _ingest_news_item(data, is_seed=False)
                            except Exception:
                                continue
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[BreakingNews] WS reconnecting after error: {e}")
                await asyncio.sleep(5)


def match_breaking_catalyst(symbol: str, name: str) -> Optional[dict]:
    """
    Whole-word / exact-symbol check against active <15m breaking news catalysts.
    Zero substring false positives.
    """
    _prune_expired_catalysts()
    if not _breaking_catalysts:
        return None

    sym = re.sub(r"[^A-Z0-9]", "", (symbol or "").upper())
    name_clean = re.sub(r"[^A-Z0-9 ]", " ", (name or "").upper())
    name_clean = re.sub(r"\s+", " ", name_clean).strip()
    name_words = set(name_clean.split()) if name_clean else set()

    # 1. Exact symbol match (highest confidence)
    if sym and sym in _breaking_catalysts:
        return _breaking_catalysts[sym]

    # 2. Whole-word or exact 2-word phrase match in token name
    for term, info in list(_breaking_catalysts.items()):
        if " " in term:
            if term in name_clean:
                return info
        else:
            if term in name_words:
                return info

    return None


def get_news_score_bonus(symbol: str, name: str) -> Tuple[int, List[str]]:
    """
    Return (bonus_points, reasons) for a coin based on real breaking news & CoinGecko trending.
    Called from scorer.py.
    """
    sym = re.sub(r"[^A-Z0-9]", "", (symbol or "").upper())
    name_up = (name or "").upper().strip()

    bonus = 0
    reasons: List[str] = []

    # 1. Live <15m breaking news catalyst (whole-word match only)
    cat = match_breaking_catalyst(symbol, name)
    if cat:
        bonus += 20
        reasons.append(f"BREAKING ({cat['term']}): {cat['headline'][:70]}")

    # 2. CoinGecko trending exact match
    if (sym and sym in _trending_symbols) or (name_up and name_up in _trending_symbols):
        bonus += 15
        reasons.append("Trending on CoinGecko")

    return bonus, reasons


def get_active_catalysts() -> List[dict]:
    """Return currently armed <15m breaking catalysts for debugging/dashboard."""
    _prune_expired_catalysts()
    return sorted(_breaking_catalysts.values(), key=lambda x: x["ts"], reverse=True)


def is_stale() -> bool:
    return time.time() - _last_refresh > REFRESH_INTERVAL
