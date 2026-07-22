#!/usr/bin/env python3
"""
GCC Real Estate Intelligence Bot
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
A free-first, multi-tier intelligence cascade for Gulf (GCC) real-estate news.
It collects signals across five source tiers, dedupes them, then runs an LLM
editorial pipeline to draft KSA-focused LinkedIn post ideas — each refined
live through a stateful Telegram inline-keyboard workflow.

Source tiers:
  TIER 1 — News & press (RSS)     -> 20 curated GCC feeds + Google News RSS
  TIER 2 — Official data          -> DLD open data, SAMA, GASTAT
  TIER 3 — Social / forums        -> Reddit (.json), SkyscraperCity
  TIER 4 — X / LinkedIn (indirect)-> Google News RSS as a no-login proxy
  TIER 5 — Research firms          -> public newsroom RSS feeds
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Usage:
  python3 gcc_re_intel_bot.py --scrape   # scrape + push 10 idea cards to Telegram
  python3 gcc_re_intel_bot.py --bot      # run the interactive Telegram loop
  python3 gcc_re_intel_bot.py --test     # dry-run every source, print stats only
"""

import os, json, time, re, hashlib, logging, argparse
import feedparser, requests
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus, urljoin
from bs4 import BeautifulSoup

# ── CONFIG ───────────────────────────────────────────────────────────────────
# Secrets are read from the environment (optionally seeded by a local .env next
# to this file). No credentials live in the repo — see .env.example for the
# variable names.
def _load_env() -> dict:
    env = dict(os.environ)
    dotenv = Path(__file__).with_name(".env")
    if dotenv.exists():
        for line in dotenv.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            env.setdefault(key.strip(), val.strip())  # real env wins over .env
    return env

_env = _load_env()
TELEGRAM_TOKEN     = _env.get("TELEGRAM_TOKEN", "")
TELEGRAM_USER_ID   = int(_env.get("TELEGRAM_USER_ID", "0") or "0")
OPENROUTER_KEY     = _env.get("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL   = _env.get("OPENROUTER_MODEL", "deepseek/deepseek-chat")
OPENROUTER_REFERER = _env.get("OPENROUTER_REFERER", "https://github.com")

# Runtime state / cache / logs live in a local dir (override with DATA_DIR);
# generated infographics go to OUTPUT_DIR. Nothing here is committed.
DATA_DIR   = Path(_env.get("DATA_DIR", Path(__file__).with_name(".state")))
OUTPUT_DIR = Path(_env.get("OUTPUT_DIR", Path(__file__).with_name("output")))
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

STATE_FILE = str(DATA_DIR / "state.json")
NEWS_CACHE = str(DATA_DIR / "news_cache.json")
LOG_FILE   = str(DATA_DIR / "bot.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

RE_KEYWORDS = [
    "real estate", "property", "housing", "mortgage", "reit", "developer",
    "construction", "megaproject", "neom", "vision 2030", "diriyah", "qiddiya",
    "roshn", "aldar", "emaar", "nakheel", "damac", "dar al arkan",
    "residential", "commercial", "retail", "hospitality", "logistics",
    "saudi", "uae", "dubai", "riyadh", "abu dhabi", "gcc", "bahrain",
    "kuwait", "qatar", "oman", "jeddah", "sharjah", "dammam",
    "financing", "rents", "prices", "launches", "permits", "regulation",
    "rera", "sama", "gastat", "dld", "pif", "affordable housing", "luxury",
    "off-plan", "yield", "sqm", "sqft", "infrastructure", "urban",
    "zoning", "master plan", "economic city", "giga project",
    "tokenization", "proptech", "golden visa", "freehold", "leasehold"
]

# ═══════════════════════════════════════════════════════════════════════════════
# TIER 1 — RSS FEEDS (news & press)
# ═══════════════════════════════════════════════════════════════════════════════

RSS_FEEDS = [
    # ── GCC specialist media ─────────────────────────────────
    ("ArabianBusiness RE",      "https://www.arabianbusiness.com/rss/real-estate"),
    ("ArabianBusiness Latest",  "https://www.arabianbusiness.com/rss/latest-news"),
    ("Zawya RE",                "https://www.zawya.com/rss/real_estate/"),
    ("Gulf News Property",      "https://gulfnews.com/rss/property"),
    ("The National Property",   "https://www.thenationalnews.com/rss/business/property"),
    ("Khaleej Times Property",  "https://www.khaleejtimes.com/rss/property"),
    ("Saudi Gazette Business",  "https://saudigazette.com.sa/rss/business"),
    ("Arab News Business",      "https://www.arabnews.com/rss.xml?category=2"),
    ("Trade Arabia Construction","https://tradearabia.com/rss/CONS.xml"),
    ("Construction Week",       "https://www.constructionweekonline.com/rss"),
    ("Gulf Business",           "https://gulfbusiness.com/feed/"),
    ("Arabian Post",            "https://thearabianpost.com/feed/"),
    # ── Google News RSS (no login, no HTML scraping) ───────
    ("GNews KSA RE",    "https://news.google.com/rss/search?q=Saudi+Arabia+real+estate+property+2025&hl=en-US&gl=US&ceid=US:en"),
    ("GNews UAE RE",    "https://news.google.com/rss/search?q=UAE+Dubai+real+estate+property+2025&hl=en-US&gl=US&ceid=US:en"),
    ("GNews Mega",      "https://news.google.com/rss/search?q=NEOM+THE+LINE+Diriyah+Qiddiya+megaproject&hl=en-US&gl=US&ceid=US:en"),
    ("GNews GCC RE",    "https://news.google.com/rss/search?q=GCC+real+estate+Vision2030+property+investment&hl=en-US&gl=US&ceid=US:en"),
    ("GNews RE Law",    "https://news.google.com/rss/search?q=Saudi+UAE+real+estate+law+regulation+RERA+2025&hl=en-US&gl=US&ceid=US:en"),
    ("GNews KW BH QA",  "https://news.google.com/rss/search?q=Kuwait+Bahrain+Qatar+Oman+real+estate+property&hl=en-US&gl=US&ceid=US:en"),
    ("GNews PropTech",  "https://news.google.com/rss/search?q=GCC+proptech+tokenization+real+estate+tech+2025&hl=en-US&gl=US&ceid=US:en"),
    # ── Research firms (public newsrooms) ───────────────────
    ("CBRE Research",   "https://www.cbre.com/rss/research"),
]

def scrape_rss() -> list:
    articles = []
    seen = set()
    cutoff = datetime.utcnow() - timedelta(hours=48)

    for source, url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url, request_headers=HEADERS)
            count = 0
            for entry in feed.entries[:25]:
                title   = entry.get("title", "").strip()
                summary = entry.get("summary", entry.get("description", "")).strip()
                link    = entry.get("link", "")

                # Date filter
                pub = entry.get("published_parsed")
                if pub:
                    pub_dt = datetime(*pub[:6])
                    if pub_dt < cutoff:
                        continue

                # Relevance filter
                full = (title + " " + summary).lower()
                if not any(kw in full for kw in RE_KEYWORDS):
                    continue

                # Dedup
                h = hashlib.md5(title.lower().encode()).hexdigest()
                if h in seen:
                    continue
                seen.add(h)

                summary_clean = re.sub(r'<[^>]+>', '', summary)[:600]
                articles.append({
                    "source": source, "tier": "news",
                    "title": title, "summary": summary_clean,
                    "link": link, "date": entry.get("published", "")
                })
                count += 1
            log.info(f"  RSS [{source}]: {count} articles")
        except Exception as e:
            log.warning(f"  RSS error [{source}]: {e}")

    return articles


# ═══════════════════════════════════════════════════════════════════════════════
# TIER 2 — OFFICIAL DATA SOURCES
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_dubaipulse_news() -> list:
    """Scrape DLD/DubaiPulse latest news (open data portal)"""
    articles = []
    try:
        url = "https://dubailand.gov.ae/en/news-and-media/latest-news/"
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        cutoff = datetime.utcnow() - timedelta(days=7)

        for item in soup.select(".news-item, .media-card, article")[:15]:
            title_el = item.select_one("h2, h3, .title, .news-title")
            link_el  = item.select_one("a[href]")
            date_el  = item.select_one(".date, time, .news-date")

            if not title_el:
                continue

            title = title_el.get_text(strip=True)
            link  = urljoin(url, link_el["href"]) if link_el else url
            date  = date_el.get_text(strip=True) if date_el else ""

            if not any(kw in title.lower() for kw in RE_KEYWORDS[:20]):
                continue

            articles.append({
                "source": "Dubai Land Department (Official)",
                "tier": "official_data",
                "title": title, "summary": f"Official DLD announcement. {date}",
                "link": link, "date": date
            })

        log.info(f"  DLD: {len(articles)} items")
    except Exception as e:
        log.warning(f"  DLD fetch error: {e}")
    return articles


def fetch_sama_news() -> list:
    """SAMA (Saudi Central Bank) — mortgage/financing press releases"""
    articles = []
    try:
        url = "https://www.sama.gov.sa/en-US/News/Pages/News.aspx"
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        for item in soup.select(".news-item, .ms-rtestate-field, li")[:20]:
            text = item.get_text(strip=True)
            if len(text) < 20:
                continue
            if not any(kw in text.lower() for kw in ["mortgage", "real estate", "housing", "financing", "property"]):
                continue
            link_el = item.select_one("a[href]")
            link = urljoin(url, link_el["href"]) if link_el else url
            articles.append({
                "source": "SAMA (Saudi Central Bank)", "tier": "official_data",
                "title": text[:120], "summary": "SAMA official release — mortgage/financing data",
                "link": link, "date": ""
            })

        log.info(f"  SAMA: {len(articles)} items")
    except Exception as e:
        log.warning(f"  SAMA fetch error: {e}")
    return articles


def fetch_gastat_news() -> list:
    """GASTAT (Saudi General Authority for Statistics) — price indices"""
    articles = []
    try:
        url = "https://www.stats.gov.sa/en/news"
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        for item in soup.select("article, .news-card, .list-item")[:15]:
            title_el = item.select_one("h2, h3, .title")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if not any(kw in title.lower() for kw in ["real estate", "price", "housing", "property", "construction", "rent"]):
                continue
            link_el = item.select_one("a[href]")
            link = urljoin(url, link_el["href"]) if link_el else url
            articles.append({
                "source": "GASTAT (Saudi Statistics)", "tier": "official_data",
                "title": title, "summary": "GASTAT statistical release",
                "link": link, "date": ""
            })

        log.info(f"  GASTAT: {len(articles)} items")
    except Exception as e:
        log.warning(f"  GASTAT fetch error: {e}")
    return articles


# ═══════════════════════════════════════════════════════════════════════════════
# TIER 3 — REDDIT (via .json endpoint — free, no auth)
# ═══════════════════════════════════════════════════════════════════════════════

SUBREDDITS = [
    # GCC-specific
    ("r/dubai",             "dubai"),
    ("r/abudhabi",          "abudhabi"),
    ("r/saudiarabia",       "saudiarabia"),
    ("r/qatar",             "qatar"),
    ("r/kuwait",            "kuwait"),
    ("r/bahrain",           "bahrain"),
    # RE investing global (GCC mentions)
    ("r/realestateinvesting","realestateinvesting"),
    ("r/expatfinance",      "ExpatFinance"),
    ("r/digitalnomad",      "digitalnomad"),  # Dubai relocations
]

REDDIT_RE_KEYWORDS = [
    "property", "real estate", "apartment", "villa", "rent", "buy", "invest",
    "off-plan", "mortgage", "developer", "dubai property", "riyadh", "neom",
    "yield", "golden visa", "freehold", "rera", "dld", "emaar", "aldar",
    "damac", "roshn", "price", "sqft", "dirhams", "sar", "aed"
]

def scrape_reddit() -> list:
    """
    Fetch Reddit posts via the unofficial .json API.
    No auth needed. Rate limit: ~60 requests/minute. We stay far below.
    """
    articles = []
    seen = set()

    for display_name, subreddit in SUBREDDITS:
        try:
            # .json endpoint — no login, no API key
            url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=25&t=day"
            r = requests.get(url, headers={
                **HEADERS,
                "User-Agent": "GCCRealEstateBot/1.0 (research only)"
            }, timeout=15)

            if r.status_code != 200:
                log.warning(f"  Reddit [{subreddit}]: HTTP {r.status_code}")
                continue

            data = r.json()
            posts = data.get("data", {}).get("children", [])
            count = 0

            for post in posts:
                p = post.get("data", {})
                title   = p.get("title", "").strip()
                selftext = p.get("selftext", "").strip()[:400]
                url_post = f"https://reddit.com{p.get('permalink', '')}"
                score    = p.get("score", 0)
                n_comments = p.get("num_comments", 0)
                created  = p.get("created_utc", 0)

                # Age filter: last 48h
                if created and (time.time() - created) > 172800:
                    continue

                # Relevance filter
                full = (title + " " + selftext).lower()
                if not any(kw in full for kw in REDDIT_RE_KEYWORDS):
                    continue

                # Min engagement (not pure spam)
                if score < 2 and n_comments < 1:
                    continue

                h = hashlib.md5(title.lower().encode()).hexdigest()
                if h in seen:
                    continue
                seen.add(h)

                articles.append({
                    "source": f"Reddit {display_name}",
                    "tier": "social",
                    "title": title,
                    "summary": (selftext or f"Score: {score} | Comments: {n_comments}")[:400],
                    "link": url_post,
                    "date": datetime.utcfromtimestamp(created).isoformat() if created else "",
                    "engagement": {"score": score, "comments": n_comments}
                })
                count += 1

            log.info(f"  Reddit [{subreddit}]: {count} relevant posts")
            time.sleep(1.5)  # Polite delay between subreddits

        except Exception as e:
            log.warning(f"  Reddit [{subreddit}] error: {e}")

    return articles


def scrape_reddit_search(query: str, label: str) -> list:
    """Search Reddit across all subreddits for a specific GCC RE topic"""
    articles = []
    try:
        q = quote_plus(query)
        url = f"https://www.reddit.com/search.json?q={q}&sort=new&t=week&limit=15"
        r = requests.get(url, headers={
            **HEADERS,
            "User-Agent": "GCCRealEstateBot/1.0 (research only)"
        }, timeout=15)

        if r.status_code != 200:
            return []

        posts = r.json().get("data", {}).get("children", [])
        for post in posts:
            p = post.get("data", {})
            title = p.get("title", "").strip()
            if not title:
                continue
            articles.append({
                "source": f"Reddit Search: {label}",
                "tier": "social",
                "title": title,
                "summary": p.get("selftext", "")[:300],
                "link": f"https://reddit.com{p.get('permalink', '')}",
                "date": datetime.utcfromtimestamp(p.get("created_utc", 0)).isoformat(),
                "engagement": {"score": p.get("score", 0), "comments": p.get("num_comments", 0)}
            })
        time.sleep(1.5)
    except Exception as e:
        log.warning(f"  Reddit search [{label}] error: {e}")
    return articles


# ═══════════════════════════════════════════════════════════════════════════════
# TIER 4 — X / TWITTER (via Google cache — no login, free)
# ═══════════════════════════════════════════════════════════════════════════════

TWITTER_QUERIES = [
    ("Dubai real estate market 2025 site:twitter.com OR site:x.com",     "X: Dubai RE"),
    ("Saudi Arabia property Vision2030 site:twitter.com OR site:x.com",   "X: KSA RE"),
    ("NEOM megaproject update site:twitter.com OR site:x.com",            "X: NEOM"),
    ("GCC real estate investment site:twitter.com OR site:x.com",         "X: GCC RE"),
    ("off-plan Dubai apartments site:twitter.com OR site:x.com",          "X: Off-plan"),
]

def scrape_twitter_via_google() -> list:
    """
    Fetch recent Twitter/X public posts via Google News search cache.
    Works without any Twitter account or API key.
    Returns public tweets mentioning GCC RE topics.
    """
    articles = []
    seen = set()

    for query, label in TWITTER_QUERIES:
        try:
            # Google News RSS — often indexes recent Twitter discussions
            q = quote_plus(query)
            url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url, request_headers=HEADERS)

            count = 0
            for entry in feed.entries[:8]:
                title = entry.get("title", "").strip()
                link  = entry.get("link", "")
                if not title:
                    continue

                h = hashlib.md5(title.lower().encode()).hexdigest()
                if h in seen:
                    continue
                seen.add(h)

                articles.append({
                    "source": label,
                    "tier": "social_indirect",
                    "title": title,
                    "summary": entry.get("summary", "")[:300],
                    "link": link,
                    "date": entry.get("published", "")
                })
                count += 1

            log.info(f"  Twitter/Google [{label}]: {count} items")
            time.sleep(1)

        except Exception as e:
            log.warning(f"  Twitter/Google [{label}] error: {e}")

    return articles


# ═══════════════════════════════════════════════════════════════════════════════
# TIER 4b — LINKEDIN (via Google cache — public posts only)
# ═══════════════════════════════════════════════════════════════════════════════

LINKEDIN_QUERIES = [
    ("Dubai real estate market analysis site:linkedin.com",           "LinkedIn: Dubai RE"),
    ("Saudi Arabia property investment insight site:linkedin.com",    "LinkedIn: KSA RE"),
    ("GCC real estate developer news site:linkedin.com",              "LinkedIn: GCC Dev"),
    ("NEOM Diriyah Qiddiya update site:linkedin.com",                 "LinkedIn: Mega"),
    ("UAE mortgage financing market site:linkedin.com",               "LinkedIn: UAE Finance"),
]

def scrape_linkedin_via_google() -> list:
    """
    Google indexes public LinkedIn posts/articles.
    This fetches them via Google News RSS — no login, no scraping of LinkedIn directly.
    """
    articles = []
    seen = set()

    for query, label in LINKEDIN_QUERIES:
        try:
            q = quote_plus(query)
            url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url, request_headers=HEADERS)

            count = 0
            for entry in feed.entries[:6]:
                title = entry.get("title", "").strip()
                if not title or len(title) < 15:
                    continue
                h = hashlib.md5(title.lower().encode()).hexdigest()
                if h in seen:
                    continue
                seen.add(h)

                articles.append({
                    "source": label,
                    "tier": "social_indirect",
                    "title": title,
                    "summary": entry.get("summary", "")[:300],
                    "link": entry.get("link", ""),
                    "date": entry.get("published", "")
                })
                count += 1

            log.info(f"  LinkedIn/Google [{label}]: {count} items")
            time.sleep(1)

        except Exception as e:
            log.warning(f"  LinkedIn/Google [{label}] error: {e}")

    return articles


# ═══════════════════════════════════════════════════════════════════════════════
# TIER 5 — SPECIALIST FORUMS
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_skyscrapercity() -> list:
    """SkyscraperCity — forum tracking GCC real-estate / urban projects"""
    articles = []
    threads = [
        ("https://www.skyscrapercity.com/forums/dubai-urban-development.44/",     "SSC: Dubai Urbanism"),
        ("https://www.skyscrapercity.com/forums/saudi-arabia-projects.396/",       "SSC: KSA Projects"),
        ("https://www.skyscrapercity.com/forums/abu-dhabi-urban-development.166/", "SSC: Abu Dhabi"),
    ]
    for url, label in threads:
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            seen_titles = set()

            for item in soup.select(".structItem-title, .thread-title, h3 a")[:10]:
                title = item.get_text(strip=True)
                if len(title) < 10 or title in seen_titles:
                    continue
                seen_titles.add(title)
                href = item.get("href", "")
                link = urljoin(url, href) if href else url
                articles.append({
                    "source": label, "tier": "forum",
                    "title": title,
                    "summary": "SkyscraperCity thread — developer/project discussion",
                    "link": link, "date": ""
                })
            log.info(f"  SkyscraperCity [{label}]: {len(articles)} threads")
            time.sleep(1)
        except Exception as e:
            log.warning(f"  SkyscraperCity [{label}] error: {e}")
    return articles


def scrape_propertyfinder_blog() -> list:
    """PropertyFinder blog — market reports UAE"""
    articles = []
    try:
        url = "https://www.propertyfinder.ae/blog/"
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        for item in soup.select("article, .blog-card, .post")[:10]:
            title_el = item.select_one("h2, h3, .entry-title")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            link_el = item.select_one("a[href]")
            link = urljoin(url, link_el["href"]) if link_el else url
            articles.append({
                "source": "PropertyFinder Blog (UAE)",
                "tier": "forum",
                "title": title,
                "summary": "PropertyFinder market analysis / UAE property blog",
                "link": link, "date": ""
            })
        log.info(f"  PropertyFinder blog: {len(articles)} items")
    except Exception as e:
        log.warning(f"  PropertyFinder blog error: {e}")
    return articles


# ═══════════════════════════════════════════════════════════════════════════════
# MASTER SCRAPE — assemble every source tier
# ═══════════════════════════════════════════════════════════════════════════════

def run_full_scrape(test_mode=False) -> list:
    log.info("━━━━ STARTING FULL SCRAPE ━━━━")
    all_articles = []

    # TIER 1 — RSS
    log.info("── TIER 1: RSS News Feeds")
    all_articles += scrape_rss()

    # TIER 2 — Official data
    log.info("── TIER 2: Official Data Sources")
    all_articles += fetch_dubaipulse_news()
    all_articles += fetch_sama_news()
    all_articles += fetch_gastat_news()

    # TIER 3 — Reddit
    log.info("── TIER 3: Reddit")
    all_articles += scrape_reddit()
    # Extra targeted Reddit searches
    for q, label in [
        ("Dubai real estate 2025",         "Dubai RE 2025"),
        ("Saudi Arabia property market",   "KSA Property"),
        ("NEOM construction update",       "NEOM"),
        ("UAE off-plan investment",        "UAE Off-plan"),
    ]:
        all_articles += scrape_reddit_search(q, label)

    # TIER 4 — X / LinkedIn via Google
    log.info("── TIER 4: Social (via Google)")
    all_articles += scrape_twitter_via_google()
    all_articles += scrape_linkedin_via_google()

    # TIER 5 — Forums
    log.info("── TIER 5: Forums")
    all_articles += scrape_skyscrapercity()
    all_articles += scrape_propertyfinder_blog()

    # Dedup across all sources
    seen = set()
    unique = []
    for a in all_articles:
        h = hashlib.md5(a["title"].lower().encode()).hexdigest()
        if h not in seen:
            seen.add(h)
            unique.append(a)

    # Stats by tier
    tiers = {}
    for a in unique:
        t = a.get("tier", "unknown")
        tiers[t] = tiers.get(t, 0) + 1

    log.info(f"━━━━ SCRAPE COMPLETE: {len(unique)} unique articles")
    log.info(f"     By tier: {tiers}")

    return unique


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def tg_send(text: str, keyboard=None, parse_mode="HTML"):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_USER_ID,
        "text": text[:4096],
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    if keyboard:
        payload["reply_markup"] = json.dumps({"inline_keyboard": keyboard})
    try:
        r = requests.post(url, json=payload, timeout=15)
        return r.json()
    except Exception as e:
        log.error(f"TG send error: {e}")
        return {}

def tg_send_doc(filepath: str, caption: str = ""):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    try:
        with open(filepath, "rb") as f:
            r = requests.post(url, data={
                "chat_id": TELEGRAM_USER_ID,
                "caption": caption[:1024]
            }, files={"document": f}, timeout=30)
        return r.json()
    except Exception as e:
        log.error(f"TG doc error: {e}")
        return {}

def tg_answer(callback_id: str, text: str = ""):
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
        json={"callback_query_id": callback_id, "text": text}, timeout=10
    )

def tg_get_updates(offset=None):
    params = {"timeout": 30, "allowed_updates": ["message", "callback_query"]}
    if offset:
        params["offset"] = offset
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params=params, timeout=40
        )
        return r.json().get("result", [])
    except:
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════════════════════════

def load_state():
    try:
        return json.loads(Path(STATE_FILE).read_text())
    except:
        return {"offset": 0, "ideas": [], "selected": None, "mode": "idle", "last_post": None}

def save_state(s):
    Path(STATE_FILE).write_text(json.dumps(s, ensure_ascii=False, indent=2))

def load_cache():
    try:
        return json.loads(Path(NEWS_CACHE).read_text())
    except:
        return {"articles": [], "last_run": None}

def save_cache(c):
    Path(NEWS_CACHE).write_text(json.dumps(c, ensure_ascii=False, indent=2))


# ═══════════════════════════════════════════════════════════════════════════════
# AI — OPENROUTER
# ═══════════════════════════════════════════════════════════════════════════════

def call_ai(system: str, user: str, max_tokens=3000) -> str:
    r = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {OPENROUTER_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": OPENROUTER_REFERER,
            "X-Title": "GCC RE Intel Bot"
        },
        json={
            "model": OPENROUTER_MODEL,
            "max_tokens": max_tokens,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user}
            ]
        },
        timeout=90
    )
    return r.json()["choices"][0]["message"]["content"]


def generate_ideas(articles: list) -> list:
    # Build rich context with tier labels
    context_parts = []
    for a in articles[:50]:
        tier_label = {
            "news": "📰", "official_data": "🏛️", "social": "💬",
            "social_indirect": "🐦", "forum": "🗣️"
        }.get(a.get("tier", ""), "•")
        context_parts.append(
            f"{tier_label} [{a['source']}] {a['title']}\n{a['summary'][:200]}\n{a['link']}"
        )

    system = """You are one of the world's leading experts on the Saudi Arabian real estate market.
You have 20 years of experience advising PIF-backed developers, international institutional investors, and top-tier consultancies (CBRE, Knight Frank, Savills MENA).

Your audience on LinkedIn: Saudi real estate developers, international investors entering KSA, consultants, C-suite at ROSHN/NEOM/Diriyah/Qiddiya, Vision 2030 watchers.

You are known for:
- Identifying structural misfits between policy, supply, demand and pricing in KSA
- Cross-referencing SAMA mortgage data, GASTAT price indices, ROSHN pipeline, PIF announcements
- Exposing what the headline numbers miss (e.g. "mortgage volumes up 30% BUT average ticket size down 18%")
- Connecting Vision 2030 milestones to on-the-ground developer opportunity or risk
- Writing that makes a CFO or fund manager stop scrolling

STRICT RULES:
- ONLY Saudi Arabia topics. No UAE, no GCC generic posts.
- Every idea must have a specific data angle with real numbers
- Every idea must have a clear "so what" for either a developer OR an investor (or both)
- NO fluff, NO hype, NO "Saudi Arabia is booming" without specifics
- Topics must come from: mortgage financing, housing supply/demand, megaproject economics, land regulation, foreign investment rules, hospitality RE, logistics RE, affordable housing gap, Vision 2030 deadlines pressure

Reference post style:
"In 2018, apartments represented 12% of Saudi mortgage financing. In 2025, it's 30%.
But Riyadh's housing stock? Still 46% villas and only 34% apartments.
We cross-referenced SAMA, GASTAT census, GASTAT price index. Here's what stands out."
"""

    user = f"""Here is today's GCC real estate intelligence across ALL sources (news, official data, Reddit sentiment, social media, forums):

{chr(10).join(context_parts)}

Generate exactly 10 LinkedIn post ideas. Each must:
1. Be EXCLUSIVELY about Saudi Arabia real estate
2. Have a specific data angle — real numbers, percentages, SAR amounts, YoY comparisons
3. Identify a structural tension, policy gap, or market mismatch — not just "market is growing"
4. Have a clear implication for: (a) a developer building in KSA, OR (b) an international investor evaluating entry
5. Be based on something concrete from the sources above (not invented)
6. Cover diverse KSA topics: Riyadh residential, Jeddah coastal, NEOM/giga-projects economics, ROSHN affordable housing, SAMA mortgage trends, foreign ownership rules, hospitality pipeline, logistics/industrial RE, land banking, off-plan regulation

REJECT any idea that:
- Could apply to any market (not KSA-specific)
- Has no specific number or data point
- Is just a restatement of official PR ("Vision 2030 is transforming...")
- Is about UAE, GCC broadly, or any non-KSA market

Return ONLY a JSON array:
[
  {{
    "id": 1,
    "hook": "One-sentence scroll-stopping opener with a specific number or fact",
    "angle": "Core insight in 2-3 sentences",
    "opportunity": "What developers/investors should do with this",
    "sources_used": ["source1", "source2"],
    "geography": "UAE|KSA|GCC|Qatar|etc",
    "topic": "regulation|supply-demand|financing|megaproject|foreign-investment|proptech|sentiment",
    "complexity": "simple|data-driven|investigative"
  }}
]"""

    raw = call_ai(system, user, max_tokens=3000)
    raw = re.sub(r'^```json\s*', '', raw.strip())
    raw = re.sub(r'\s*```$', '', raw)
    import re as re2
    match = re2.search(r'\[[\s\S]+\]', raw)
    if match:
        raw = match.group()
    return json.loads(raw)


def generate_post(idea: dict, articles: list, instructions: str = "") -> dict:
    # Find most relevant articles for this idea
    geo = idea.get("geography", "").lower()
    topic = idea.get("topic", "").lower()
    sources_used = [s.lower() for s in idea.get("sources_used", [])]

    scored = []
    for a in articles:
        score = 0
        full = (a["title"] + " " + a.get("summary", "")).lower()
        if any(s in a["source"].lower() for s in sources_used): score += 3
        if any(kw in full for kw in geo.split("|")): score += 2
        if any(kw in full for kw in topic.split("-")): score += 1
        if a.get("tier") == "official_data": score += 2
        if a.get("tier") == "social": score += 1
        scored.append((score, a))

    scored.sort(key=lambda x: -x[0])
    context = "\n\n".join([
        f"[{a['source']} | {a.get('tier','?')}]\n{a['title']}\n{a.get('summary','')[:300]}\n{a['link']}"
        for _, a in scored[:8]
    ])

    system = """You are a senior Saudi Arabia real estate market specialist writing for LinkedIn.
Your readers are: KSA developers, international fund managers evaluating Saudi exposure, Vision 2030 consultants, C-suite at ROSHN/NEOM/Diriyah/Qiddiya, regional REITs.

Post structure (proven format):
1. HOOK (2 lines max): One specific, counterintuitive number or fact about KSA RE. Make it sting.
2. THE MISMATCH (3-4 lines): What two forces are pulling in opposite directions in this market right now?
3. THE DATA (3-5 short paragraphs): Walk through the numbers. Each paragraph = one data point. Cite source by name (SAMA, GASTAT, CBRE, Knight Frank, PIF, ROSHN annual report...). Use SAR amounts, %, sqm, YoY.
4. WHAT THE MARKET IS MISSING (2-3 lines): The non-obvious insight. What does this mean that most analysts aren't saying?
5. THE IMPLICATION (2-3 lines): Specific action or consideration for a developer OR investor in KSA.
6. CLOSE (1 line): One sharp sentence that reframes the whole post.
7. SOURCES: Bullet list of named sources.

Hard rules:
- ONLY Saudi Arabia. Zero UAE or generic GCC content.
- Every claim needs a number. No vague statements.
- NEVER: "game-changer", "unprecedented", "exciting", "Vision 2030 is transforming", "thrilled to share"
- Max 550 words. LinkedIn mobile readers scan fast.
- Line breaks after every 2-3 sentences. No walls of text.
- If you cite a data point you're not certain about, write "estimated" or "reported" — never invent numbers."""

    user = f"""Write a LinkedIn post based on:

HOOK: {idea['hook']}
ANGLE: {idea['angle']}
OPPORTUNITY: {idea['opportunity']}
GEOGRAPHY: {idea.get('geography','')}
TOPIC: {idea.get('topic','')}

Source material:
{context}

{f"Additional instructions from editor: {instructions}" if instructions else ""}

After the post, add "---INFOGRAPHIC---" then this JSON:
{{
  "headline": "Short visual headline (max 8 words)",
  "subtitle": "One-line context",
  "kpis": [
    {{"label": "Metric", "value": "X%", "delta": "+Y% YoY", "trend": "up|down|flat"}},
    {{"label": "Metric", "value": "...", "delta": "...", "trend": "..."}},
    {{"label": "Metric", "value": "...", "delta": "...", "trend": "..."}},
    {{"label": "Metric", "value": "...", "delta": "...", "trend": "..."}}
  ],
  "insight": "The one-sentence key takeaway",
  "sources": ["Source 1", "Source 2", "Source 3"]
}}"""

    raw = call_ai(system, user, max_tokens=4000)

    if "---INFOGRAPHIC---" in raw:
        parts = raw.split("---INFOGRAPHIC---")
        post_text = parts[0].strip()
        json_match = re.search(r'\{[\s\S]+\}', parts[1])
        infographic = json.loads(json_match.group()) if json_match else {}
    else:
        post_text = raw
        infographic = {}

    return {"post": post_text, "infographic": infographic}


# ═══════════════════════════════════════════════════════════════════════════════
# INFOGRAPHIC HTML
# ═══════════════════════════════════════════════════════════════════════════════

def build_infographic(data: dict, idea: dict) -> str:
    headline = data.get("headline", idea.get("hook", "GCC Real Estate")[:60])
    subtitle = data.get("subtitle", "")
    kpis = data.get("kpis", [])
    insight = data.get("insight", "")
    sources = data.get("sources", [])
    today = datetime.utcnow().strftime("%B %d, %Y")

    kpi_cards = ""
    for k in kpis[:4]:
        trend = k.get("trend", "flat")
        color = {"up": "#4ade80", "down": "#f87171", "flat": "#94a3b8"}.get(trend, "#94a3b8")
        arrow = {"up": "↑", "down": "↓", "flat": "→"}.get(trend, "→")
        kpi_cards += f"""
        <div class="kpi">
          <div class="kpi-label">{k.get('label','')}</div>
          <div class="kpi-value" style="color:{color}">{k.get('value','')} <span class="arrow">{arrow}</span></div>
          <div class="kpi-delta">{k.get('delta','')}</div>
        </div>"""

    src_html = "  ·  ".join(sources)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>GCC RE Intel</title>
<link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@600;700&family=IBM+Plex+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ background:#07090f; display:flex; align-items:center; justify-content:center; min-height:100vh; padding:32px; font-family:'IBM Plex Sans',sans-serif; }}
.card {{ width:840px; background:linear-gradient(160deg,#0d1117 0%,#161b22 60%,#0d1117 100%); border:1px solid rgba(212,175,55,0.2); border-radius:20px; overflow:hidden; position:relative; }}
.card::before {{ content:''; position:absolute; top:0;left:0;right:0; height:2px; background:linear-gradient(90deg,transparent,#d4af37,transparent); }}
.header {{ padding:36px 44px 28px; border-bottom:1px solid rgba(255,255,255,0.05); }}
.meta {{ font-size:10px; font-weight:500; letter-spacing:3px; text-transform:uppercase; color:#d4af37; margin-bottom:14px; }}
.headline {{ font-family:'Cormorant Garamond',serif; font-size:30px; font-weight:700; color:#f0f6fc; line-height:1.25; max-width:600px; margin-bottom:10px; }}
.subtitle {{ font-size:14px; color:#7d8590; font-weight:300; line-height:1.6; }}
.date {{ position:absolute; top:36px; right:44px; font-size:11px; color:#484f58; text-align:right; line-height:1.7; }}
.kpi-grid {{ display:grid; grid-template-columns:repeat(4,1fr); gap:1px; background:rgba(255,255,255,0.04); }}
.kpi {{ background:#0d1117; padding:24px 20px; }}
.kpi-label {{ font-size:10px; font-weight:500; letter-spacing:1.5px; text-transform:uppercase; color:#484f58; margin-bottom:10px; }}
.kpi-value {{ font-family:'Cormorant Garamond',serif; font-size:28px; font-weight:700; line-height:1; margin-bottom:6px; }}
.arrow {{ font-size:18px; vertical-align:middle; }}
.kpi-delta {{ font-size:11px; color:#7d8590; }}
.insight-block {{ padding:24px 44px; background:rgba(212,175,55,0.03); border-top:1px solid rgba(212,175,55,0.1); border-bottom:1px solid rgba(255,255,255,0.04); }}
.insight {{ font-size:15px; font-weight:400; color:#cdd9e5; line-height:1.65; padding-left:16px; border-left:2px solid #d4af37; font-style:italic; }}
.footer {{ padding:16px 44px; display:flex; justify-content:space-between; align-items:center; }}
.sources {{ font-size:10px; color:#484f58; }}
.brand {{ font-family:'Cormorant Garamond',serif; font-size:13px; color:#d4af37; opacity:0.5; letter-spacing:1px; }}
</style>
</head>
<body>
<div class="card">
  <div class="header">
    <div class="meta">GCC Real Estate Intelligence · Daily Brief</div>
    <h1 class="headline">{headline}</h1>
    {f'<p class="subtitle">{subtitle}</p>' if subtitle else ''}
    <div class="date">{today}<br><span style="color:#d4af37">Market Signal</span></div>
  </div>
  <div class="kpi-grid">{kpi_cards}</div>
  {f'<div class="insight-block"><p class="insight">{insight}</p></div>' if insight else ''}
  <div class="footer">
    <div class="sources">{src_html}</div>
    <div class="brand">RE Intel</div>
  </div>
</div>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════════════════════
# DAILY RUN — scrape + send ideas
# ═══════════════════════════════════════════════════════════════════════════════

def daily_run():
    tg_send("🔍 <b>GCC RE Intelligence — Daily Brief</b>\nScraping all sources... ⏳\n<i>News · Official data · Reddit · Social · Forums</i>")

    articles = run_full_scrape()

    # Tier summary
    tiers = {}
    for a in articles:
        t = a.get("tier","?")
        tiers[t] = tiers.get(t,0)+1

    tier_msg = "\n".join([
        f"  📰 News: {tiers.get('news',0)}",
        f"  🏛️ Official data: {tiers.get('official_data',0)}",
        f"  💬 Reddit: {tiers.get('social',0)}",
        f"  🐦 X/LinkedIn: {tiers.get('social_indirect',0)}",
        f"  🗣️ Forums: {tiers.get('forum',0)}",
    ])
    tg_send(f"✅ <b>{len(articles)} signals collected</b>\n{tier_msg}\n\nGenerating 10 post ideas... 🧠")

    save_cache({"articles": articles, "last_run": datetime.utcnow().isoformat()})

    ideas = generate_ideas(articles)

    state = load_state()
    state.update({"ideas": ideas, "selected": None, "mode": "selecting"})
    save_state(state)

    # Format ideas message
    msg = "📊 <b>Today's 10 Post Ideas</b>\n<i>Tap to generate full post + infographic</i>\n\n"
    emoji_map = {"simple": "📌", "data-driven": "📊", "investigative": "🔬"}
    geo_flag  = {"UAE": "🇦🇪", "KSA": "🇸🇦", "GCC": "🌍", "Qatar": "🇶🇦", "Kuwait": "🇰🇼", "Bahrain": "🇧🇭", "Oman": "🇴🇲"}

    for idea in ideas:
        em = emoji_map.get(idea.get("complexity",""), "📌")
        geo = idea.get("geography","")
        flag = geo_flag.get(geo, "🌍")
        msg += f"{em} <b>{idea['id']}.</b> {flag} {idea['hook']}\n"
        msg += f"<i>{idea['angle'][:90]}...</i>\n\n"

    keyboard = []
    row = []
    for idea in ideas:
        geo = idea.get("geography","")
        flag = geo_flag.get(geo,"🌍")
        btn = {"text": f"{flag} {idea['id']}. {idea['hook'][:28]}...", "callback_data": f"idea_{idea['id']}"}
        row.append(btn)
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    tg_send(msg, keyboard=keyboard)
    log.info("Daily ideas sent ✓")


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM BOT LOOP
# ═══════════════════════════════════════════════════════════════════════════════

ACTION_INSTRUCTIONS = {
    "action_tone":     "Rewrite with a sharper, more Bloomberg-analyst tone. Less diplomatic, more direct. Cut adjectives. Lead with the harshest truth first.",
    "action_data":     "Add 3-4 more specific data points with source citations. Include YoY comparisons where possible. Make it feel like a research note.",
    "action_angle":    "Rewrite from a completely opposite angle: if it was bullish, make it bearish (or vice versa). What are the risks being overlooked?",
    "action_shorter":  "Cut to 300 words max. Keep only the 3 most powerful points. One hook, one data section, one clear so-what.",
    "action_investor": "Rewrite specifically for a foreign institutional investor. Include: entry yield, currency considerations, liquidity risk, regulatory risk.",
}

def run_bot():
    log.info("Bot loop starting...")
    state = load_state()

    while True:
        updates = tg_get_updates(offset=state.get("offset"))

        for update in updates:
            state["offset"] = update["update_id"] + 1
            save_state(state)

            # ── CALLBACK: button tap ──────────────────────────────────────
            if "callback_query" in update:
                cb = update["callback_query"]
                data = cb.get("data","")
                tg_answer(cb["id"])

                # Idea selected
                if data.startswith("idea_"):
                    idea_id = int(data.split("_")[1])
                    idea = next((i for i in state.get("ideas",[]) if i["id"] == idea_id), None)
                    if not idea:
                        tg_send("❌ Idea not found. Try /morning to regenerate.")
                        continue

                    state["selected"] = idea
                    state["mode"] = "generating"
                    save_state(state)

                    tg_send(f"⚡ Generating post #{idea_id}...\n<i>{idea['hook']}</i>")

                    cache = load_cache()
                    result = generate_post(idea, cache.get("articles",[]))
                    state["last_post"] = result
                    state["mode"] = "reviewing"
                    save_state(state)

                    # Send post (split if needed)
                    post = result["post"]
                    for chunk in [post[i:i+3800] for i in range(0, len(post), 3800)]:
                        tg_send(f"📝 <b>Draft:</b>\n\n{chunk}")

                    # Infographic
                    if result.get("infographic"):
                        html = build_infographic(result["infographic"], idea)
                        fpath = str(OUTPUT_DIR / f"infographic_{idea_id}_{int(time.time())}.html")
                        Path(fpath).write_text(html)
                        tg_send_doc(fpath, "🖼️ Infographic — open in browser, screenshot for LinkedIn")

                    keyboard = [
                        [{"text": "✅ Approve & copy", "callback_data": "action_approve"},
                         {"text": "📊 More data", "callback_data": "action_data"}],
                        [{"text": "🎯 Sharper tone", "callback_data": "action_tone"},
                         {"text": "↔️ Opposite angle", "callback_data": "action_angle"}],
                        [{"text": "✂️ Shorter", "callback_data": "action_shorter"},
                         {"text": "💼 Investor focus", "callback_data": "action_investor"}],
                        [{"text": "⬅️ Back to ideas", "callback_data": "action_back"}]
                    ]
                    tg_send("What would you like to do with this draft?", keyboard=keyboard)

                elif data == "action_approve":
                    post = state.get("last_post", {}).get("post", "")
                    tg_send(f"✅ <b>Ready to post on LinkedIn:</b>\n\n{post[:4000]}")
                    tg_send("💡 Copy the text above → paste in LinkedIn → attach the infographic screenshot.")
                    state["mode"] = "idle"
                    save_state(state)

                elif data == "action_back":
                    ideas = state.get("ideas",[])
                    if not ideas:
                        tg_send("No ideas loaded. Run /morning first.")
                        continue
                    msg = "📊 <b>Choose another idea:</b>\n\n"
                    for idea in ideas:
                        msg += f"<b>{idea['id']}.</b> {idea['hook']}\n\n"
                    keyboard = []
                    row = []
                    for idea in ideas:
                        btn = {"text": f"{idea['id']}. {idea['hook'][:32]}...", "callback_data": f"idea_{idea['id']}"}
                        row.append(btn)
                        if len(row) == 2:
                            keyboard.append(row)
                            row = []
                    if row:
                        keyboard.append(row)
                    tg_send(msg[:4000], keyboard=keyboard)
                    state["mode"] = "selecting"
                    save_state(state)

                elif data in ACTION_INSTRUCTIONS:
                    instruction = ACTION_INSTRUCTIONS[data]
                    tg_send(f"🔄 Rewriting: <i>{instruction[:80]}...</i>")
                    idea = state.get("selected",{})
                    cache = load_cache()
                    result = generate_post(idea, cache.get("articles",[]), instruction)
                    state["last_post"] = result
                    save_state(state)

                    post = result["post"]
                    for chunk in [post[i:i+3800] for i in range(0, len(post), 3800)]:
                        tg_send(f"📝 <b>Revised:</b>\n\n{chunk}")

                    if result.get("infographic"):
                        html = build_infographic(result["infographic"], idea)
                        fpath = str(OUTPUT_DIR / f"infographic_r_{int(time.time())}.html")
                        Path(fpath).write_text(html)
                        tg_send_doc(fpath, "🖼️ Updated infographic")

                    keyboard = [
                        [{"text": "✅ Approve", "callback_data": "action_approve"},
                         {"text": "📊 More data", "callback_data": "action_data"}],
                        [{"text": "🎯 Sharper", "callback_data": "action_tone"},
                         {"text": "⬅️ Ideas", "callback_data": "action_back"}]
                    ]
                    tg_send("Better?", keyboard=keyboard)

            # ── MESSAGE: text commands ────────────────────────────────────
            elif "message" in update:
                msg = update["message"]
                text = msg.get("text","").strip()
                uid  = msg.get("from",{}).get("id")
                if uid != TELEGRAM_USER_ID:
                    continue

                if text in ["/morning", "/scrape", "/start"]:
                    daily_run()

                elif text == "/status":
                    cache = load_cache()
                    arts = cache.get("articles",[])
                    tiers = {}
                    for a in arts:
                        t = a.get("tier","?")
                        tiers[t] = tiers.get(t,0)+1
                    tg_send(
                        f"📊 <b>Bot Status</b>\n"
                        f"Last run: {cache.get('last_run','never')}\n"
                        f"Articles: {len(arts)}\n"
                        f"  📰 News: {tiers.get('news',0)}\n"
                        f"  🏛️ Official: {tiers.get('official_data',0)}\n"
                        f"  💬 Reddit: {tiers.get('social',0)}\n"
                        f"  🐦 X/LinkedIn: {tiers.get('social_indirect',0)}\n"
                        f"  🗣️ Forums: {tiers.get('forum',0)}\n"
                        f"Mode: {state.get('mode','idle')}\n"
                        f"Ideas loaded: {len(state.get('ideas',[]))}"
                    )

                elif text == "/help":
                    tg_send("""🏠 <b>GCC RE Intelligence Bot</b>

<b>Commands:</b>
/morning — Run today's scrape + ideas
/status — Bot status + article counts
/help — This message

<b>Sources covered:</b>
📰 20 RSS feeds (ArabianBusiness, Zawya, Gulf News, The National, TradeArabia, Google News...)
🏛️ Official: DLD/DubaiPulse, SAMA, GASTAT
💬 Reddit: r/dubai, r/saudiarabia, r/UAE, r/realestateinvesting + search
🐦 X/Twitter & LinkedIn via Google cache
🗣️ SkyscraperCity forums, PropertyFinder blog

<b>Post workflow:</b>
1. /morning → 10 ideas sent
2. Tap idea → full post + infographic generated
3. Adjust with buttons or type custom instructions
4. Approve → copy to LinkedIn

<b>Custom instructions:</b>
While reviewing a post, just type freely:
"make it shorter", "add more KSA data", "focus on developers not investors", etc.""")

                elif state.get("mode") == "reviewing" and len(text) > 8:
                    # Custom instruction
                    tg_send(f"✏️ Applying: <i>{text}</i>...")
                    idea = state.get("selected",{})
                    cache = load_cache()
                    result = generate_post(idea, cache.get("articles",[]), text)
                    state["last_post"] = result
                    save_state(state)

                    post = result["post"]
                    for chunk in [post[i:i+3800] for i in range(0, len(post), 3800)]:
                        tg_send(f"📝 <b>Revised:</b>\n\n{chunk}")

                    if result.get("infographic"):
                        html = build_infographic(result["infographic"], idea)
                        fpath = str(OUTPUT_DIR / f"infographic_custom_{int(time.time())}.html")
                        Path(fpath).write_text(html)
                        tg_send_doc(fpath, "🖼️ Updated infographic")

                    keyboard = [
                        [{"text": "✅ Approve", "callback_data": "action_approve"},
                         {"text": "⬅️ Ideas", "callback_data": "action_back"}]
                    ]
                    tg_send("How's that?", keyboard=keyboard)

        time.sleep(1)


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--scrape", action="store_true", help="Run morning scrape + send ideas")
    p.add_argument("--bot",    action="store_true", help="Run interactive bot loop")
    p.add_argument("--test",   action="store_true", help="Test all sources, print stats")
    args = p.parse_args()

    if args.scrape:
        daily_run()
    elif args.bot:
        run_bot()
    elif args.test:
        print("Testing all sources...")
        arts = run_full_scrape(test_mode=True)
        tiers = {}
        for a in arts:
            t = a.get("tier","?")
            tiers[t] = tiers.get(t,0)+1
        print(f"\n{'='*50}")
        print(f"TOTAL: {len(arts)} unique articles")
        print(f"By tier: {json.dumps(tiers, indent=2)}")
        print(f"\nSample titles per tier:")
        seen_tiers = set()
        for a in arts:
            t = a.get("tier","?")
            if t not in seen_tiers:
                seen_tiers.add(t)
                print(f"\n  [{t}] {a['source']}")
                print(f"  → {a['title'][:80]}")
    else:
        print(__doc__)
