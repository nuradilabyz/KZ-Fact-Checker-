"""
Factcheck.kz Scraper — Incremental Ingestion

Strategy:
1. Discover article URLs via Wayback CDX API (primary) or sitemap/RSS (fallback)
2. For each URL: check DB content_hash → skip if unchanged
3. Fetch via Wayback Machine (primary) → requests → Playwright (fallbacks)
4. Extract title, verdict, clean_text, published_at
5. Upsert article; explicitly delete old chunks if content changed
6. Chunk + embed new/changed articles
"""
import hashlib
import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from ingestion.chunker import chunk_text
from ingestion.embedder import embed_and_store_chunks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("factcheck_scraper")

# ── Configuration ────────────────────────────────────────────
import os
from dotenv import load_dotenv

load_dotenv()

SITEMAP_URL = os.getenv("FACTCHECK_SITEMAP_URL", "https://factcheck.kz/news-sitemap.xml")
RSS_URL = os.getenv("FACTCHECK_RSS_URL", "https://factcheck.kz/kaz/feed/")
KAZ_PREFIX = "/kaz/"

REQUEST_TIMEOUT = 30
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}


# ── Database helpers ─────────────────────────────────────────
import psycopg2
from psycopg2.extras import execute_values


def get_db_conn():
    """Create a database connection using env vars."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "factcheck"),
        user=os.getenv("POSTGRES_USER", "factcheck"),
        password=os.getenv("POSTGRES_PASSWORD", "changeme_secure_password"),
    )


def get_existing_hashes(conn) -> dict[str, str]:
    """Return {url: content_hash} for all existing articles."""
    with conn.cursor() as cur:
        cur.execute("SELECT url, content_hash FROM fact_articles")
        return {row[0]: row[1] for row in cur.fetchall()}


def upsert_article(conn, article: dict):
    """Insert or update an article. Explicitly delete old chunks if hash changed."""
    with conn.cursor() as cur:
        # Check if article exists
        cur.execute("SELECT content_hash FROM fact_articles WHERE url = %s", (article["url"],))
        row = cur.fetchone()

        if row:
            old_hash = row[0]
            if old_hash == article["content_hash"]:
                logger.info(f"  ⏭  Unchanged: {article['url']}")
                return False  # no change

            # Content changed → delete old chunks explicitly
            logger.info(f"  🔄 Content changed, deleting old chunks: {article['url']}")
            cur.execute("DELETE FROM fact_chunks WHERE article_url = %s", (article["url"],))

            # Update article
            cur.execute("""
                UPDATE fact_articles
                SET title = %s, published_at = %s, verdict_text = %s,
                    clean_text = %s, content_hash = %s, updated_at = now()
                WHERE url = %s
            """, (
                article["title"], article["published_at"], article["verdict_text"],
                article["clean_text"], article["content_hash"], article["url"],
            ))
        else:
            # New article
            cur.execute("""
                INSERT INTO fact_articles (url, title, published_at, verdict_text, clean_text, content_hash)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                article["url"], article["title"], article["published_at"],
                article["verdict_text"], article["clean_text"], article["content_hash"],
            ))

    conn.commit()
    return True  # new or changed


# ── URL Discovery ────────────────────────────────────────────

def fetch_urls_from_sitemap(sitemap_url: str) -> list[str]:
    """Parse news-sitemap.xml and return Kazakh article URLs."""
    urls = []
    try:
        resp = requests.get(sitemap_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        # Handle XML namespaces
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for url_elem in root.findall(".//sm:url/sm:loc", ns):
            url = url_elem.text.strip()
            if KAZ_PREFIX in url:
                urls.append(url)

        # Also try without namespace (some sitemaps don't use namespace)
        if not urls:
            for url_elem in root.iter():
                if url_elem.tag.endswith("loc") and url_elem.text:
                    url = url_elem.text.strip()
                    if KAZ_PREFIX in url:
                        urls.append(url)

        logger.info(f"Sitemap: found {len(urls)} Kazakh URLs")
    except Exception as e:
        logger.warning(f"Sitemap fetch failed: {e}")

    return urls


def fetch_urls_from_rss(rss_url: str) -> list[str]:
    """Parse RSS feed and return Kazakh article URLs."""
    urls = []
    try:
        resp = requests.get(rss_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        for item in root.findall(".//item/link"):
            if item.text:
                url = item.text.strip()
                if KAZ_PREFIX in url:
                    urls.append(url)

        logger.info(f"RSS: found {len(urls)} Kazakh URLs")
    except Exception as e:
        logger.warning(f"RSS fetch failed: {e}")

    return urls


def fetch_urls_from_site_pages(months_back: int = 2, max_pages: int = 15) -> list[str]:
    """
    Directly scrape factcheck.kz category listing pages to find article URLs.
    Paginates through /kaz/category/<cat>/page/N/ for each known category.
    Stops when it hits articles older than `months_back` months.
    """
    from datetime import datetime, timezone, timedelta
    from bs4 import BeautifulSoup

    cutoff = datetime.now(timezone.utc) - timedelta(days=months_back * 31)
    urls = []
    seen = set()

    # All known Kazakh categories on factcheck.kz
    CATEGORIES = [
        "zhanalyq", "faktchek", "alayaqtyq", "adam-quqyqtary",
        "economics-kaz", "basty-bet", "cifrly-quqyq", "gender-kaz",
        "texnologiya", "fejk-kaz",
    ]

    logger.info(
        f"Scraping factcheck.kz category pages directly (last {months_back} months, "
        f"cutoff: {cutoff.date()})..."
    )

    for category in CATEGORIES:
        cat_found = 0
        for page_num in range(1, max_pages + 1):
            if page_num == 1:
                page_url = f"https://factcheck.kz/kaz/category/{category}/"
            else:
                page_url = f"https://factcheck.kz/kaz/category/{category}/page/{page_num}/"

            try:
                resp = requests.get(page_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 404:
                    break
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"  Category {category} page {page_num} failed: {e}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")

            # Extract article links — article URLs have pattern /kaz/<category>/<slug>
            found_on_page = 0
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("/"):
                    href = "https://factcheck.kz" + href
                if "/kaz/" not in href or not _is_article_url(href):
                    continue
                normalized = href.rstrip("/")
                if normalized in seen:
                    continue
                seen.add(normalized)
                urls.append(href)
                found_on_page += 1
                cat_found += 1

            # Detect dates to know when to stop paginating this category
            dates_on_page = []
            for time_el in soup.find_all("time", datetime=True):
                try:
                    dt = datetime.fromisoformat(time_el["datetime"].replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    dates_on_page.append(dt)
                except Exception:
                    pass

            if dates_on_page:
                oldest = min(dates_on_page)
                logger.info(
                    f"  [{category}] page {page_num}: {found_on_page} links, "
                    f"oldest={oldest.date()}"
                )
                if oldest < cutoff:
                    logger.info(f"  [{category}] Reached cutoff, moving to next category")
                    break
            else:
                logger.info(f"  [{category}] page {page_num}: {found_on_page} links")
                if found_on_page == 0:
                    break  # no more articles in this category

            time.sleep(0.8)  # polite crawling

        logger.info(f"  [{category}] total: {cat_found} URLs")

    logger.info(f"Direct site scrape: found {len(urls)} article URLs total")
    return urls


def discover_urls() -> list[str]:
    """Discover article URLs: Wayback CDX (primary) → direct site pages → sitemap → RSS."""
    # Primary: Wayback CDX API
    urls = discover_urls_via_wayback()

    # Fallback 1: scrape listing pages directly
    if len(urls) < 5:
        logger.info("Wayback CDX returned few URLs, scraping site listing pages directly...")
        site_urls = fetch_urls_from_site_pages(months_back=2, max_pages=30)
        urls.extend(site_urls)

    # Fallback 2: sitemap + RSS (still useful for very recent articles)
    if len(urls) < 5:
        logger.info("Trying sitemap/RSS fallback...")
        sitemap_urls = fetch_urls_from_sitemap(SITEMAP_URL)
        rss_urls = fetch_urls_from_rss(RSS_URL)
        urls.extend(sitemap_urls)
        urls.extend(rss_urls)

    # Deduplicate preserving order
    seen = set()
    deduped = []
    for url in urls:
        normalized = url.rstrip("/")
        if normalized not in seen:
            seen.add(normalized)
            deduped.append(url)

    logger.info(f"Total unique Kazakh URLs discovered: {len(deduped)}")
    return deduped


# ── Wayback Machine CDX API ──────────────────────────────────

# URL patterns to EXCLUDE (non-article pages)
_EXCLUDE_PATTERNS = re.compile(
    r"/(page|tag|category|wp-content|wp-json|feed|author|comments|search|amp)/",
    re.IGNORECASE,
)


def _is_article_url(url: str) -> bool:
    """Return True if the URL looks like an actual article (not category/tag/page)."""
    path = urlparse(url).path
    if _EXCLUDE_PATTERNS.search(path):
        return False
    # Must have /kaz/ and at least one more path segment
    parts = [p for p in path.split("/") if p]
    return len(parts) >= 3 and parts[0] == "kaz"


def discover_urls_via_wayback(months_back: int = 2) -> list[str]:
    """
    Use the Wayback Machine CDX API to find archived factcheck.kz/kaz/* URLs.
    By default only fetches the last `months_back` months (default: 2).
    The collapse=urlkey parameter deduplicates by URL.
    """
    from datetime import datetime, timezone, timedelta
    import calendar

    now = datetime.now(timezone.utc)
    # Compute 'from' date: first day of (current month - months_back)
    from_dt = now - timedelta(days=months_back * 31)
    from_str = from_dt.strftime("%Y%m01000000")   # e.g. 20251201000000
    to_str   = now.strftime("%Y%m%d%H%M%S")       # e.g. 20260226135725

    cdx_url = (
        "https://web.archive.org/cdx/search/cdx"
        "?url=factcheck.kz/kaz/*"
        "&output=json"
        "&fl=original"
        "&collapse=urlkey"
        f"&from={from_str}"
        f"&to={to_str}"
        "&limit=5000"
    )
    logger.info(f"Discovering URLs via Wayback CDX API (from {from_str[:8]} to {to_str[:8]})...")

    try:
        resp = requests.get(cdx_url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        rows = resp.json()

        # First row is the header ["original"]
        if not rows or len(rows) < 2:
            logger.warning("CDX API returned no results")
            return []

        urls = []
        for row in rows[1:]:
            url = row[0]
            # Normalize: ensure https
            if url.startswith("http://"):
                url = "https://" + url[7:]
            if _is_article_url(url):
                urls.append(url)

        logger.info(f"Wayback CDX: found {len(urls)} article URLs")
        return urls

    except Exception as e:
        logger.warning(f"Wayback CDX discovery failed: {e}")
        return []


# ── Article Parsing ──────────────────────────────────────────

def extract_verdict(text: str) -> Optional[str]:
    """Extract verdict pattern like 'Үкім: Жалған' from article text."""
    patterns = [
        r"Үкім\s*:\s*(.+?)(?:\.|$)",
        r"Вердикт\s*:\s*(.+?)(?:\.|$)",
        r"ҮКІМ\s*:\s*(.+?)(?:\.|$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            verdict = match.group(0).strip().rstrip(".")
            return verdict
    return None


def parse_article_html(html: str, url: str) -> Optional[dict]:
    """Parse article HTML and extract structured data."""
    soup = BeautifulSoup(html, "lxml")

    # Title
    title_tag = soup.find("h1")
    if not title_tag:
        title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else "Untitled"

    # Published date
    published_at = None
    time_tag = soup.find("time")
    if time_tag and time_tag.get("datetime"):
        try:
            published_at = datetime.fromisoformat(
                time_tag["datetime"].replace("Z", "+00:00")
            )
        except ValueError:
            pass

    # Also try meta tags
    if not published_at:
        meta = soup.find("meta", {"property": "article:published_time"})
        if meta and meta.get("content"):
            try:
                published_at = datetime.fromisoformat(
                    meta["content"].replace("Z", "+00:00")
                )
            except ValueError:
                pass

    # Clean text — article body
    # Try common WordPress article selectors
    article_body = (
        soup.find("div", class_="entry-content")
        or soup.find("article")
        or soup.find("div", class_="post-content")
        or soup.find("div", class_="td-post-content")
        or soup.find("main")
    )

    if article_body:
        # Remove script/style tags
        for tag in article_body.find_all(["script", "style", "nav", "footer", "aside"]):
            tag.decompose()
        paragraphs = article_body.find_all(["p", "li", "h2", "h3", "h4", "blockquote"])
        clean_text = "\n".join(
            p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)
        )
    else:
        # Fallback: grab all paragraphs
        paragraphs = soup.find_all("p")
        clean_text = "\n".join(
            p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)
        )

    if not clean_text or len(clean_text) < 50:
        logger.warning(f"  ⚠  Too little text for {url}")
        return None

    # Verdict
    verdict_text = extract_verdict(clean_text)

    # Content hash
    content_hash = hashlib.sha256(clean_text.encode("utf-8")).hexdigest()

    return {
        "url": url,
        "title": title,
        "published_at": published_at,
        "verdict_text": verdict_text,
        "clean_text": clean_text,
        "content_hash": content_hash,
    }


def _is_real_content(html: str) -> bool:
    """Check if HTML is actual article content, not a Cloudflare challenge page."""
    cf_markers = [
        "security service to protect",
        "Checking if the site connection is secure",
        "cf-browser-verification",
        "challenge-platform",
        "Just a moment...",
    ]
    for marker in cf_markers:
        if marker in html:
            return False
    # Must have some real content
    return "<p>" in html or "<article" in html


def fetch_article(url: str) -> Optional[str]:
    """
    Fetch article HTML.
    Strategy: Wayback Machine (best) → requests → Playwright fallback.
    Cloudflare blocks direct requests, so Wayback is tried first.
    """
    # ── Attempt 1: Wayback Machine (bypasses Cloudflare) ──
    html = fetch_from_wayback(url)
    if html and _is_real_content(html):
        return html

    # ── Attempt 2: plain requests (in case Cloudflare is down) ──
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200 and len(resp.text) > 500 and _is_real_content(resp.text):
            logger.debug(f"  ✅ Fetched via requests: {url}")
            return resp.text
    except Exception:
        pass

    # ── Attempt 3: Playwright fallback ──
    logger.info(f"  ⚠  Wayback + requests failed, trying Playwright...")
    html = fetch_with_playwright(url)
    if html and _is_real_content(html):
        return html

    logger.error(f"  ❌ All fetch methods failed for: {url}")
    return None


FETCH_DELAY = float(os.getenv("FETCH_DELAY_SECONDS", "1.5"))  # Rate limit


def fetch_from_wayback(url: str) -> Optional[str]:
    """Fetch article from Internet Archive Wayback Machine."""
    wayback_url = f"https://web.archive.org/web/2024/{url}"
    try:
        resp = requests.get(wayback_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200 and len(resp.text) > 500:
            logger.info(f"  ✅ Fetched via Wayback Machine: {url}")
            return resp.text
        logger.info(f"  ⚠  Wayback returned {resp.status_code}")
    except Exception as e:
        logger.info(f"  ⚠  Wayback failed: {e}")

    # Try different years
    for year in ["2025", "2023", "2022"]:
        try:
            time.sleep(0.5)
            wayback_url = f"https://web.archive.org/web/{year}/{url}"
            resp = requests.get(wayback_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200 and len(resp.text) > 500:
                logger.info(f"  ✅ Fetched via Wayback Machine ({year}): {url}")
                return resp.text
        except Exception:
            pass

    return None


def fetch_with_playwright(url: str) -> Optional[str]:
    """Fallback: use Playwright to handle Cloudflare JS challenge."""
    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=USER_AGENT)
            page = context.new_page()

            page.goto(url, wait_until="networkidle", timeout=45000)
            # Wait a bit extra for Cloudflare challenge to resolve
            page.wait_for_timeout(5000)

            html = page.content()
            browser.close()

            if len(html) > 500:
                logger.debug(f"  ✅ Fetched via Playwright: {url}")
                return html
            else:
                logger.warning(f"  ❌ Playwright returned too little for: {url}")
                return None

    except Exception as e:
        logger.error(f"  ❌ Playwright failed for {url}: {e}")
        return None


# ── Main Pipeline ────────────────────────────────────────────

def load_urls_from_file(filepath: str) -> list[str]:
    """Load article URLs from a text file (one URL per line)."""
    urls = []
    try:
        with open(filepath, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and line.startswith("http"):
                    urls.append(line)
        logger.info(f"Loaded {len(urls)} URLs from {filepath}")
    except Exception as e:
        logger.error(f"Failed to load URLs from {filepath}: {e}")
    return urls


def run_ingestion(limit: int = 0, urls_file: str = None):
    """
    Full ingestion pipeline:
    1. Discover URLs (sitemap/RSS or from file)
    2. Filter new/changed
    3. Scrape → parse → upsert article
    4. Chunk → embed → store chunks
    """
    conn = get_db_conn()
    existing_hashes = get_existing_hashes(conn)

    # URL discovery: file first, then sitemap/RSS
    if urls_file:
        urls = load_urls_from_file(urls_file)
    else:
        urls = discover_urls()

    if limit > 0:
        urls = urls[:limit]

    stats = {"total": len(urls), "new": 0, "updated": 0, "skipped": 0, "errors": 0}

    for i, url in enumerate(urls, 1):
        logger.info(f"[{i}/{len(urls)}] Processing: {url}")

        # Rate limiting
        if i > 1:
            time.sleep(FETCH_DELAY)

        # Quick skip check
        normalized_url = url.rstrip("/")
        existing_hash = existing_hashes.get(url) or existing_hashes.get(normalized_url)

        # Fetch HTML
        html = fetch_article(url)
        if not html:
            stats["errors"] += 1
            continue

        # Parse
        article = parse_article_html(html, url)
        if not article:
            stats["errors"] += 1
            continue

        # Quick skip: if hash matches, no need to upsert
        if existing_hash and existing_hash == article["content_hash"]:
            logger.info(f"  ⏭  Unchanged (hash match): {url}")
            stats["skipped"] += 1
            continue

        # Upsert article
        is_new_or_changed = upsert_article(conn, article)
        if not is_new_or_changed:
            stats["skipped"] += 1
            continue

        # Chunk text
        chunks = chunk_text(article["clean_text"])
        logger.info(f"  📄 {len(chunks)} chunks")

        # Embed and store
        try:
            embed_and_store_chunks(conn, article["url"], chunks)
            if existing_hash:
                stats["updated"] += 1
            else:
                stats["new"] += 1
        except Exception as e:
            logger.error(f"  ❌ Embedding failed for {url}: {e}")
            stats["errors"] += 1

    conn.close()

    logger.info(
        f"\n{'='*60}\n"
        f"Ingestion Complete:\n"
        f"  Total URLs:   {stats['total']}\n"
        f"  New articles: {stats['new']}\n"
        f"  Updated:      {stats['updated']}\n"
        f"  Skipped:      {stats['skipped']}\n"
        f"  Errors:       {stats['errors']}\n"
        f"{'='*60}"
    )
    return stats


# ── CLI Entry Point ──────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Factcheck.kz Kazakh content scraper")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of URLs to process (0=all)")
    parser.add_argument("--urls-file", type=str, default=None,
                        help="Path to a text file with article URLs (one per line)")
    args = parser.parse_args()

    run_ingestion(limit=args.limit, urls_file=args.urls_file)
