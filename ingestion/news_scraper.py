"""
Multi-Source News Scraper — Unified ingestion for 5 Kazakh news sites.

Sources (reference / knowledge base):
  - factcheck.kz   — Fact-checking articles (Kazakh)
  - azattyq.org     — Radio Azattyq / RFE/RL Kazakh Service
  - informburo.kz   — Informburo news portal
  - tengrinews.kz   — Tengrinews Kazakh section

Verification target:
  - ztb.kz          — ZTB News (claims extracted for fact-checking)

Pipeline per source:
  1. Discover new URLs (RSS feed → category pages fallback)
  2. Filter against existing DB hashes (skip unchanged)
  3. Fetch & parse HTML → extract title, clean_text, author, published_at
  4. Chunk text → generate embeddings → store in knowledge_chunks
  5. (ZTB only) Extract claims via GPT → verify against knowledge base
"""

import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree

import psycopg2
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from psycopg2.extras import Json

load_dotenv()

logger = logging.getLogger("news_scraper")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# ── Config ───────────────────────────────────────────────────

REQUEST_TIMEOUT = 30
FETCH_DELAY = float(os.getenv("FETCH_DELAY_SECONDS", "1.5"))
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}

# ── Source Definitions ───────────────────────────────────────

SOURCES = {
    "factcheck": {
        "base_url": "https://factcheck.kz",
        "rss": "https://factcheck.kz/kaz/feed/",
        "categories": [
            "/kaz/category/faktchek/", "/kaz/category/zhanalyq/",
            "/kaz/category/claim-checking/",
        ],
        "article_selector": "div.entry-content, div.post-content, article .entry-content",
        "title_selector": "h1.entry-title, h1.post-title, h1",
        "date_selector": "time[datetime]",
        "is_reference": True,
    },
    "azattyq": {
        "base_url": "https://www.azattyq.org",
        "rss": None,  # azattyq uses API, we'll scrape listing pages
        "categories": ["/z/330", "/z/331"],  # Жаңалықтар, Бас тақырып
        "article_selector": "#article-content, .body-container .wsw",
        "title_selector": "h1",
        "date_selector": "time[datetime]",
        "is_reference": True,
    },
    "informburo": {
        "base_url": "https://informburo.kz",
        "rss": None,  # feed returns 404
        "categories": ["/novosti", "/stati"],
        "article_selector": "div.article__body, div.content-body, div.article-body",
        "title_selector": "h1",
        "date_selector": "time[datetime], meta[property='article:published_time']",
        "is_reference": True,
    },
    "tengrinews": {
        "base_url": "https://tengrinews.kz",
        "rss": "https://tengrinews.kz/rss/",
        "categories": ["/kazakh/", "/kazakhstan_news/"],
        "article_selector": "div.content_main_text, div.entry-content, div.article-text",
        "title_selector": "h1",
        "date_selector": "time[datetime], .date-time",
        "is_reference": True,
    },
    "ztb": {
        "base_url": "https://ztb.kz",
        "rss": None,
        "categories": ["/novosti-kazaxstana", "/novosti-mira", "/ru"],
        "article_selector": "div.article-body, div.content-body, div.entry-content, article",
        "title_selector": "h1",
        "date_selector": "time[datetime], meta[property='article:published_time']",
        "is_reference": False,  # verification target
    },
}

# ── Database ─────────────────────────────────────────────────

def get_db_conn():
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return psycopg2.connect(dsn=database_url)
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "factcheck"),
        user=os.getenv("POSTGRES_USER", "factcheck"),
        password=os.getenv("POSTGRES_PASSWORD", "changeme_secure_password"),
    )


def get_existing_hashes(conn, source: str) -> dict:
    """Return {url: content_hash} for all existing articles of a source."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT url, content_hash FROM source_articles WHERE source = %s",
            (source,),
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def get_articles_missing_date(conn, source: str) -> set:
    """Return set of URLs for articles that have no published_at date."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT url FROM source_articles WHERE source = %s AND published_at IS NULL",
            (source,),
        )
        return {row[0] for row in cur.fetchall()}


def upsert_article(conn, article: dict):
    """Insert or update an article. Delete old chunks if hash changed."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO source_articles (url, source, title, author, published_at,
                                         clean_text, content_hash, verdict_label)
            VALUES (%(url)s, %(source)s, %(title)s, %(author)s, %(published_at)s,
                    %(clean_text)s, %(content_hash)s, %(verdict_label)s)
            ON CONFLICT (url) DO UPDATE SET
                title = EXCLUDED.title,
                clean_text = EXCLUDED.clean_text,
                content_hash = EXCLUDED.content_hash,
                verdict_label = EXCLUDED.verdict_label,
                updated_at = now()
            """,
            article,
        )
        # Delete old chunks when content changed (they'll be regenerated)
        cur.execute(
            "DELETE FROM knowledge_chunks WHERE article_url = %s",
            (article["url"],),
        )
    conn.commit()


# ── URL Discovery ────────────────────────────────────────────

def discover_urls_rss(rss_url: str, max_items: int = 200) -> list[str]:
    """Parse RSS feed and return article URLs."""
    urls = []
    try:
        resp = requests.get(rss_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        root = ElementTree.fromstring(resp.content)

        for item in root.iter("item"):
            link = item.find("link")
            if link is not None and link.text:
                urls.append(link.text.strip())

        # Also check Atom format
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.iter("{http://www.w3.org/2005/Atom}entry"):
            link_el = entry.find("atom:link[@rel='alternate']", ns) or entry.find("{http://www.w3.org/2005/Atom}link")
            if link_el is not None:
                href = link_el.get("href", "")
                if href:
                    urls.append(href.strip())

        logger.info(f"  RSS {rss_url}: found {len(urls)} URLs")
    except Exception as e:
        logger.warning(f"  RSS {rss_url} failed: {e}")
    return urls[:max_items]


def discover_urls_pages(source_key: str, months_back: int = 1, max_pages: int = 50) -> list[str]:
    """Scrape category/listing pages to discover article URLs."""
    cfg = SOURCES[source_key]
    base = cfg["base_url"]
    cutoff = datetime.now(timezone.utc) - timedelta(days=months_back * 31)
    urls = []
    seen = set()

    for cat_path in cfg.get("categories", []):
        for page_num in range(1, max_pages + 1):
            if page_num == 1:
                page_url = f"{base}{cat_path}"
            else:
                # Different pagination patterns per site
                if source_key == "factcheck":
                    page_url = f"{base}{cat_path}page/{page_num}/"
                elif source_key == "azattyq":
                    page_url = f"{base}{cat_path}?p={page_num}"
                elif source_key == "informburo":
                    page_url = f"{base}{cat_path}?page={page_num}"
                elif source_key == "tengrinews":
                    page_url = f"{base}{cat_path}page/{page_num}/"
                elif source_key == "ztb":
                    page_url = f"{base}{cat_path}?page={page_num}"
                else:
                    page_url = f"{base}{cat_path}?page={page_num}"

            try:
                resp = requests.get(page_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 404:
                    break
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"  [{source_key}] page {page_url} failed: {e}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            found = 0

            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                if href.startswith("/"):
                    href = base + href

                if not href.startswith(base):
                    continue
                if not _is_article_url(href, source_key):
                    continue

                normalized = href.split("?")[0].split("#")[0].rstrip("/")
                if normalized not in seen:
                    seen.add(normalized)
                    urls.append(normalized)
                    found += 1

            # Check dates to stop early
            dates = _extract_dates_from_page(soup)
            if dates and min(dates) < cutoff:
                logger.info(f"  [{source_key}] {cat_path} page {page_num}: {found} links, reached cutoff")
                break

            logger.info(f"  [{source_key}] {cat_path} page {page_num}: {found} links")
            if found == 0:
                break

            time.sleep(0.8)

    logger.info(f"  [{source_key}] page discovery total: {len(urls)} URLs")
    return urls


def _is_article_url(url: str, source_key: str) -> bool:
    """Basic heuristic: article URLs have enough path depth and no admin patterns."""
    exclude = re.compile(
        r"/(page|tag|category|wp-content|wp-json|feed|author|comments|search|amp|rss|login|register|api)/",
        re.IGNORECASE,
    )
    if exclude.search(url):
        return False

    path = url.split("//", 1)[-1].split("/", 1)[-1] if "//" in url else url
    parts = [p for p in path.split("/") if p]

    if source_key == "factcheck":
        return "/kaz/" in url and len(parts) >= 2  # Kazakh articles only
    elif source_key == "azattyq":
        return "/a/" in url  # azattyq uses /a/{slug}/{id}.html
    elif source_key == "informburo":
        return len(parts) >= 2 and parts[0] in ("novosti", "stati", "interview", "mneniya", "cards")
    elif source_key == "tengrinews":
        return len(parts) >= 2 and not url.endswith("/kazakh/")
    elif source_key == "ztb":
        # Accept both /ru/slug and /novosti-*/slug style URLs
        return ("/ru/" in url or "/novosti-" in url) and len(parts) >= 2
    return len(parts) >= 2


def _extract_dates_from_page(soup) -> list[datetime]:
    """Extract dates from <time datetime=...> tags on a listing page."""
    dates = []
    for time_el in soup.find_all("time", datetime=True):
        try:
            dt = datetime.fromisoformat(time_el["datetime"].replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dates.append(dt)
        except Exception:
            pass
    return dates


def discover_urls(source_key: str, months_back: int = 1) -> list[str]:
    """Discover article URLs for a source: RSS + page scraping (always both)."""
    cfg = SOURCES[source_key]
    urls = []

    # Try RSS first
    if cfg.get("rss"):
        urls = discover_urls_rss(cfg["rss"])

    # ALWAYS supplement with page scraping for fuller coverage
    # On hourly runs, most articles are already known — use shallow crawl
    existing_count = len(urls)
    incremental_pages = 5  # shallow: only check recent pages for new articles
    logger.info(f"[{source_key}] RSS gave {len(urls)} URLs, also running page discovery (max {incremental_pages} pages)...")
    page_urls = discover_urls_pages(source_key, months_back=months_back, max_pages=incremental_pages)
    existing = set(urls)
    for u in page_urls:
        if u not in existing:
            urls.append(u)

    # Deduplicate
    seen = set()
    deduped = []
    for u in urls:
        norm = u.split("?")[0].split("#")[0].rstrip("/")
        if norm not in seen:
            seen.add(norm)
            deduped.append(u)

    logger.info(f"[{source_key}] Total discovered: {len(deduped)} unique URLs")
    return deduped


# ── Article Parsing ──────────────────────────────────────────

def fetch_article_html(url: str) -> str | None:
    """Fetch article HTML. Tries direct request, then Wayback Machine."""
    # Direct request
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200 and len(resp.text) > 1000:
            return resp.text
    except Exception as e:
        logger.debug(f"  Direct fetch failed for {url}: {e}")

    # Wayback Machine fallback
    try:
        wb_url = f"https://web.archive.org/web/2024/{url}"
        resp = requests.get(wb_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200 and len(resp.text) > 1000:
            return resp.text
    except Exception:
        pass

    logger.warning(f"  Could not fetch: {url}")
    return None


_RU_MONTHS = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
    "мая": 5, "июня": 6, "июля": 7, "августа": 8,
    "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}


def _parse_ztb_russian_date(soup) -> datetime | None:
    """Parse ZTB.kz dates like '5 января, 10:15' or '31 декабря, 13:41' from page text."""
    page_text = soup.get_text(" ", strip=True)
    # Pattern: day month_name, HH:MM
    pattern = r"(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)[,\s]+(\d{1,2}):(\d{2})"
    match = re.search(pattern, page_text, re.IGNORECASE)
    if not match:
        return None
    try:
        day = int(match.group(1))
        month = _RU_MONTHS.get(match.group(2).lower())
        hour = int(match.group(3))
        minute = int(match.group(4))
        if not month:
            return None
        # Use current year; if the resulting date is in the future, use previous year
        now = datetime.now(timezone.utc)
        year = now.year
        dt = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
        if dt > now:
            dt = datetime(year - 1, month, day, hour, minute, tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def parse_article(html: str, url: str, source_key: str) -> dict | None:
    """Parse article HTML using source-specific selectors."""
    cfg = SOURCES[source_key]
    soup = BeautifulSoup(html, "html.parser")

    # Title
    title = None
    for sel in cfg["title_selector"].split(", "):
        el = soup.select_one(sel.strip())
        if el:
            title = el.get_text(strip=True)
            break
    if not title:
        title_tag = soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else "Untitled"

    # Published date
    published_at = None
    for sel in cfg["date_selector"].split(", "):
        el = soup.select_one(sel.strip())
        if el:
            dt_str = el.get("datetime") or el.get("content") or el.get_text(strip=True)
            if dt_str:
                try:
                    published_at = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                    break
                except Exception:
                    pass

    # ZTB-specific: dates are in Russian text like "5 января, 10:15"
    if not published_at and source_key == "ztb":
        published_at = _parse_ztb_russian_date(soup)

    # Body text
    body_text = ""
    for sel in cfg["article_selector"].split(", "):
        el = soup.select_one(sel.strip())
        if el:
            # Remove scripts, styles, nav
            for tag in el.find_all(["script", "style", "nav", "aside", "footer"]):
                tag.decompose()
            body_text = el.get_text(separator="\n", strip=True)
            break

    # Fallback: get all paragraph text from article tag
    if len(body_text) < 100:
        article_el = soup.find("article") or soup.find("main") or soup.find("body")
        if article_el:
            paragraphs = article_el.find_all("p")
            body_text = "\n".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20)

    if len(body_text) < 50:
        logger.warning(f"  Insufficient content at {url} ({len(body_text)} chars)")
        return None

    # Author
    author = None
    for meta_name in ["author", "article:author"]:
        meta = soup.find("meta", attrs={"name": meta_name}) or soup.find("meta", property=meta_name)
        if meta and meta.get("content"):
            author = meta["content"]
            break

    # Verdict (factcheck.kz only)
    verdict_label = None
    if source_key == "factcheck":
        verdict_label = _extract_factcheck_verdict(body_text)

    content_hash = hashlib.md5(body_text.encode("utf-8")).hexdigest()

    return {
        "url": url,
        "source": source_key,
        "title": title[:500] if title else "Untitled",
        "author": author,
        "published_at": published_at,
        "clean_text": body_text,
        "content_hash": content_hash,
        "verdict_label": verdict_label,
    }


def _extract_factcheck_verdict(text: str) -> str | None:
    """Extract verdict pattern like 'Жалған' from Factcheck.kz text."""
    patterns = [
        r"[Үү]к[іi]м[:\s]+(.+?)(?:\n|\.|$)",
        r"(Жалған|Расталды|Жартылай жалған|Шындық|Манипуляция|Сатира|Жаңылыс)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()[:100]
    return None


# ── Chunking & Embedding ─────────────────────────────────────

def chunk_and_embed(conn, article_url: str, source: str, text: str):
    """Chunk text, generate embeddings, store in knowledge_chunks."""
    from ingestion.chunker import chunk_text
    from ingestion.embedder import compute_embeddings

    chunks = chunk_text(text)
    if not chunks:
        return 0

    texts = [c["chunk_text"] for c in chunks]
    embeddings = compute_embeddings(texts)

    stored = 0
    with conn.cursor() as cur:
        for chunk, embedding in zip(chunks, embeddings):
            cur.execute(
                """
                INSERT INTO knowledge_chunks
                    (article_url, source, chunk_text, embedding, chunk_hash)
                VALUES (%s, %s, %s, %s::vector, %s)
                ON CONFLICT (article_url, chunk_hash) DO NOTHING
                """,
                (
                    article_url,
                    source,
                    chunk["chunk_text"],
                    str(embedding),
                    chunk["chunk_hash"],
                ),
            )
            stored += 1
    conn.commit()
    logger.info(f"  ✅ Stored {stored} chunks for {article_url}")
    return stored


# ── ZTB Claim Extraction & Verification ──────────────────────

def extract_claims_from_text(text: str) -> list[str]:
    """Use LLM to extract atomic checkable claims from article text."""
    import openai

    client = openai.OpenAI(
        api_key=os.getenv("OPENAI_API_KEY"),
        base_url=os.getenv("LLM_BASE_URL", "https://llm.alem.ai/v1"),
    )

    try:
        resp = client.chat.completions.create(
            model=os.getenv("LLM_MODEL", "kazllm"),
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Сен мәтіннен тексерілетін фактілерді (claim) шығарып алатын жүйесің.\n"
                        "Берілген мәтіннен тек нақты фактілік мәлімдемелерді JSON массиві ретінде қайтар.\n"
                        "Пікірлер емес, тек фактілерді шығар. Максимум 5 claim.\n"
                        'Формат: ["claim1", "claim2", ...]'
                    ),
                },
                {"role": "user", "content": text[:3000]},
            ],
            temperature=0.1,
            max_completion_tokens=500,
        )
        raw = resp.choices[0].message.content.strip()
        claims = json.loads(raw)
        if isinstance(claims, list):
            return [c for c in claims if isinstance(c, str) and len(c) > 10]
    except Exception as e:
        logger.error(f"  Claim extraction failed: {e}")
    return []


def verify_claim_against_kb(claim_text: str) -> dict | None:
    """Verify a claim using the /check API endpoint."""
    api_url = os.getenv("API_URL", "http://api:8000")
    try:
        resp = requests.post(
            f"{api_url}/check",
            json={"claim": claim_text, "top_k": 5, "similarity_threshold": 0.6},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"  Verification API error: {e}")
        return None


# ── Main Pipeline ────────────────────────────────────────────

def run_source_ingestion(source_key: str, months_back: int = 1, limit: int = 0):
    """
    Full ingestion pipeline for a single source:
    1. Discover URLs
    2. Filter new/changed
    3. Fetch → parse → upsert article
    4. Chunk → embed → store (reference sources only)
    5. (ZTB) Extract claims → verify
    """
    cfg = SOURCES[source_key]
    is_reference = cfg["is_reference"]

    logger.info(f"{'='*60}")
    logger.info(f"Starting ingestion for [{source_key}] (reference={is_reference})")
    logger.info(f"{'='*60}")

    conn = get_db_conn()
    existing = get_existing_hashes(conn, source_key)
    missing_dates = get_articles_missing_date(conn, source_key)

    # 1. Discover URLs
    urls = discover_urls(source_key, months_back=months_back)
    if limit > 0:
        urls = urls[:limit]

    stats = {"discovered": len(urls), "new": 0, "updated": 0, "skipped": 0,
             "chunks": 0, "claims_extracted": 0, "claims_verified": 0, "errors": 0,
             "dates_fixed": 0}

    for i, url in enumerate(urls):
        # If URL exists but is missing published_at, re-fetch to extract date
        if url in existing and url in missing_dates:
            try:
                html = fetch_article_html(url)
                if html:
                    article = parse_article(html, url, source_key)
                    if article and article.get("published_at"):
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE source_articles SET published_at = %s, updated_at = now() WHERE url = %s",
                                (article["published_at"], url),
                            )
                        conn.commit()
                        stats["dates_fixed"] += 1
                        logger.info(f"  [{source_key}] Fixed date for: {url[:60]}")
                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"  [{source_key}] Date fix failed for {url[:60]}: {e}")
            stats["skipped"] += 1
            continue

        # Quick skip: if URL already exists, skip entirely
        if url in existing:
            stats["skipped"] += 1
            continue

        logger.info(f"  [{source_key}] ({i+1}/{len(urls)}) NEW: {url[:80]}")

        try:
            # 2. Fetch HTML
            html = fetch_article_html(url)
            if not html:
                stats["errors"] += 1
                continue

            # 3. Parse
            article = parse_article(html, url, source_key)
            if not article:
                stats["errors"] += 1
                continue

            # 4. Check if unchanged
            old_hash = existing.get(url)
            if old_hash == article["content_hash"]:
                stats["skipped"] += 1
                continue

            # 5. Upsert article
            upsert_article(conn, article)
            if old_hash:
                stats["updated"] += 1
            else:
                stats["new"] += 1

            # 6. Chunk & embed (reference sources only)
            if is_reference:
                n_chunks = chunk_and_embed(conn, url, source_key, article["clean_text"])
                stats["chunks"] += n_chunks

            # 7. ZTB: extract claims and verify
            if source_key == "ztb" and not old_hash:
                claims = extract_claims_from_text(article["clean_text"])
                stats["claims_extracted"] += len(claims)

                for claim_text in claims:
                    try:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO ztb_claims (article_url, claim_text)
                                VALUES (%s, %s)
                                ON CONFLICT (article_url, claim_text) DO NOTHING
                                RETURNING claim_id
                                """,
                                (url, claim_text),
                            )
                            row = cur.fetchone()
                            if not row:
                                continue
                            claim_id = row[0]
                        conn.commit()

                        result = verify_claim_against_kb(claim_text)
                        if result:
                            best = result.get("best_match", {})
                            with conn.cursor() as cur:
                                cur.execute(
                                    """
                                    INSERT INTO verifications
                                        (claim_id, best_article_url, best_source,
                                         similarity_score, verdict, explanation_kk, raw_response)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (claim_id) DO NOTHING
                                    """,
                                    (
                                        claim_id,
                                        best.get("url"),
                                        best.get("source"),
                                        best.get("similarity_score", 0.0),
                                        result.get("verdict", "NOT_ENOUGH_INFO"),
                                        result.get("explanation_kk", ""),
                                        Json(result, dumps=lambda o: json.dumps(o, ensure_ascii=False)),
                                    ),
                                )
                            conn.commit()
                            stats["claims_verified"] += 1
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"  Claim processing error: {e}")

            time.sleep(FETCH_DELAY)

        except Exception as e:
            conn.rollback()
            stats["errors"] += 1
            logger.error(f"  Error processing {url}: {e}")

    conn.close()
    logger.info(f"[{source_key}] Ingestion complete. Stats: {stats}")
    return stats


def run_all_sources(months_back: int = 1, limit: int = 0):
    """Run ingestion for all sources sequentially."""
    all_stats = {}
    # Reference sources first, then ZTB
    order = ["factcheck", "azattyq", "informburo", "tengrinews", "ztb"]
    for source_key in order:
        try:
            stats = run_source_ingestion(source_key, months_back=months_back, limit=limit)
            all_stats[source_key] = stats
        except Exception as e:
            logger.error(f"Source [{source_key}] failed entirely: {e}")
            all_stats[source_key] = {"error": str(e)}
    return all_stats


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Multi-source news scraper")
    parser.add_argument("--source", type=str, default="all",
                        choices=["all"] + list(SOURCES.keys()),
                        help="Source to scrape (default: all)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max articles per source (0 = unlimited)")
    parser.add_argument("--months", type=int, default=1,
                        help="How many months back to look (default: 1)")
    args = parser.parse_args()

    if args.source == "all":
        run_all_sources(months_back=args.months, limit=args.limit)
    else:
        run_source_ingestion(args.source, months_back=args.months, limit=args.limit)
