"""
Database helpers — connection pool and vector search queries.
Updated for multi-source pipeline (knowledge_chunks / source_articles).
"""
import os
from contextlib import contextmanager
from urllib.parse import urlsplit, urlunsplit

import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv

load_dotenv()

_pool = None


def _db_kwargs() -> dict:
    """Return connection kwargs: DATABASE_URL if set, else individual POSTGRES_* vars."""
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return {"dsn": database_url}
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "dbname": os.getenv("POSTGRES_DB", "factcheck"),
        "user": os.getenv("POSTGRES_USER", "factcheck"),
        "password": os.getenv("POSTGRES_PASSWORD", "changeme_secure_password"),
    }


def get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        kwargs = _db_kwargs()
        _pool = ThreadedConnectionPool(minconn=2, maxconn=10, **kwargs)
    return _pool


@contextmanager
def get_conn():
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


def get_db_connection():
    """Direct un-pooled connection for ad-hoc queries."""
    return psycopg2.connect(**_db_kwargs())


def text_search(
    query: str,
    similarity_threshold: float = 0.05,
    top_k: int = 5,
) -> list[dict]:
    """
    PostgreSQL full-text search on knowledge_chunks.
    Score = fraction of query words that actually appear in the chunk.
    """
    def _normalized_article_key(url: str, source: str, title: str) -> str:
        if url:
            parts = urlsplit(url)
            norm_path = parts.path.rstrip("/") or "/"
            norm_url = urlunsplit((parts.scheme.lower(), parts.netloc.lower(), norm_path, "", ""))
            return norm_url
        return f"{(source or '').strip().lower()}::{(title or '').strip().lower()}"

    import re
    # Stop words to ignore for overlap calculation
    STOP_WORDS = {
        "млрд", "миллиард", "миллион", "млн", "тыс", "тысяч", "тенге",
        "процент", "процентов", "год", "года", "лет", "месяц",
        "доллар", "долларов", "евро", "рубль", "рублей",
        "был", "была", "были", "это", "этот", "эта", "эти",
        "что", "как", "где", "когда", "который", "которая", "которые",
        "the", "is", "of", "in", "to", "and", "for", "with",
        "бар", "емес", "болды", "болған",
    }
    all_words = [w.lower() for w in re.findall(r"\w+", query, flags=re.UNICODE) if len(w) > 2]
    meaningful = [w for w in all_words if w not in STOP_WORDS]
    if len(meaningful) < 2:
        return []
    words = meaningful

    tsquery = " | ".join(all_words)
    total_words = len(meaningful)

    sql = """
        SELECT
            kc.chunk_id,
            kc.article_url,
            kc.chunk_text,
            kc.source,
            ts_rank_cd(to_tsvector('simple', kc.chunk_text), to_tsquery('simple', %s)) AS rank,
            sa.title,
            sa.published_at,
            sa.verdict_label
        FROM knowledge_chunks kc
        JOIN source_articles sa ON sa.url = kc.article_url
        WHERE to_tsvector('simple', kc.chunk_text) @@ to_tsquery('simple', %s)
        ORDER BY rank DESC
        LIMIT %s
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (tsquery, tsquery, top_k * 10))
            rows = cur.fetchall()

    if not rows:
        return []

    deduped_results = {}
    for row in rows:
        chunk_text_lower = (row[2] or "").lower()
        title_lower = (row[5] or "").lower()
        combined = chunk_text_lower + " " + title_lower

        # Count how many MEANINGFUL words appear in the chunk
        matched_words = sum(1 for w in meaningful if w in combined)
        word_overlap = matched_words / total_words

        # Stricter overlap: meaningful words only
        if total_words <= 3:
            min_overlap = 1.0
        elif total_words <= 5:
            min_overlap = 0.85
        else:
            min_overlap = 0.7

        if word_overlap < min_overlap:
            continue

        # Score = word overlap (0..1) — clean & meaningful
        if word_overlap >= similarity_threshold:
            candidate = {
                "chunk_id": row[0],
                "url": row[1],
                "snippet": row[2],
                "source": row[3],
                "similarity_score": round(word_overlap, 4),
                "title": row[5],
                "published_at": row[6].isoformat() if row[6] else None,
                "source_verdict": row[7],
            }
            article_key = _normalized_article_key(row[1], row[3], row[5])
            existing = deduped_results.get(article_key)
            if existing is None or candidate["similarity_score"] > existing["similarity_score"]:
                deduped_results[article_key] = candidate

    results = sorted(
        deduped_results.values(),
        key=lambda item: item["similarity_score"],
        reverse=True,
    )
    return results[:top_k]


