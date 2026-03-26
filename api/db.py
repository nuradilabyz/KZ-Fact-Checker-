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


def get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            dbname=os.getenv("POSTGRES_DB", "factcheck"),
            user=os.getenv("POSTGRES_USER", "factcheck"),
            password=os.getenv("POSTGRES_PASSWORD", "changeme_secure_password"),
        )
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
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "factcheck"),
        user=os.getenv("POSTGRES_USER", "factcheck"),
        password=os.getenv("POSTGRES_PASSWORD", "changeme_secure_password"),
    )


def vector_search(
    query_embedding: list[float],
    fetch_limit: int = 30,
    similarity_threshold: float = 0.50,
    top_k: int = 5,
) -> list[dict]:
    """
    Two-stage retrieval from knowledge_chunks:
    1. Fetch nearest neighbors via pgvector HNSW index
    2. Filter by similarity_threshold
    3. Return top_k with article metadata
    """
    def _normalized_article_key(url: str, source: str, title: str) -> str:
        """Collapse duplicate rows for the same article across chunks or trailing-slash URL variants."""
        if url:
            parts = urlsplit(url)
            norm_path = parts.path.rstrip("/") or "/"
            norm_url = urlunsplit((parts.scheme.lower(), parts.netloc.lower(), norm_path, "", ""))
            return norm_url
        return f"{(source or '').strip().lower()}::{(title or '').strip().lower()}"

    embedding_str = str(query_embedding)
    effective_fetch_limit = max(fetch_limit, top_k * 10)

    query = """
        SELECT
            kc.chunk_id,
            kc.article_url,
            kc.chunk_text,
            kc.source,
            1 - (kc.embedding <=> %s::vector) AS similarity_score,
            sa.title,
            sa.published_at,
            sa.verdict_label
        FROM knowledge_chunks kc
        JOIN source_articles sa ON sa.url = kc.article_url
        ORDER BY kc.embedding <=> %s::vector
        LIMIT %s
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (embedding_str, embedding_str, effective_fetch_limit))
            rows = cur.fetchall()

    deduped_results = {}
    for row in rows:
        score = row[4]
        if score >= similarity_threshold:
            candidate = {
                "chunk_id": row[0],
                "url": row[1],
                "snippet": row[2],
                "source": row[3],
                "similarity_score": round(score, 4),
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
