"""
Embedding Pipeline — embeds new chunks via OpenAI text-embedding-3-small
and stores them in pgvector.

- Checks (article_url, chunk_hash) uniqueness before inserting
- Batches API calls (max 100 texts per batch)
"""
import logging
import os
from typing import Optional

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

logger = logging.getLogger("embedder")

EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
EMBEDDING_DIM = 1536  # text-embedding-3-small default
BATCH_SIZE = 100

_client: Optional[OpenAI] = None


def _get_openai_client() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _client


def compute_embeddings(texts: list[str]) -> list[list[float]]:
    """Compute embeddings for a list of texts using OpenAI API."""
    client = _get_openai_client()
    all_embeddings = []

    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i : i + BATCH_SIZE]
        response = client.embeddings.create(
            input=batch,
            model=EMBEDDING_MODEL,
        )
        batch_embeddings = [item.embedding for item in response.data]
        all_embeddings.extend(batch_embeddings)

    return all_embeddings


def embed_and_store_chunks(conn, article_url: str, chunks: list[dict]):
    """
    Embed chunks and store in fact_chunks.

    Args:
        conn: psycopg2 connection
        article_url: URL of the parent article
        chunks: list of {"chunk_text": str, "chunk_hash": str}
    """
    if not chunks:
        return

    # Filter out chunks that already exist
    chunk_hashes = [c["chunk_hash"] for c in chunks]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT chunk_hash FROM fact_chunks
            WHERE article_url = %s AND chunk_hash = ANY(%s)
            """,
            (article_url, chunk_hashes),
        )
        existing_hashes = {row[0] for row in cur.fetchall()}

    new_chunks = [c for c in chunks if c["chunk_hash"] not in existing_hashes]

    if not new_chunks:
        logger.info(f"  ⏭  All {len(chunks)} chunks already embedded for {article_url}")
        return

    logger.info(f"  🧮 Embedding {len(new_chunks)} new chunks (skipped {len(existing_hashes)} existing)")

    # Compute embeddings
    texts = [c["chunk_text"] for c in new_chunks]
    embeddings = compute_embeddings(texts)

    # Insert into DB
    with conn.cursor() as cur:
        for chunk, embedding in zip(new_chunks, embeddings):
            cur.execute(
                """
                INSERT INTO fact_chunks (article_url, chunk_text, embedding, chunk_hash)
                VALUES (%s, %s, %s::vector, %s)
                ON CONFLICT (article_url, chunk_hash) DO NOTHING
                """,
                (article_url, chunk["chunk_text"], str(embedding), chunk["chunk_hash"]),
            )
    conn.commit()
    logger.info(f"  ✅ Stored {len(new_chunks)} chunks for {article_url}")
