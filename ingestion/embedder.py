"""
Embedding Pipeline — embeds new chunks via sentence-transformers (local, free)
and stores them in pgvector.

- Uses paraphrase-multilingual-MiniLM-L12-v2 (384-dim, multilingual incl. Kazakh)
- Checks (article_url, chunk_hash) uniqueness before inserting
- Batches encoding for efficiency
"""
import logging
import os
from typing import Optional

from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

load_dotenv()

logger = logging.getLogger("embedder")

EMBEDDING_MODEL_NAME = os.getenv(
    "EMBEDDING_MODEL_NAME", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
)
EMBEDDING_DIM = 384
BATCH_SIZE = 64

_model: Optional[SentenceTransformer] = None


def _get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        logger.info(f"Loading embedding model: {EMBEDDING_MODEL_NAME}")
        _model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    return _model


def compute_embeddings(texts: list[str]) -> list[list[float]]:
    """Compute embeddings for a list of texts using local model."""
    model = _get_model()
    embeddings = model.encode(texts, batch_size=BATCH_SIZE, show_progress_bar=False, normalize_embeddings=True)
    return [emb.tolist() for emb in embeddings]


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
        logger.info(f"  All {len(chunks)} chunks already embedded for {article_url}")
        return

    logger.info(f"  Embedding {len(new_chunks)} new chunks (skipped {len(existing_hashes)} existing)")

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
    logger.info(f"  Stored {len(new_chunks)} chunks for {article_url}")
