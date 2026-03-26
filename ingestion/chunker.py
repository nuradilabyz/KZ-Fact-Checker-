"""
Text Chunker — splits article clean_text into overlapping chunks.

- ~500 tokens per chunk, ~50 token overlap
- Uses tiktoken for token counting
- NO verdict/title prepend (clean embeddings)
"""
import hashlib

import tiktoken

# Use cl100k_base (same tokenizer as text-embedding-3-small/large)
_enc = tiktoken.get_encoding("cl100k_base")

CHUNK_SIZE = 500    # tokens
CHUNK_OVERLAP = 50  # tokens


def _token_len(text: str) -> int:
    return len(_enc.encode(text))


def chunk_text(
    text: str,
    chunk_size: int = CHUNK_SIZE,
    overlap: int = CHUNK_OVERLAP,
) -> list[dict]:
    """
    Split text into overlapping chunks.

    Returns list of:
        {"chunk_text": str, "chunk_hash": str}
    """
    tokens = _enc.encode(text)
    total = len(tokens)

    if total == 0:
        return []

    chunks = []
    start = 0

    while start < total:
        end = min(start + chunk_size, total)
        chunk_tokens = tokens[start:end]
        chunk_text = _enc.decode(chunk_tokens)

        chunk_hash = hashlib.sha256(chunk_text.encode("utf-8")).hexdigest()
        chunks.append({
            "chunk_text": chunk_text,
            "chunk_hash": chunk_hash,
        })

        # Move forward by (chunk_size - overlap)
        start += chunk_size - overlap

        # If remaining tokens < overlap, break to avoid tiny chunks
        if total - start < overlap:
            break

    return chunks
