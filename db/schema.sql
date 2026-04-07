-- KZ Fact-Checker Engine — PostgreSQL Schema (v2: Multi-Source)
-- Requires: pgvector extension

CREATE EXTENSION IF NOT EXISTS vector;

-- ============================================================
-- Source articles (all 5 sites)
-- ============================================================

CREATE TABLE IF NOT EXISTS source_articles (
    url             TEXT PRIMARY KEY,
    source          TEXT NOT NULL,                -- factcheck | azattyq | informburo | tengrinews | ztb
    title           TEXT NOT NULL,
    author          TEXT,
    published_at    TIMESTAMPTZ,
    clean_text      TEXT NOT NULL,
    content_hash    TEXT NOT NULL,                -- MD5 for change detection
    verdict_label   TEXT,                         -- e.g. "Жалған" (factcheck.kz only)
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_articles_source
    ON source_articles(source);

CREATE INDEX IF NOT EXISTS idx_articles_published
    ON source_articles(published_at DESC);

-- ============================================================
-- Knowledge chunks (reference sources only — NOT ztb)
-- Embedded text fragments used for RAG retrieval
-- ============================================================

CREATE TABLE IF NOT EXISTS knowledge_chunks (
    chunk_id        SERIAL PRIMARY KEY,
    article_url     TEXT NOT NULL REFERENCES source_articles(url) ON DELETE CASCADE,
    source          TEXT NOT NULL,                -- denormalized for fast filtering
    chunk_text      TEXT NOT NULL,
    embedding       vector(384),                 -- paraphrase-multilingual-MiniLM-L12-v2
    chunk_hash      TEXT NOT NULL,               -- SHA-256 of chunk_text
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(article_url, chunk_hash)
);

CREATE INDEX IF NOT EXISTS idx_kchunks_article
    ON knowledge_chunks(article_url);

CREATE INDEX IF NOT EXISTS idx_kchunks_source
    ON knowledge_chunks(source);

-- HNSW index for cosine similarity search
CREATE INDEX IF NOT EXISTS idx_kchunks_embedding
    ON knowledge_chunks USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- ============================================================
-- ZTB claims (extracted from ztb.kz articles)
-- ============================================================

CREATE TABLE IF NOT EXISTS ztb_claims (
    claim_id        SERIAL PRIMARY KEY,
    article_url     TEXT NOT NULL REFERENCES source_articles(url) ON DELETE CASCADE,
    claim_text      TEXT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(article_url, claim_text)
);

-- ============================================================
-- Verification results (RAG verdicts)
-- ============================================================

CREATE TABLE IF NOT EXISTS verifications (
    id              SERIAL PRIMARY KEY,
    claim_id        INT UNIQUE REFERENCES ztb_claims(claim_id) ON DELETE CASCADE,
    best_article_url TEXT REFERENCES source_articles(url),
    best_source     TEXT,                        -- which reference source matched
    similarity_score FLOAT,
    verdict         TEXT,                        -- SUPPORTED | REFUTED | NOT_ENOUGH_INFO
    explanation_kk  TEXT,                        -- Kazakh-language explanation
    raw_response    JSONB,
    created_at      TIMESTAMPTZ DEFAULT now()
);
