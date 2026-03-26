# KZ Fact-Checker Engine 🔍

> RAG-based fact-checking pipeline for Kazakh-language claims using Factcheck.kz as the source of truth.

## Architecture

```
Factcheck.kz ──sitemap──▸ Scraper ──▸ Chunker ──▸ Embedder ──▸ PostgreSQL + pgvector
                                                                       │
Threads API ──▸ Collector ──▸ Claim Extractor ──▸ Embedder             │
                                                      │                │
                                                      ▼                │
User/Streamlit ──▸ POST /check ──▸ Vector Search ◂────┘
                                        │
                                        ▼
                                   LLM Verdict
                                        │
                                        ▼
                                  JSON Response
```

## Tech Stack

| Component | Tech |
|-----------|------|
| Storage | PostgreSQL 16 + pgvector (HNSW) |
| Embeddings | OpenAI `text-embedding-3-small` (1536d) |
| LLM | OpenAI `gpt-4o-mini` |
| API | FastAPI + uvicorn |
| UI | Streamlit |
| Orchestration | Apache Airflow |
| Scraping | requests + bs4 (Playwright fallback) |
| Infra | Docker + docker-compose |

## Quick Start

### 1. Clone & configure

```bash
cp .env.example .env
# Edit .env: set OPENAI_API_KEY, THREADS_ACCESS_TOKEN, POSTGRES_PASSWORD
```

### 2. Start services

```bash
docker compose up -d
```

This starts:
- **PostgreSQL** (port 5432) — schema auto-applied on first boot
- **API** (port 8000) — FastAPI backend
- **Streamlit** (port 8501) — web UI

### 3. Run initial Factcheck.kz ingestion

```bash
# Ingest 5 articles (quick test)
docker compose run --rm ingestion python -m ingestion.factcheck_scraper --limit 5

# Full ingestion (all Kazakh articles from news-sitemap.xml)
docker compose run --rm ingestion python -m ingestion.factcheck_scraper
```

### 4. Use the UI

Open [http://localhost:8501](http://localhost:8501) and:
1. Paste a Kazakh claim
2. Click **🔍 Тексеру**
3. See: verdict badge, confidence, evidence quotes, Kazakh explanation

### 5. API usage (direct)

```bash
# Verify a claim
curl -X POST http://localhost:8000/check \
  -H "Content-Type: application/json" \
  -d '{"claim": "Қазақстанда инфляция 20% болды", "top_k": 5}'

# Extract claims from a post
curl -X POST http://localhost:8000/extract_claims \
  -H "Content-Type: application/json" \
  -d '{"post_id": "test1", "username": "user", "created_at": "2025-01-01T00:00:00Z", "text": "..."}'
```

## Project Structure

```
PROJECT-1-FREEDOM/
├── docker-compose.yml         # All services
├── Dockerfile.api             # FastAPI image
├── Dockerfile.ui              # Streamlit image
├── Dockerfile.ingestion       # Scraper image (with Playwright)
├── .env.example               # Environment template
│
├── db/
│   └── schema.sql             # PostgreSQL + pgvector schema
│
├── api/
│   ├── main.py                # FastAPI app (/check, /extract_claims)
│   ├── db.py                  # Connection pool + vector search
│   └── prompt.py              # LLM system prompt
│
├── ingestion/
│   ├── factcheck_scraper.py   # Sitemap → scrape → upsert articles
│   ├── chunker.py             # Text → overlapping chunks
│   ├── embedder.py            # Chunks → OpenAI embeddings → pgvector
│   └── threads_collector.py   # Threads API → posts → claims
│
├── ui/
│   └── app.py                 # Streamlit UI
│
├── dags/
│   ├── factcheck_ingest_dag.py  # Airflow: daily Factcheck.kz
│   └── threads_ingest_dag.py    # Airflow: daily Threads + auto-verify
│
└── requirements/
    ├── api.txt
    ├── ingestion.txt
    ├── ui.txt
    └── airflow.txt
```

## Data Model

### Factcheck.kz
- `fact_articles` — parsed articles (url PK, title, verdict_text, clean_text, content_hash)
- `fact_chunks` — text chunks with embeddings (HNSW index, UNIQUE(article_url, chunk_hash))

### Threads
- `threads_posts` — raw posts (post_id PK, lang_detected)
- `threads_claims` — extracted claims (UNIQUE(post_id, claim_text))
- `matches` — verification results (verdict, explanation_kk, raw JSON)

## Incremental Updates

- **Factcheck.kz**: compares `content_hash` (SHA-256) — only scrapes new/changed articles, only embeds new chunks
- **Threads**: checks `post_id` uniqueness — only processes new posts
- **Embeddings**: checked by `(article_url, chunk_hash)` — no duplicate embeddings

## License

MIT
