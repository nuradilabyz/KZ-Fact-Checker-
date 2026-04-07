# KZ Fact-Checker Engine

**Automated RAG-based fact-checking pipeline for Kazakh-language news**

Live demo: [kz-factchecker-ui.onrender.com](https://kz-factchecker-ui.onrender.com)

---

## Overview

End-to-end data engineering pipeline that scrapes Kazakh news from 5 sources, builds a vector knowledge base, and automatically verifies claims using RAG (Retrieval-Augmented Generation).

The system ingests articles hourly, extracts factual claims from ZTB.kz, and cross-references them against trusted sources (Factcheck.kz, Azattyq, Informburo, Tengrinews) to produce SUPPORTED / REFUTED verdicts with Kazakh-language explanations.

## Architecture

```
                        Hourly Cron (GitHub Actions / Airflow)
                                      |
              ┌───────────┬───────────┼───────────┬──────────┐
              v           v           v           v          v
         factcheck.kz  azattyq.org  informburo.kz  tengri   ztb.kz
              |           |           |           |          |
              └─────┬─────┴─────┬─────┘           |     Claim Extractor
                    v           v                  v       (LLM API)
              HTML Scraper   RSS Parser      HTML Scraper     |
                    |           |                  |          v
                    └─────┬─────┴──────────────────┘    Extracted Claims
                          v                                   |
                    Text Chunker                              |
                          |                                   |
                          v                                   |
                 Embedding Model ──────────> pgvector  <──────┘
              (sentence-transformers)       (PostgreSQL)   Vector Search
                                                |              |
                                                v              v
                                           Knowledge Base   LLM Verdict
                                                          (API Integration)
                                                               |
                                                               v
                                            FastAPI ──── Streamlit UI
```

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Orchestration** | Apache Airflow / GitHub Actions | Hourly scheduled ingestion pipeline |
| **Ingestion** | Python, BeautifulSoup, lxml | Web scraping & RSS parsing from 5 news sources |
| **NLP** | LLM API (OpenAI-compatible) | Claim extraction & verdict generation |
| **Embeddings** | sentence-transformers (MiniLM) | Multilingual text vectorization (384-dim) |
| **Storage** | PostgreSQL + pgvector (HNSW) | Relational data + vector similarity search |
| **Backend** | FastAPI + Uvicorn | REST API with RAG retrieval pipeline |
| **Frontend** | Streamlit | Interactive dashboard with real-time results |
| **Infrastructure** | Docker, Render, Neon, GitHub Actions | Containerized local dev + cloud deployment |

## Data Pipeline

```
1. DISCOVER  ──  RSS feeds & category pages → list of article URLs
2. FILTER    ──  Content hash (MD5) check → skip unchanged articles
3. SCRAPE    ──  HTTP fetch → BeautifulSoup parsing → clean text extraction
4. CHUNK     ──  Overlapping text chunks (500 tokens, 50 overlap)
5. EMBED     ──  sentence-transformers → 384-dim vectors → pgvector HNSW index
6. EXTRACT   ──  (ZTB only) LLM extracts checkable claims from article text
7. VERIFY    ──  Vector search → top-K evidence retrieval → LLM verdict
```

**Incremental processing:** each step checks for duplicates — only new/changed data flows through the pipeline.

## Data Model

```sql
source_articles    -- 5 news sources (url PK, content_hash, verdict_label)
knowledge_chunks   -- text chunks + embeddings (pgvector HNSW index)
ztb_claims         -- extracted claims from ZTB articles
verifications      -- RAG verdicts (SUPPORTED / REFUTED / NOT_ENOUGH_INFO)
```

| Table | Records | Description |
|-------|---------|-------------|
| `source_articles` | ~2000+ | Articles from 5 Kazakh news sites |
| `knowledge_chunks` | ~10K+ | Embedded text chunks for vector search |
| `ztb_claims` | ~500+ | Auto-extracted factual claims |
| `verifications` | ~500+ | LLM verdicts with evidence |

## Key Features

- **Multi-source ingestion** — parallel scraping of 5 Kazakh news sites with rate limiting
- **Incremental updates** — content hashing prevents redundant processing
- **Vector search** — pgvector HNSW index for fast cosine similarity retrieval
- **Deduplication** — URL normalization + chunk hashing at every stage
- **Heuristic shortcuts** — Factcheck.kz verdict labels bypass LLM when confidence is high
- **Automated scheduling** — hourly cron via GitHub Actions (or Airflow for local dev)

## Project Structure

```
├── .github/workflows/
│   └── ingest.yml              # Hourly cron: ingestion pipeline
├── dags/
│   └── news_ingest_dag.py      # Airflow DAG (local dev alternative)
├── ingestion/
│   ├── news_scraper.py         # Multi-source scraper + pipeline orchestration
│   ├── factcheck_scraper.py    # Factcheck.kz specialized scraper
│   ├── chunker.py              # Text → overlapping chunks
│   └── embedder.py             # Chunks → vectors → pgvector
├── api/
│   ├── main.py                 # FastAPI endpoints (/check, /ztb_results, etc.)
│   ├── db.py                   # Connection pool + vector search queries
│   └── prompt.py               # LLM system prompt for verdicts
├── ui/
│   └── app.py                  # Streamlit dashboard
├── db/
│   └── schema.sql              # PostgreSQL + pgvector schema
├── docker-compose.yml          # Local dev: all services
├── render.yaml                 # Cloud deployment config
└── requirements/               # Per-service dependencies
```

## Local Development

```bash
# 1. Clone & configure
git clone https://github.com/nuradilabyz/KZ-Fact-Checker-.git
cp .env.example .env   # set API keys and DB credentials

# 2. Start all services
docker compose up -d

# 3. Run ingestion
docker compose run --rm ingestion python -c "
from ingestion.news_scraper import run_source_ingestion
run_source_ingestion('factcheck', months_back=1)
"

# 4. Open UI
open http://localhost:8501
```

## Cloud Deployment

| Service | Platform | Tier |
|---------|----------|------|
| PostgreSQL + pgvector | Neon | Free |
| FastAPI backend | Render | Free |
| Streamlit UI | Render | Free |
| Hourly ingestion | GitHub Actions | Free |

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/check` | Verify a claim against knowledge base |
| `POST` | `/extract_claims` | Extract checkable claims from text |
| `GET` | `/ztb_results` | Latest verified ZTB articles |
| `GET` | `/knowledge_stats` | Article & chunk counts per source |
| `GET` | `/health` | Health check |

## License

MIT
