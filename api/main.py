"""
KZ Fact-Checker — FastAPI Backend (v2: Multi-Source)

Endpoints:
  POST /check            — verify a claim against knowledge base
  POST /extract_claims   — extract checkable claims from text
  GET  /ztb_results      — latest ZTB verifications
  GET  /knowledge_stats  — article counts per source
  GET  /health           — health check
"""
import json
import logging
import os
import re

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from openai import OpenAI
from pydantic import BaseModel, Field

from api.db import get_db_connection, vector_search
from api.prompt import SYSTEM_PROMPT

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")

app = FastAPI(
    title="KZ Fact-Checker Engine",
    description="Multi-source RAG fact-checking for Kazakh news",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── LLM client (Alem.AI, OpenAI-compatible) ─────────────────
LLM_MODEL = os.getenv("LLM_MODEL", "kazllm")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://llm.alem.ai/v1")

_openai_client = None


def get_openai() -> OpenAI:
    global _openai_client
    if _openai_client is None:
        _openai_client = OpenAI(
            api_key=os.getenv("OPENAI_API_KEY"),
            base_url=LLM_BASE_URL,
        )
    return _openai_client


# ── Embedding model (local, free) ───────────────────────────
from sentence_transformers import SentenceTransformer

_embed_model = None


def get_embed_model() -> SentenceTransformer:
    global _embed_model
    if _embed_model is None:
        model_name = os.getenv(
            "EMBEDDING_MODEL_NAME",
            "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        )
        _embed_model = SentenceTransformer(model_name)
    return _embed_model


def _normalize_match_text(text: str) -> str:
    """Normalize text for lightweight lexical matching across Cyrillic/Latin punctuation."""
    lowered = (text or "").lower()
    cleaned = re.sub(r"[^\w\s]", " ", lowered, flags=re.UNICODE)
    return re.sub(r"\s+", " ", cleaned, flags=re.UNICODE).strip()


def _token_overlap_ratio(claim_text: str, evidence_text: str) -> float:
    """Measure how much of the claim vocabulary appears in the evidence snippet."""
    claim_tokens = {t for t in _normalize_match_text(claim_text).split() if len(t) > 2}
    evidence_tokens = {t for t in _normalize_match_text(evidence_text).split() if len(t) > 2}
    if not claim_tokens or not evidence_tokens:
        return 0.0
    return len(claim_tokens & evidence_tokens) / len(claim_tokens)


def _factcheck_label_heuristic(claim_text: str, evidence_blocks: list[dict]) -> dict | None:
    """
    Deterministic shortcut for Factcheck.kz items:
    if the claim text is directly present in a factcheck snippet and the article has an explicit verdict label,
    use that label instead of relying on a potentially unstable LLM interpretation.
    """
    truth_labels = {"шындық", "расталды", "ақиқат"}
    false_labels = {"жалған", "жалған ақпарат", "манипуляция", "жартылай шындық"}

    normalized_claim = _normalize_match_text(claim_text)
    for ev in evidence_blocks:
        if ev.get("source") != "factcheck":
            continue

        verdict_label = _normalize_match_text(ev.get("source_verdict", ""))
        if not verdict_label:
            continue

        normalized_snippet = _normalize_match_text(ev.get("snippet", ""))
        overlap_ratio = _token_overlap_ratio(claim_text, ev.get("snippet", ""))
        has_direct_match = bool(normalized_claim) and normalized_claim in normalized_snippet

        if not has_direct_match and overlap_ratio < 0.85:
            continue

        if verdict_label in truth_labels:
            return {
                "verdict": "SUPPORTED",
                "confidence": 0.9,
                "explanation_kk": (
                    "Factcheck.kz материалында осы мәлімдеме тікелей келтірілген және мақала үкімі "
                    "\"Шындық\" деп берілген. Сондықтан бұл claim расталған деп қабылданды."
                ),
            }
        if verdict_label in false_labels:
            return {
                "verdict": "REFUTED",
                "confidence": 0.9,
                "explanation_kk": (
                    "Factcheck.kz материалында осы мәлімдеме тікелей келтірілген және мақалада ол "
                    "жалған/манипуляция ретінде белгіленген. Сондықтан бұл claim теріске шығарылды."
                ),
            }

    return None


# ── Request models ───────────────────────────────────────────

class CheckRequest(BaseModel):
    claim: str = Field(..., min_length=5, description="Claim text to verify")
    top_k: int = Field(default=5, ge=1, le=20)
    similarity_threshold: float = Field(default=0.50, ge=0.0, le=1.0)


class ExtractRequest(BaseModel):
    text: str = Field(..., min_length=10, description="Article or post text to extract claims from")


# ── Endpoints ────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "kz-factchecker-v2"}


@app.post("/check")
def check_claim(req: CheckRequest):
    """
    Verify a claim:
    1. Embed claim → vector
    2. pgvector search → top evidence from knowledge_chunks
    3. LLM verdict
    4. Return structured JSON
    """
    client = get_openai()

    # 1. Embed (local model)
    try:
        model = get_embed_model()
        query_embedding = model.encode(req.claim, normalize_embeddings=True).tolist()
    except Exception as e:
        logger.error(f"Embedding failed: {e}")
        raise HTTPException(status_code=500, detail=f"Embedding error: {e}")

    # 2. Vector search
    evidence_blocks = vector_search(
        query_embedding,
        similarity_threshold=req.similarity_threshold,
        top_k=req.top_k,
    )

    if not evidence_blocks:
        return {
            "claim": req.claim,
            "verdict": "NOT_ENOUGH_INFO",
            "confidence": 0.0,
            "explanation_kk": "Деректер қорынан бұл мәлімдемеге қатысты ақпарат табылмады.",
            "evidence": [],
            "best_match": {},
            "retrieval_debug": {"evidence_count": 0, "top_similarity": 0.0},
        }

    heuristic_result = _factcheck_label_heuristic(req.claim, evidence_blocks)
    if heuristic_result:
        best = evidence_blocks[0] if evidence_blocks else {}
        top_sim = best.get("similarity_score", 0.0)
        return {
            "claim": req.claim,
            "verdict": heuristic_result["verdict"],
            "confidence": heuristic_result["confidence"],
            "explanation_kk": heuristic_result["explanation_kk"],
            "evidence": evidence_blocks,
            "best_match": best,
            "retrieval_debug": {
                "evidence_count": len(evidence_blocks),
                "top_similarity": top_sim,
                "matched_by": "factcheck_label_heuristic",
            },
        }

    # 3. Build context for LLM — include title, source type, verdict label
    context_parts = []
    for i, ev in enumerate(evidence_blocks, 1):
        source_label = ev.get("source", "?")
        title = ev.get("title", "")
        verdict_label = ev.get("source_verdict", "")
        sim_score = ev.get("similarity_score", 0)

        header = f"[{i}] SOURCE: {source_label} | TITLE: {title} | SIMILARITY: {sim_score:.0%}"
        if verdict_label:
            header += f" | VERDICT_LABEL: {verdict_label}"
        context_parts.append(
            f"{header}\n{ev['snippet'][:1200]}"
        )
    context_str = "\n\n".join(context_parts)

    # 4. LLM verdict
    try:
        llm_resp = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": (
                        f"CLAIM: {req.claim}\n\n"
                        f"EVIDENCE:\n{context_str}\n\n"
                        "IMPORTANT: Read each evidence carefully. Check if it discusses the EXACT SAME specific fact as the claim, not just the same general topic.\n"
                        "Respond in JSON: {\"verdict\", \"confidence\", \"explanation_kk\"}"
                    ),
                },
            ],
            temperature=0.0,
            max_completion_tokens=500,
            response_format={"type": "json_object"},
        )
        raw = llm_resp.choices[0].message.content
        result = json.loads(raw)
    except Exception as e:
        logger.error(f"LLM error: {e}")
        result = {
            "verdict": "NOT_ENOUGH_INFO",
            "confidence": 0.0,
            "explanation_kk": f"LLM қатесі: {e}",
        }

    best = evidence_blocks[0] if evidence_blocks else {}
    top_sim = best.get("similarity_score", 0.0)

    return {
        "claim": req.claim,
        "verdict": result.get("verdict", "NOT_ENOUGH_INFO"),
        "confidence": result.get("confidence", 0.0),
        "explanation_kk": result.get("explanation_kk", ""),
        "evidence": evidence_blocks,
        "best_match": best,
        "retrieval_debug": {
            "evidence_count": len(evidence_blocks),
            "top_similarity": top_sim,
        },
    }


@app.post("/extract_claims")
def extract_claims(req: ExtractRequest):
    """Extract checkable claims from text using GPT."""
    client = get_openai()
    try:
        resp = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Сен мәтіннен тексерілетін фактілерді (claim) шығарып алатын жүйесің.\n"
                        "Берілген мәтіннен тек нақты фактілік мәлімдемелерді JSON ретінде қайтар.\n"
                        "Пікірлер емес, тек фактілерді шығар. Максимум 5 claim.\n"
                        "Әр claim қысқа, нақты бір фактіні қамтуы керек.\n\n"
                        'Формат: {"claims": ["claim1", "claim2", ...], "topics": ["theme1"]}'
                    ),
                },
                {"role": "user", "content": req.text[:3000]},
            ],
            temperature=0.1,
            max_completion_tokens=500,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content
        parsed = json.loads(raw)
        return {
            "claims": parsed.get("claims", []),
            "topics": parsed.get("topics", []),
        }
    except Exception as e:
        logger.error(f"Extract claims error: {e}")
        raise HTTPException(status_code=500, detail=f"LLM error: {e}")


@app.get("/ztb_results")
def get_ztb_results(limit: int = 30):
    """Return latest ZTB articles that have SUPPORTED/REFUTED verification results."""
    def _to_payload(raw_response: dict | str | None) -> dict:
        if isinstance(raw_response, dict):
            return raw_response
        if isinstance(raw_response, str) and raw_response.strip():
            try:
                parsed = json.loads(raw_response)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                return {}
        return {}

    def _to_score(value) -> float | None:
        try:
            score = float(value)
        except (TypeError, ValueError):
            return None
        if score < 0:
            return 0.0
        if score > 1:
            return 1.0
        return score

    def _extract_scores(payload: dict, fallback_similarity: float | None) -> tuple[float | None, float | None]:
        llm_confidence = _to_score(payload.get("confidence"))
        best_match = payload.get("best_match", {}) if isinstance(payload, dict) else {}
        retrieval_score = _to_score(best_match.get("similarity_score")) if isinstance(best_match, dict) else None
        if retrieval_score is None:
            retrieval_score = _to_score(fallback_similarity)
        return llm_confidence, retrieval_score

    def _extract_evidence_sources(
        payload: dict,
        fallback_source: str | None,
        fallback_url: str | None,
        fallback_similarity: float | None,
    ) -> list[dict]:
        """Build compact evidence source list from stored raw verification response."""
        raw_evidence = payload.get("evidence", []) if isinstance(payload, dict) else []
        evidence_sources: list[dict] = []
        seen: set[tuple[str, str, str]] = set()

        if isinstance(raw_evidence, list):
            for ev in raw_evidence:
                if not isinstance(ev, dict):
                    continue
                source = (ev.get("source") or "").strip()
                title = (ev.get("title") or "").strip()
                url = (ev.get("url") or "").strip()
                similarity = _to_score(ev.get("similarity_score"))

                key = (source, title, url)
                if key in seen:
                    continue
                seen.add(key)
                evidence_sources.append({
                    "source": source,
                    "title": title,
                    "url": url,
                    "similarity_score": similarity,
                })

        if not evidence_sources and (fallback_source or fallback_url):
            evidence_sources.append({
                "source": fallback_source or "",
                "title": "",
                "url": fallback_url or "",
                "similarity_score": _to_score(fallback_similarity),
            })

        return evidence_sources[:10]

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT COUNT(DISTINCT sa.url)
            FROM source_articles sa
            JOIN ztb_claims zc ON zc.article_url = sa.url
            JOIN verifications v ON v.claim_id = zc.claim_id
            WHERE sa.source = 'ztb'
              AND v.verdict IN ('SUPPORTED', 'REFUTED')
            """,
        )
        total_verified_articles = cur.fetchone()[0]

        cur.execute(
            """
            WITH latest_verified_articles AS (
                SELECT sa.url, sa.title, sa.published_at, sa.created_at
                FROM source_articles sa
                WHERE sa.source = 'ztb'
                  AND EXISTS (
                      SELECT 1
                      FROM ztb_claims zc
                      JOIN verifications v ON v.claim_id = zc.claim_id
                      WHERE zc.article_url = sa.url
                        AND v.verdict IN ('SUPPORTED', 'REFUTED')
                  )
                ORDER BY COALESCE(sa.published_at, sa.created_at) DESC NULLS LAST
                LIMIT %s
            )
            SELECT
                lva.url,
                lva.title,
                lva.published_at,
                lva.created_at,
                zc.claim_text,
                v.verdict,
                v.similarity_score,
                v.explanation_kk,
                v.best_article_url,
                v.best_source,
                v.raw_response,
                ref_sa.title AS best_source_title
            FROM latest_verified_articles lva
            JOIN ztb_claims zc ON zc.article_url = lva.url
            JOIN verifications v ON v.claim_id = zc.claim_id
            LEFT JOIN source_articles ref_sa ON ref_sa.url = v.best_article_url
            WHERE v.verdict IN ('SUPPORTED', 'REFUTED')
            ORDER BY COALESCE(lva.published_at, lva.created_at) DESC NULLS LAST, zc.claim_id
            """,
            (limit,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Group claims by article
        articles = {}
        for row in rows:
            url = row[0]
            if url not in articles:
                pub_date = row[2]  # only real published_at, no created_at fallback
                articles[url] = {
                    "url": url,
                    "title": row[1],
                    "published_at": pub_date.isoformat() if pub_date else None,
                    "claims": [],
                }
            if row[4]:  # has claim
                payload = _to_payload(row[10])
                llm_confidence, retrieval_score = _extract_scores(payload, row[6])
                evidence_sources = _extract_evidence_sources(
                    payload=payload,
                    fallback_source=row[9],
                    fallback_url=row[8],
                    fallback_similarity=row[6],
                )
                articles[url]["claims"].append({
                    "claim_text": row[4],
                    "verdict": row[5],
                    "confidence": llm_confidence,
                    "retrieval_score": retrieval_score,
                    "explanation_kk": row[7],
                    "source_url": row[8],
                    "source_name": row[9],
                    "source_title": row[11] or "",
                    "evidence": evidence_sources,
                })

        result = sorted(articles.values(), key=lambda x: x["published_at"] or "", reverse=True)
        return {
            "ztb_results": result,
            "total_verified_articles": total_verified_articles,
        }

    except Exception as e:
        logger.error(f"Failed to fetch ZTB results: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@app.get("/knowledge_stats")
def knowledge_stats():
    """Return article and chunk counts per source."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT source, COUNT(*) as articles
            FROM source_articles
            GROUP BY source
            ORDER BY source
        """)
        article_counts = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute("""
            SELECT source, COUNT(*) as chunks
            FROM knowledge_chunks
            GROUP BY source
            ORDER BY source
        """)
        chunk_counts = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute("SELECT COUNT(*) FROM ztb_claims")
        total_claims = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM verifications")
        total_verifications = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(DISTINCT zc.article_url)
            FROM ztb_claims zc
            JOIN verifications v ON v.claim_id = zc.claim_id
            WHERE v.verdict IN ('SUPPORTED', 'REFUTED')
        """)
        total_verified_true_false_articles = cur.fetchone()[0]

        cur.execute("""
            SELECT source, COUNT(*) as articles
            FROM source_articles
            WHERE created_at >= NOW() - INTERVAL '1 hour'
            GROUP BY source
            ORDER BY source
        """)
        last_hour_articles_per_source = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute("""
            SELECT COUNT(*) FILTER (WHERE verdict = 'SUPPORTED') AS supported_count,
                   COUNT(*) FILTER (WHERE verdict = 'REFUTED') AS refuted_count,
                   COUNT(*) AS total_count
            FROM verifications
            WHERE created_at >= NOW() - INTERVAL '1 hour'
        """)
        row = cur.fetchone()
        last_hour_verifications = {
            "supported": row[0] or 0,
            "refuted": row[1] or 0,
            "total": row[2] or 0,
        }

        cur.execute("""
            SELECT COUNT(*)
            FROM ztb_claims
            WHERE created_at >= NOW() - INTERVAL '1 hour'
        """)
        last_hour_ztb_claims = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(DISTINCT zc.article_url)
            FROM ztb_claims zc
            JOIN verifications v ON v.claim_id = zc.claim_id
            WHERE v.created_at >= NOW() - INTERVAL '1 hour'
              AND v.verdict IN ('SUPPORTED', 'REFUTED')
        """)
        last_hour_verified_true_false_articles = cur.fetchone()[0]

        # Fetch actual recent articles with links
        cur.execute("""
            SELECT url, source, title, created_at
            FROM source_articles
            WHERE created_at >= NOW() - INTERVAL '1 hour'
            ORDER BY created_at DESC
            LIMIT 50
        """)
        last_hour_article_list = [
            {
                "url": row[0],
                "source": row[1],
                "title": row[2],
                "created_at": row[3].isoformat() if row[3] else None,
            }
            for row in cur.fetchall()
        ]

        cur.close()
        conn.close()

        return {
            "articles_per_source": article_counts,
            "chunks_per_source": chunk_counts,
            "total_ztb_claims": total_claims,
            "total_verifications": total_verifications,
            "total_verified_true_false_articles": total_verified_true_false_articles,
            "last_hour_articles_per_source": last_hour_articles_per_source,
            "last_hour_ztb_claims": last_hour_ztb_claims,
            "last_hour_verifications": last_hour_verifications,
            "last_hour_verified_true_false_articles": last_hour_verified_true_false_articles,
            "last_hour_article_list": last_hour_article_list,
        }
    except Exception as e:
        logger.error(f"Stats error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
