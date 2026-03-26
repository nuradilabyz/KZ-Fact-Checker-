"""
One-time script: Re-extract claims from existing ZTB articles and verify them.

Run inside Docker network:
  docker compose run --rm -e API_URL=http://api:8000 ingestion \
    python scripts/rerun_ztb_verify.py

Or from host (if API is accessible):
  API_URL=http://localhost:8000 python scripts/rerun_ztb_verify.py
"""

import json
import logging
import os
import sys

import psycopg2
import requests
from dotenv import load_dotenv

# Add parent dir so we can import ingestion module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("rerun_ztb")

API_URL = os.getenv("API_URL", "http://api:8000")


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "factcheck"),
        user=os.getenv("POSTGRES_USER", "factcheck"),
        password=os.getenv("POSTGRES_PASSWORD", "changeme_secure_password"),
    )


def extract_claims_from_text(text: str) -> list[str]:
    """Use GPT to extract checkable claims from article text."""
    import openai

    client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    try:
        resp = client.chat.completions.create(
            model=os.getenv("LLM_MODEL", "gpt-4o-mini"),
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Сен мәтіннен тексерілетін фактілерді (claim) шығарып алатын жүйесің.\n"
                        "Берілген мәтіннен тек нақты фактілік мәлімдемелерді JSON массиві ретінде қайтар.\n"
                        "Пікірлер емес, тек НАҚТЫ фактілерді шығар.\n"
                        "Сандар, даталар, оқиғалар, заттар туралы мәлімдемелерді іздеңіз.\n"
                        "Максимум 5 claim. Әр claim қысқа, бір сөйлем.\n"
                        'Формат: ["claim1", "claim2", ...]'
                    ),
                },
                {"role": "user", "content": text[:3000]},
            ],
            temperature=0.1,
            max_completion_tokens=500,
        )
        raw = resp.choices[0].message.content.strip()
        # Handle potential markdown wrapping
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        claims = json.loads(raw)
        if isinstance(claims, list):
            return [c for c in claims if isinstance(c, str) and len(c) > 10]
    except Exception as e:
        logger.error(f"  Claim extraction failed: {e}")
    return []


def verify_claim(claim_text: str) -> dict | None:
    """Verify a claim using the fact-checker API."""
    try:
        resp = requests.post(
            f"{API_URL}/check",
            json={"claim": claim_text, "top_k": 5, "similarity_threshold": 0.6},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"  Verification API error: {e}")
        return None


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--clear", action="store_true", help="Delete all existing verifications first")
    parser.add_argument(
        "--keep-existing-claims",
        action="store_true",
        help="Do not delete existing claims before re-extracting them",
    )
    args = parser.parse_args()

    conn = get_db_conn()

    # Check API health
    try:
        r = requests.get(f"{API_URL}/health", timeout=5)
        r.raise_for_status()
        logger.info(f"✅ API is healthy: {r.json()}")
    except Exception as e:
        logger.error(f"❌ Cannot reach API at {API_URL}: {e}")
        logger.error("Make sure to run this inside the Docker network or set API_URL correctly.")
        sys.exit(1)

    # Clear old verifications if requested
    if args.clear:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM verifications")
            deleted = cur.rowcount
        conn.commit()
        logger.info(f"🗑  Deleted {deleted} old verifications")

    # Fetch all ZTB articles
    with conn.cursor() as cur:
        cur.execute(
            "SELECT url, title, clean_text FROM source_articles WHERE source = 'ztb' ORDER BY published_at DESC"
        )
        articles = cur.fetchall()

    logger.info(f"Found {len(articles)} ZTB articles")

    total_claims = 0
    total_verified = 0
    verdict_counts = {"SUPPORTED": 0, "REFUTED": 0, "NOT_ENOUGH_INFO": 0}

    for i, (url, title, clean_text) in enumerate(articles, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"[{i}/{len(articles)}] {title[:80]}")
        logger.info(f"  URL: {url}")

        # Skip very short articles or listing pages
        if len(clean_text) < 200:
            logger.warning(f"  Skipping: article too short ({len(clean_text)} chars)")
            continue

        if not args.keep_existing_claims:
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM ztb_claims WHERE article_url = %s", (url,))
                    deleted_claims = cur.rowcount
                conn.commit()
                if deleted_claims:
                    logger.info(f"  Refreshed article claims: deleted {deleted_claims} old rows")
            except Exception as e:
                conn.rollback()
                logger.error(f"  DB error refreshing claims: {e}")
                continue

        # Extract claims
        claims = extract_claims_from_text(clean_text)
        if not claims:
            logger.warning(f"  No claims extracted")
            continue

        logger.info(f"  Extracted {len(claims)} claims")

        for claim_text in claims:
            total_claims += 1
            logger.info(f"  Claim: {claim_text[:100]}...")

            # Insert claim into DB
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
                        # Already exists, get existing claim_id
                        cur.execute(
                            "SELECT claim_id FROM ztb_claims WHERE article_url = %s AND claim_text = %s",
                            (url, claim_text),
                        )
                        row = cur.fetchone()
                        if not row:
                            logger.warning(f"  Could not get claim_id")
                            continue
                    claim_id = row[0]
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"  DB error inserting claim: {e}")
                continue

            # Check if already verified (skip unless --clear was used)
            if not args.clear:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM verifications WHERE claim_id = %s", (claim_id,))
                    if cur.fetchone():
                        logger.info(f"  Already verified, skipping")
                        continue

            # Verify claim
            result = verify_claim(claim_text)
            if not result:
                continue

            verdict = result.get("verdict", "NOT_ENOUGH_INFO")
            confidence = result.get("confidence", 0.0)
            logger.info(f"  → {verdict} (confidence: {confidence:.2f})")
            verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1

            # Store verification
            try:
                best = result.get("best_match", {})
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO verifications
                            (claim_id, best_article_url, best_source,
                             similarity_score, verdict, explanation_kk, raw_response)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (claim_id) DO UPDATE SET
                            verdict = EXCLUDED.verdict,
                            similarity_score = EXCLUDED.similarity_score,
                            explanation_kk = EXCLUDED.explanation_kk,
                            raw_response = EXCLUDED.raw_response
                        """,
                        (
                            claim_id,
                            best.get("url"),
                            best.get("source"),
                            best.get("similarity_score", 0.0),
                            verdict,
                            result.get("explanation_kk", ""),
                            json.dumps(result, ensure_ascii=False),
                        ),
                    )
                conn.commit()
                total_verified += 1
            except Exception as e:
                conn.rollback()
                logger.error(f"  DB error storing verification: {e}")

    conn.close()

    logger.info(f"\n{'='*60}")
    logger.info(f"DONE!")
    logger.info(f"  Total claims extracted: {total_claims}")
    logger.info(f"  Total verified: {total_verified}")
    logger.info(f"  Verdicts: {verdict_counts}")


if __name__ == "__main__":
    main()
