"""
KZ Fact-Checker — Streamlit UI (v2: Multi-Source)

Two tabs:
  1. 🔍 ZTB тексеру — Latest ZTB articles with verification results + manual claim check
  2. 📊 Білім қоры — Knowledge base stats per source
"""
import os
import json
from html import escape

import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://api:8000")

# ── Page Config ──────────────────────────────────────────────
st.set_page_config(
    page_title="KZ Fact-Checker",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ───────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

.main-title {
    font-size: 2.5rem;
    font-weight: 700;
    text-align: center;
    margin-bottom: 0.5rem;
}

.subtitle {
    text-align: center;
    color: #94a3b8;
    font-size: 1.1rem;
    margin-bottom: 2rem;
}

.verdict-badge {
    display: inline-block;
    padding: 0.4rem 1.2rem;
    border-radius: 20px;
    font-weight: 700;
    font-size: 0.9rem;
}

.verdict-SUPPORTED { background: rgba(16, 185, 129, 0.15); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.3); }
.verdict-REFUTED { background: rgba(239, 68, 68, 0.15); color: #ef4444; border: 1px solid rgba(239, 68, 68, 0.3); }
.verdict-NOT_ENOUGH_INFO { background: rgba(245, 158, 11, 0.15); color: #f59e0b; border: 1px solid rgba(245, 158, 11, 0.3); }

.source-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 10px;
    font-size: 0.75rem;
    font-weight: 600;
    background: rgba(99, 102, 241, 0.12);
    color: #818cf8;
    border: 1px solid rgba(99, 102, 241, 0.2);
}

.stat-card {
    background: rgba(30, 41, 59, 0.5);
    border: 1px solid rgba(148, 163, 184, 0.15);
    border-radius: 12px;
    padding: 1.2rem;
    text-align: center;
}

.stat-number {
    font-size: 2rem;
    font-weight: 700;
    color: #818cf8;
}

.stat-label {
    font-size: 0.85rem;
    color: #94a3b8;
    margin-top: 0.3rem;
}

.custom-divider {
    border-top: 1px solid rgba(148, 163, 184, 0.15);
    margin: 1rem 0;
}

.evidence-card {
    background: rgba(30, 41, 59, 0.4);
    border-radius: 10px;
    padding: 1rem;
    margin-top: 0.5rem;
}

.explanation-box {
    background: rgba(99, 102, 241, 0.08);
    border: 1px solid rgba(99, 102, 241, 0.2);
    border-radius: 12px;
    padding: 1.2rem;
    margin-top: 0.8rem;
    line-height: 1.7;
}

/* Article card with color indicator */
.article-card {
    border-radius: 12px;
    padding: 1rem 1.2rem;
    margin-bottom: 0.8rem;
    position: relative;
}

.article-card .title-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0.5rem;
}

.article-card .title-text {
    font-weight: 700;
    font-size: 1.05rem;
}

.article-card .date-text {
    color: #64748b;
    font-size: 0.8rem;
}

.article-card .claim-row {
    margin: 6px 0;
    padding: 6px 0;
    border-bottom: 1px solid rgba(148, 163, 184, 0.1);
}

.article-card .link-row {
    margin-top: 0.5rem;
}

/* Color indicator dot */
.color-dot {
    display: inline-block;
    width: 12px;
    height: 12px;
    border-radius: 50%;
    margin-right: 6px;
    vertical-align: middle;
}
.dot-green { background-color: #10b981; box-shadow: 0 0 6px rgba(16, 185, 129, 0.5); }
.dot-red { background-color: #ef4444; box-shadow: 0 0 6px rgba(239, 68, 68, 0.5); }
.dot-yellow { background-color: #f59e0b; box-shadow: 0 0 6px rgba(245, 158, 11, 0.5); }
.dot-gray { background-color: #64748b; }

/* Confidence bar */
.conf-bar-bg {
    display: inline-block;
    width: 60px;
    height: 6px;
    background: rgba(148, 163, 184, 0.2);
    border-radius: 3px;
    margin-left: 8px;
    vertical-align: middle;
}
.conf-bar-fill {
    height: 100%;
    border-radius: 3px;
}
</style>
""", unsafe_allow_html=True)

# ── Header ───────────────────────────────────────────────────
st.markdown('<h1 class="main-title">🔍 KZ Fact-Checker</h1>', unsafe_allow_html=True)
st.markdown(
    '<p class="subtitle">ZTB.kz мақалаларын 4 сенімді дерек көзімен тексеру<br>'
    '<small>Azattyq · Factcheck.kz · Informburo · Tengrinews</small></p>',
    unsafe_allow_html=True,
)

# ── Tabs ─────────────────────────────────────────────────────
tab_ztb, tab_check, tab_stats = st.tabs([
    "📰 ZTB тексеру нәтижелері",
    "🔍 Мәлімдемені тексеру",
    "📊 Білім қоры",
])


def _verdict_badge(verdict: str) -> str:
    """Return HTML badge for a verdict."""
    if verdict == "SUPPORTED":
        return '<span style="background:rgba(16,185,129,0.15);color:#10b981;padding:2px 10px;border-radius:12px;font-weight:700;font-size:0.8rem;">✅ РАСТАЛДЫ</span>'
    elif verdict == "REFUTED":
        return '<span style="background:rgba(239,68,68,0.15);color:#ef4444;padding:2px 10px;border-radius:12px;font-weight:700;font-size:0.8rem;">❌ ЖАЛҒАН</span>'
    else:
        return '<span style="background:rgba(245,158,11,0.15);color:#f59e0b;padding:2px 10px;border-radius:12px;font-weight:700;font-size:0.8rem;">⚠️ АҚПАРАТ ЖЕТКІЛІКСІЗ</span>'


def _verdict_dot(verdict: str) -> str:
    """Return small colored dot HTML for a verdict."""
    if verdict == "SUPPORTED":
        return '<span class="color-dot dot-green"></span>'
    elif verdict == "REFUTED":
        return '<span class="color-dot dot-red"></span>'
    elif verdict == "NOT_ENOUGH_INFO":
        return '<span class="color-dot dot-yellow"></span>'
    else:
        return '<span class="color-dot dot-gray"></span>'


def _conf_bar(confidence: float, verdict: str) -> str:
    """Return a mini confidence bar HTML."""
    if not confidence:
        return ""
    pct = int(confidence * 100)
    if verdict == "SUPPORTED":
        color = "#10b981"
    elif verdict == "REFUTED":
        color = "#ef4444"
    else:
        color = "#f59e0b"
    return (
        f'<span class="conf-bar-bg">'
        f'<span class="conf-bar-fill" style="width:{pct}%;background:{color};"></span>'
        f'</span>'
        f' <span style="font-size:0.75rem;color:#94a3b8;">{pct}%</span>'
    )


def _claim_evidence_html(evidence: list[dict]) -> str:
    """Render compact list of evidence source links for a claim."""
    if not evidence:
        return ""

    links = []
    for ev in evidence[:6]:
        if not isinstance(ev, dict):
            continue
        source = escape((ev.get("source") or "дереккөз").strip())
        title = escape((ev.get("title") or source).strip())
        url = (ev.get("url") or "").strip()
        similarity = ev.get("similarity_score")

        sim_text = ""
        if isinstance(similarity, (int, float)):
            sim_text = f" ({int(similarity * 100)}%)"

        if url:
            links.append(
                f'<a href="{escape(url)}" target="_blank" title="{title}" '
                f'style="color:#93c5fd;text-decoration:none;">{source}</a>{sim_text}'
            )
        else:
            links.append(f'<span style="color:#93c5fd;">{source}</span>{sim_text}')

    if not links:
        return ""

    return (
        '<div style="margin-top:6px;color:#94a3b8;font-size:0.78rem;">'
        '📚 Дереккөздер: ' + " · ".join(links) + "</div>"
    )


def _score_percent(value) -> str:
    """Format score [0..1] as percentage string."""
    if not isinstance(value, (int, float)):
        return "—"
    pct = int(max(0.0, min(1.0, float(value))) * 100)
    return f"{pct}%"


def _display_date(value: str | None) -> str:
    """Show YYYY-MM-DD for API date strings."""
    if not value:
        return "—"
    return escape(str(value)[:10])


def _render_evidence_cards(evidence: list[dict]) -> None:
    """Render evidence cards with source, date, link, similarity, and snippet."""
    if not evidence:
        return

    st.markdown("#### 📚 Табылған дереккөздер:")
    for ev in evidence:
        if not isinstance(ev, dict):
            continue

        source_name = escape((ev.get("source") or "?").strip())
        title = escape((ev.get("title") or "Дереккөз").strip())
        url = (ev.get("url") or "").strip()
        similarity_text = _score_percent(ev.get("similarity_score"))
        published_at = _display_date(ev.get("published_at"))
        snippet = escape((ev.get("snippet") or "").strip()[:240])

        title_html = f"<strong>{title}</strong>"
        if url:
            title_html = (
                f'<a href="{escape(url)}" target="_blank" '
                f'style="color:#cbd5e1;text-decoration:none;font-weight:700;">{title}</a>'
            )

        meta_parts = [f"Ұқсастық: {similarity_text}"]
        if published_at != "—":
            meta_parts.append(f"Күні: {published_at}")

        snippet_html = f"<br>{snippet}..." if snippet else ""
        st.markdown(
            f'<div class="evidence-card">'
            f'<span class="source-badge">{source_name}</span> '
            f'{title_html}<br>'
            f'<small>{" · ".join(meta_parts)}</small>'
            f'{snippet_html}'
            f'</div>',
            unsafe_allow_html=True,
        )


def _render_primary_article(best_match: dict) -> None:
    """Render the main article link for the current verification result."""
    if not isinstance(best_match, dict):
        return

    url = (best_match.get("url") or "").strip()
    if not url:
        return

    title = escape((best_match.get("title") or "Дереккөз мақаласы").strip())
    source = escape((best_match.get("source") or "").strip())
    published_at = _display_date(best_match.get("published_at"))

    meta_parts = []
    if source:
        meta_parts.append(source)
    if published_at != "—":
        meta_parts.append(f"Күні: {published_at}")

    meta_line = f'<div style="margin-top:0.25rem;color:#94a3b8;font-size:0.82rem;">{" · ".join(meta_parts)}</div>' if meta_parts else ""
    st.markdown(
        f'<div style="margin:0.75rem 0 0.6rem;padding:0.8rem 1rem;'
        f'background:rgba(59,130,246,0.08);border:1px solid rgba(59,130,246,0.18);'
        f'border-radius:12px;">'
        f'<div style="color:#94a3b8;font-size:0.72rem;font-weight:600;'
        f'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:0.3rem;">'
        f'🔗 НЕГІЗГІ МАҚАЛА</div>'
        f'<a href="{escape(url)}" target="_blank" '
        f'style="color:#93c5fd;text-decoration:none;font-weight:700;">{title}</a>'
        f'{meta_line}'
        f'</div>',
        unsafe_allow_html=True,
    )


def _aggregate_article_sources(claims: list[dict]) -> list[dict]:
    """Aggregate unique evidence sources across all claims of one article."""
    aggregated: dict[str, dict] = {}

    for claim in claims:
        evidence = claim.get("evidence", [])
        if not isinstance(evidence, list):
            continue

        for ev in evidence:
            if not isinstance(ev, dict):
                continue
            source = (ev.get("source") or claim.get("source_name") or "дереккөз").strip()
            url = (ev.get("url") or claim.get("source_url") or "").strip()
            title = (ev.get("title") or source).strip()
            similarity = ev.get("similarity_score")
            similarity = similarity if isinstance(similarity, (int, float)) else None

            key = source or "дереккөз"
            if key not in aggregated:
                aggregated[key] = {
                    "source": source,
                    "url": url,
                    "title": title,
                    "hits": 0,
                    "best_similarity": similarity,
                }
            aggregated[key]["hits"] += 1
            if similarity is not None:
                prev = aggregated[key]["best_similarity"]
                if prev is None or similarity > prev:
                    aggregated[key]["best_similarity"] = similarity
                    if url:
                        aggregated[key]["url"] = url
                        aggregated[key]["title"] = title

    result = list(aggregated.values())
    result.sort(key=lambda x: (x["hits"], x["best_similarity"] or -1), reverse=True)
    return result


def _article_border_style(verdicts: list[str]) -> tuple[str, str, str]:
    """Determine border color, bg color, and status icon based on dominant verdict."""
    has_refuted = "REFUTED" in verdicts
    has_supported = "SUPPORTED" in verdicts

    if has_refuted:
        return "#ef4444", "rgba(239, 68, 68, 0.06)", "❌"
    elif has_supported:
        return "#10b981", "rgba(16, 185, 129, 0.06)", "✅"
    else:
        return "#f59e0b", "rgba(245, 158, 11, 0.06)", "⚠️"


# ── TAB 1: ZTB Verification Results ─────────────────────────
with tab_ztb:
    st.markdown("### 📰 ZTB.kz мақалаларының тексеру нәтижелері")

    if st.button("🔄 Жаңарту", key="refresh_ztb", use_container_width=False):
        st.rerun()

    try:
        with st.spinner("Деректер жүктелуде..."):
            resp = requests.get(f"{API_URL}/ztb_results?limit=50", timeout=30)
            if resp.status_code == 200:
                payload = resp.json()
                ztb_articles = payload.get("ztb_results", [])
                total_verified_articles = payload.get("total_verified_articles", len(ztb_articles))

                if not ztb_articles:
                    st.info("Әзірге тексерілген ZTB мақалалары жоқ. DAG 'news_ingest' іске қосылғанша күтіңіз.")
                else:
                    st.markdown(
                        f'<div class="stat-card" style="margin-bottom:1rem;">'
                        f'<div class="stat-number">{total_verified_articles}</div>'
                        f'<div class="stat-label">Тексерілген мақалалар (✅ Расталды / ❌ Жалған)</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

                    for article in ztb_articles:
                        claims = article.get("claims", [])
                        verdicts_list = [c.get("verdict", "") for c in claims]
                        border_color, bg_color, status_icon = _article_border_style(verdicts_list)

                        title = escape(article.get("title", "Untitled"))
                        pub_date = (article.get("published_at") or "")[:10]
                        date_html = f' · <span style="color:#64748b;font-size:0.82rem;">{pub_date}</span>' if pub_date else ""
                        article_url = escape(article.get("url", ""))

                        # ── 1. ZTB Article Block ──
                        st.markdown(
                            f'<div style="background:{bg_color};border-left:5px solid {border_color};'
                            f'border-radius:12px;padding:1rem 1.2rem;margin-bottom:0.4rem;">'
                            f'<div style="color:#94a3b8;font-size:0.72rem;font-weight:600;'
                            f'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:0.4rem;">'
                            f'📰 ZTB.KZ МАҚАЛА</div>'
                            f'<div style="font-weight:700;font-size:1.05rem;color:#e2e8f0;'
                            f'line-height:1.4;margin-bottom:0.4rem;">'
                            f'{status_icon} {title[:140]}{date_html}</div>'
                            f'<a href="{article_url}" target="_blank" '
                            f'style="color:#818cf8;font-size:0.82rem;text-decoration:none;">'
                            f'🔗 Мақаланы оқу →</a>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )

                        # ── 2. Each Claim ──
                        for idx, c in enumerate(claims, 1):
                            verdict = c.get("verdict", "")
                            claim_text = escape((c.get("claim_text", "") or "")[:280])
                            explanation = escape((c.get("explanation_kk", "") or "").strip())
                            source_name = escape((c.get("source_name", "") or "").strip())
                            source_url = (c.get("source_url", "") or "").strip()
                            source_title = escape((c.get("source_title", "") or "").strip())
                            confidence = c.get("confidence")
                            retrieval_score = c.get("retrieval_score")

                            # Verdict color
                            if verdict == "SUPPORTED":
                                v_color = "#10b981"
                                v_bg = "rgba(16,185,129,0.1)"
                                v_label = "✅ РАСТАЛДЫ"
                                v_border = "rgba(16,185,129,0.3)"
                            elif verdict == "REFUTED":
                                v_color = "#ef4444"
                                v_bg = "rgba(239,68,68,0.1)"
                                v_label = "❌ ЖАЛҒАН"
                                v_border = "rgba(239,68,68,0.3)"
                            else:
                                v_color = "#f59e0b"
                                v_bg = "rgba(245,158,11,0.1)"
                                v_label = "⚠️ АҚПАРАТ ЖЕТКІЛІКСІЗ"
                                v_border = "rgba(245,158,11,0.3)"

                            # Claim text section
                            st.markdown(
                                f'<div style="margin:0 0 0 1.4rem;padding:0.8rem 1rem;'
                                f'background:rgba(30,41,59,0.35);border-radius:10px;'
                                f'border-left:3px solid rgba(148,163,184,0.2);margin-bottom:0.3rem;">'
                                f'<div style="color:#94a3b8;font-size:0.7rem;font-weight:600;'
                                f'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:0.3rem;">'
                                f'💬 #{idx} МӘЛІМДЕМЕ</div>'
                                f'<div style="color:#e2e8f0;font-size:0.92rem;line-height:1.5;">'
                                f'{claim_text}</div>'
                                f'</div>',
                                unsafe_allow_html=True,
                            )

                            # Source info section
                            source_link_html = ""
                            if source_url:
                                source_display_title = source_title if source_title else source_url[:60]
                                source_link_html = (
                                    f'<a href="{escape(source_url)}" target="_blank" '
                                    f'style="color:#93c5fd;font-size:0.82rem;text-decoration:none;">'
                                    f'📄 {source_display_title[:100]}</a>'
                                )

                            sim_html = ""
                            if isinstance(retrieval_score, (int, float)) and retrieval_score > 0:
                                sim_pct = int(retrieval_score * 100)
                                sim_html = (
                                    f'<span style="color:#94a3b8;font-size:0.78rem;margin-left:8px;">'
                                    f'Ұқсастық: {sim_pct}%</span>'
                                )

                            source_badge_html = ""
                            if source_name:
                                source_badge_html = (
                                    f'<span style="background:rgba(99,102,241,0.15);color:#818cf8;'
                                    f'padding:3px 10px;border-radius:10px;font-size:0.75rem;'
                                    f'font-weight:600;">{source_name}</span>'
                                )

                            if source_name or source_url:
                                st.markdown(
                                    f'<div style="margin:0 0 0 1.4rem;padding:0.6rem 1rem;'
                                    f'background:rgba(99,102,241,0.05);border-radius:10px;'
                                    f'border-left:3px solid rgba(99,102,241,0.25);margin-bottom:0.3rem;">'
                                    f'<div style="color:#94a3b8;font-size:0.7rem;font-weight:600;'
                                    f'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:0.3rem;">'
                                    f'📚 ДЕРЕККӨЗ</div>'
                                    f'{source_badge_html}{sim_html}'
                                    + (f'<div style="margin-top:0.3rem;">{source_link_html}</div>' if source_link_html else '')
                                    + f'</div>',
                                    unsafe_allow_html=True,
                                )

                            # Verdict section
                            conf_html = ""
                            if isinstance(confidence, (int, float)) and confidence > 0:
                                conf_pct = int(confidence * 100)
                                conf_html = (
                                    f'<span style="color:#94a3b8;font-size:0.78rem;margin-left:10px;">'
                                    f'Сенімділік: {conf_pct}%</span>'
                                )

                            st.markdown(
                                f'<div style="margin:0 0 0.6rem 1.4rem;padding:0.7rem 1rem;'
                                f'background:{v_bg};border-radius:10px;'
                                f'border-left:3px solid {v_border};">'
                                f'<div style="color:#94a3b8;font-size:0.7rem;font-weight:600;'
                                f'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:0.3rem;">'
                                f'⚖️ ВЕРДИКТ</div>'
                                f'<span style="color:{v_color};font-weight:700;font-size:1rem;">'
                                f'{v_label}</span>{conf_html}'
                                + (
                                    f'<div style="color:#cbd5e1;font-size:0.84rem;margin-top:0.4rem;'
                                    f'line-height:1.5;">📝 {explanation[:400]}</div>'
                                    if explanation else ''
                                )
                                + f'</div>',
                                unsafe_allow_html=True,
                            )

                        st.markdown(
                            '<div style="border-top:1px solid rgba(148,163,184,0.12);'
                            'margin:1rem 0 1.2rem;"></div>',
                            unsafe_allow_html=True,
                        )

            else:
                st.error(f"API қатесі: {resp.status_code}")
    except Exception as e:
        st.error(f"Деректерді алу мүмкін болмады: {e}")


# ── TAB 2: Manual Claim Check ────────────────────────────────
with tab_check:
    st.markdown("### 🔍 Мәлімдемені қолмен тексеру")
    st.markdown("Кез-келген мәтінді немесе мәлімдемені білім қорымен тексеріңіз.")

    input_mode = st.radio(
        "Режим:", ["Жеке мәлімдеме", "Мәтіннен фактілерді шығару"],
        horizontal=True, key="check_mode"
    )

    if input_mode == "Жеке мәлімдеме":
        claim_text = st.text_area(
            "Тексерілетін мәлімдеме:",
            placeholder="Мысалы: Қазақстанда 2025 жылы ЖІӨ 5% өсті",
            height=100,
            key="single_claim",
        )

        if st.button("🔍 Тексеру", key="check_btn", use_container_width=True) and claim_text.strip():
            with st.spinner("Тексерілуде..."):
                try:
                    resp = requests.post(
                        f"{API_URL}/check",
                        json={"claim": claim_text, "top_k": 5, "similarity_threshold": 0.5},
                        timeout=60,
                    )
                    resp.raise_for_status()
                    result = resp.json()

                    verdict = result.get("verdict", "NOT_ENOUGH_INFO")
                    st.markdown(
                        f'<div class="verdict-badge verdict-{verdict}" style="font-size:1.3rem; padding:0.6rem 1.5rem;">'
                        f'{"✅ РАСТАЛДЫ" if verdict == "SUPPORTED" else "❌ ЖАЛҒАН" if verdict == "REFUTED" else "⚠️ АҚПАРАТ ЖЕТКІЛІКСІЗ"}'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

                    conf = result.get("confidence", 0)
                    if conf:
                        st.progress(conf, text=f"Сенімділік: {int(conf * 100)}%")

                    exp = result.get("explanation_kk")
                    if exp:
                        st.markdown(f'<div class="explanation-box">📝 {exp}</div>', unsafe_allow_html=True)

                    _render_primary_article(result.get("best_match", {}))

                    evidence = result.get("evidence", [])
                    _render_evidence_cards(evidence)

                    with st.expander("🛠 Raw JSON"):
                        st.json(result)

                except Exception as e:
                    st.error(f"Тексеру қатесі: {e}")

    else:  # Extract from text
        post_text = st.text_area(
            "Мәтін:",
            placeholder="Мақала немесе пост мәтінін кірістіріңіз...",
            height=150,
            key="extract_input",
        )

        if st.button("📋 Фактілерді тауып тексеру", key="extract_btn", use_container_width=True) and post_text.strip():
            with st.spinner("🔄 Мәлімдемелер шығарылуда..."):
                try:
                    resp = requests.post(
                        f"{API_URL}/extract_claims",
                        json={"text": post_text},
                        timeout=60,
                    )
                    resp.raise_for_status()
                    extract_result = resp.json()
                except Exception as e:
                    st.error(f"Шығару қатесі: {e}")
                    st.stop()

            claims = extract_result.get("claims", [])
            topics = extract_result.get("topics", [])

            if topics:
                st.markdown("**🏷 Тақырыптар:** " + " · ".join([f"`{t}`" for t in topics]))

            if not claims:
                st.info("Тексерілетін мәлімдемелер табылмады.")
            else:
                st.markdown(f"### 📌 {len(claims)} мәлімдеме табылды:")
                for i, claim_text in enumerate(claims, 1):
                    with st.spinner(f"🔍 #{i} тексерілуде..."):
                        try:
                            check_resp = requests.post(
                                f"{API_URL}/check",
                                json={"claim": claim_text, "top_k": 5, "similarity_threshold": 0.5},
                                timeout=60,
                            )
                            check_resp.raise_for_status()
                            result = check_resp.json()
                        except Exception as e:
                            st.error(f"#{i} тексеру қатесі: {e}")
                            continue

                    verdict = result.get("verdict", "NOT_ENOUGH_INFO")
                    verdict_map = {
                        "SUPPORTED": "✅ РАСТАЛДЫ",
                        "REFUTED": "❌ ЖАЛҒАН",
                        "NOT_ENOUGH_INFO": "⚠️ АҚПАРАТ ЖЕТКІЛІКСІЗ",
                    }

                    border_color = "#10b981" if verdict == "SUPPORTED" else "#ef4444" if verdict == "REFUTED" else "#f59e0b"

                    st.markdown(f"""
                    <div class="evidence-card" style="border-left: 4px solid {border_color};">
                        <strong>#{i}</strong> {claim_text}
                        <br><span class="verdict-badge verdict-{verdict}">{verdict_map.get(verdict, verdict)}</span>
                    </div>
                    """, unsafe_allow_html=True)

                    conf = result.get("confidence", 0)
                    if conf:
                        st.progress(conf, text=f"Сенімділік: {int(conf * 100)}%")

                    exp = result.get("explanation_kk")
                    if exp:
                        st.caption(f"📝 {exp}")

                    _render_primary_article(result.get("best_match", {}))

                    evidence = result.get("evidence", [])
                    _render_evidence_cards(evidence)

                    st.markdown("---")


# ── TAB 3: Knowledge Base Stats ──────────────────────────────
with tab_stats:
    st.markdown("### 📊 Білім қоры статистикасы")
    st.markdown("4 сенімді дерек көзінен жиналған мақалалар мен эмбеддед фрагменттер.")

    if st.button("🔄 Жаңарту", key="refresh_stats", use_container_width=False):
        st.rerun()

    try:
        with st.spinner("Статистика жүктелуде..."):
            resp = requests.get(f"{API_URL}/knowledge_stats", timeout=15)
            if resp.status_code == 200:
                stats = resp.json()
                articles = stats.get("articles_per_source", {})
                chunks = stats.get("chunks_per_source", {})

                source_labels = {
                    "factcheck": "🛡 Factcheck.kz",
                    "azattyq": "📻 Azattyq.org",
                    "informburo": "📰 Informburo.kz",
                    "tengrinews": "🌐 Tengrinews.kz",
                    "ztb": "📋 ZTB.kz",
                }

                # Summary row
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    total_articles = sum(articles.values())
                    st.markdown(f'<div class="stat-card"><div class="stat-number">{total_articles}</div><div class="stat-label">Барлық мақалалар</div></div>', unsafe_allow_html=True)
                with col2:
                    total_chunks = sum(chunks.values())
                    st.markdown(f'<div class="stat-card"><div class="stat-number">{total_chunks}</div><div class="stat-label">Эмбеддед фрагменттер</div></div>', unsafe_allow_html=True)
                with col3:
                    st.markdown(f'<div class="stat-card"><div class="stat-number">{stats.get("total_ztb_claims", 0)}</div><div class="stat-label">ZTB мәлімдемелері</div></div>', unsafe_allow_html=True)
                with col4:
                    st.markdown(
                        f'<div class="stat-card"><div class="stat-number">{stats.get("total_verified_true_false_articles", 0)}</div>'
                        f'<div class="stat-label">True/False тексерілген ZTB мақалалары</div></div>',
                        unsafe_allow_html=True,
                    )

                st.markdown('<div class="custom-divider"></div>', unsafe_allow_html=True)

                # Per-source breakdown
                st.markdown("#### Дерек көздері бойынша:")
                for src_key in ["factcheck", "azattyq", "informburo", "tengrinews", "ztb"]:
                    label = source_labels.get(src_key, src_key)
                    n_articles = articles.get(src_key, 0)
                    n_chunks = chunks.get(src_key, 0)

                    col_a, col_b, col_c = st.columns([3, 1, 1])
                    with col_a:
                        st.markdown(f"**{label}**")
                    with col_b:
                        st.metric("Мақалалар", n_articles)
                    with col_c:
                        st.metric("Фрагменттер", n_chunks)

                st.markdown("#### 🕒 Соңғы 1 сағатта қосылған мақалалар:")
                last_hour_articles = stats.get("last_hour_articles_per_source", {})
                last_hour_claims = stats.get("last_hour_ztb_claims", 0)
                last_hour_verified_articles = stats.get("last_hour_verified_true_false_articles", 0)
                last_hour_verifications = stats.get("last_hour_verifications", {})
                last_hour_article_list = stats.get("last_hour_article_list", [])

                col_h1, col_h2, col_h3, col_h4 = st.columns(4)
                with col_h1:
                    st.metric("Жаңа мақалалар (1h)", sum(last_hour_articles.values()))
                with col_h2:
                    st.metric("Жаңа ZTB claim (1h)", last_hour_claims)
                with col_h3:
                    st.metric("True/False мақала (1h)", last_hour_verified_articles)
                with col_h4:
                    st.metric("Жалпы verification (1h)", last_hour_verifications.get("total", 0))

                if last_hour_article_list:
                    # Group by source
                    by_source = {}
                    for art in last_hour_article_list:
                        src = art.get("source", "?")
                        by_source.setdefault(src, []).append(art)

                    for src_key in ["factcheck", "azattyq", "informburo", "tengrinews", "ztb"]:
                        articles_in_src = by_source.get(src_key, [])
                        if not articles_in_src:
                            continue
                        label = source_labels.get(src_key, src_key)
                        st.markdown(f"**{label}** — {len(articles_in_src)} жаңа мақала:")
                        for art in articles_in_src[:10]:
                            art_title = (art.get("title") or "Untitled")[:80]
                            art_url = art.get("url", "")
                            art_time = (art.get("created_at") or "")[:16].replace("T", " ")
                            if art_url:
                                st.markdown(
                                    f'<div style="margin-left:1rem;padding:4px 0;">'
                                    f'<a href="{art_url}" target="_blank" '
                                    f'style="color:#93c5fd;text-decoration:none;">'
                                    f'📄 {art_title}</a> '
                                    f'<span style="color:#64748b;font-size:0.75rem;">{art_time}</span>'
                                    f'</div>',
                                    unsafe_allow_html=True,
                                )
                            else:
                                st.caption(f"📄 {art_title} · {art_time}")
                else:
                    st.info("Соңғы 1 сағатта жаңа мақала қосылмады.")
            else:
                st.error(f"API қатесі: {resp.status_code}")
    except Exception as e:
        st.error(f"Статистиканы алу мүмкін болмады: {e}")


# ── Footer ───────────────────────────────────────────────────
st.markdown('<div class="custom-divider"></div>', unsafe_allow_html=True)
st.markdown(
    '<p style="text-align:center; color:#64748b; font-size:0.85rem;">'
    '🛡 KZ Fact-Checker v2 · Деректер көзі: Factcheck.kz · Azattyq · Informburo · Tengrinews · '
    'Data Engineering Project</p>',
    unsafe_allow_html=True,
)
