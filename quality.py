"""
demo/quality.py
---------------
Phase 09: Quality  (DEMO VERSION)

Evaluates every generated answer on two dimensions:

  Alignment   — does the answer actually address the question?
                Score 0.0–1.0. Checks question keywords appear
                in the answer and answer is relevant to the theme.

  Faithfulness — is the answer grounded in the enriched prompt context?
                 Score 0.0–1.0. If KB context was provided, checks the
                 answer references concepts from it. If no context,
                 always passes (nothing to be unfaithful to).

Both scores must meet the threshold for QUALITY_PASSED.
If either fails:
  - iteration < MAX_QUALITY_ITERATIONS → route back to Response
  - iteration >= MAX_QUALITY_ITERATIONS → quality_flagged=True,
    forward to Recomposition (validator sees the flag)

In production: RAGAS framework with a judge LLM.
In demo:       deterministic scoring rules — honest about the
               approximation, but covers all code paths correctly.

Constants (non-negotiable, from spec):
  MAX_QUALITY_ITERATIONS = 2
  ALIGNMENT_THRESHOLD    = 0.40
  FAITHFULNESS_THRESHOLD = 0.35

Run:
    python3 demo/reception.py           ← port 8000
    ...
    python3 demo/response.py            ← port 8007
    python3 demo/quality.py             ← port 8008

Dashboard: http://localhost:8008
"""

from __future__ import annotations

import html as _html
import http.server
import json
import re
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ─────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────

DEMO_DIR          = Path(__file__).parent
INGESTION_DB_PATH = DEMO_DIR / "demo_ingestion.db"
DECOMP_DB_PATH    = DEMO_DIR / "demo_decomposition.db"
QUALITY_DB_PATH   = DEMO_DIR / "demo_quality.db"
PORT              = 8008

# ── Non-negotiable constants (identical to production) ────────
MAX_QUALITY_ITERATIONS = 2
ALIGNMENT_THRESHOLD    = 0.40
FAITHFULNESS_THRESHOLD = 0.35


# ─────────────────────────────────────────────────────────────
# ALIGNMENT SCORER
#
# In production: RAGAS AnswerRelevancy — a judge LLM generates
#   synthetic questions from the answer and measures cosine
#   similarity back to the original question.
#
# In demo: two signals combined into a 0.0–1.0 score.
#
#   Signal 1 — keyword overlap
#     How many meaningful words from the question appear in the answer?
#     A good answer addresses what was actually asked.
#
#   Signal 2 — theme relevance
#     Does the answer contain words associated with the question's theme?
#     Catches generic filler answers that don't address the domain.
# ─────────────────────────────────────────────────────────────

# Stopwords to exclude from keyword overlap (DE/FR/IT/EN)
_STOPWORDS = frozenset({
    "ich","sie","er","wir","ihr","die","der","das","ein","eine","und","oder",
    "ist","sind","hat","haben","wird","werden","dass","mit","für","von","bei",
    "auf","in","an","zu","nach","aus","aber","auch","noch","schon","nur",
    "je","vous","nous","les","des","une","est","sont","avec","pour","par",
    "dans","sur","qui","que","mais","aussi","encore","plus","très",
    "io","lei","noi","gli","una","con","per","nel","sul","che","anche",
    "i","the","a","an","and","or","is","are","has","have","will","with",
    "for","from","at","in","on","to","of","but","also","just","very",
    "bitte","sehr","guten","tag","guten","monsieur","madame","bonjour",
    "merci","grazie","please","dear","kind","regards","sincerely",
})

# Theme keyword sets for Signal 2
_THEME_WORDS: dict[str, list[str]] = {
    "DRIVERS_LICENSE": [
        "führerausweis","fahrerausweis","strassenverkehrsamt","stva",
        "permis","conduire","fahrerlaubnis","licence","driving",
        "kantonal","kanton","mfk","motorfahrzeugkontrolle",
    ],
    "ROAD_INFRASTRUCTURE": [
        "nationalstrasse","autobahn","astra","schäden","melden",
        "strasse","markierung","kontaktformular","notruf","140",
        "route","nationale","dommage","signalement",
    ],
    "NOISE_PROTECTION": [
        "lärm","lärmschutz","lärmsanierung","lärmschutzmassnahmen",
        "bruit","antibruit","schallschutz","gebietseinheit",
    ],
    "TUNNEL_SAFETY": [
        "tunnel","sicherheit","notausgang","pannenbuchten",
        "sécurité","tunnelsécurité","brandfall",
    ],
    "VEHICLE_REGISTRATION": [
        "fahrzeug","zulassung","immatrikulation","kontrollschild",
        "véhicule","immatriculation","targa",
    ],
    "GENERAL_INQUIRY": [
        "astra","kontakt","information","www.astra.admin.ch",
        "anfrage","auskunft","renseignement",
    ],
}


def score_alignment(question: str, answer: str, theme: str) -> tuple[float, dict]:
    """
    Return (score 0.0-1.0, signals_dict).

    Signal 1: keyword overlap  (weight 0.6)
    Signal 2: theme relevance  (weight 0.4)
    """
    if not answer:
        return 0.0, {"reason": "empty answer"}

    q_tokens = _meaningful_tokens(question)
    a_lower  = answer.lower()

    # Signal 1 — keyword overlap
    if q_tokens:
        hits    = sum(1 for t in q_tokens if t in a_lower)
        overlap = hits / len(q_tokens)
    else:
        # All question words were anonymised (<PERSON_1>, <DATE_1> etc.)
        # Fall back to theme relevance only — can't penalise for anonymisation
        overlap = 0.7   # generous neutral score for unanswerable token situation

    # Signal 2 — theme relevance
    theme_words = _THEME_WORDS.get(theme, [])
    if theme_words:
        theme_hits = sum(1 for w in theme_words if w in a_lower)
        theme_rel  = min(theme_hits / 3, 1.0)   # cap at 3 hits = 1.0
    else:
        theme_rel = 0.5

    score = round(overlap * 0.6 + theme_rel * 0.4, 3)

    signals = {
        "q_tokens":     q_tokens,
        "kw_hits":      hits if q_tokens else 0,
        "kw_overlap":   round(overlap, 3),
        "theme_hits":   theme_hits if theme_words else 0,
        "theme_rel":    round(theme_rel, 3),
        "score":        score,
    }
    return score, signals


def _meaningful_tokens(text: str) -> list[str]:
    """Extract non-stopword tokens of length > 3, lowercased."""
    words = re.findall(r"\b[a-zäöüéàèùâêîôûß]{4,}\b", text.lower())
    return [w for w in words if w not in _STOPWORDS]


# ─────────────────────────────────────────────────────────────
# FAITHFULNESS SCORER
#
# In production: RAGAS Faithfulness — claims in the answer are
#   extracted and each verified against the context by a judge LLM.
#
# In demo: if enriched_prompt contains KB context, check that the
#   answer shares at least some vocabulary with the context.
#   If no KB context was injected, faithfulness is N/A → passes.
# ─────────────────────────────────────────────────────────────

def score_faithfulness(answer: str,
                       enriched_prompt: str | None) -> tuple[float, dict]:
    """
    Return (score 0.0-1.0, signals_dict).

    If no KB context in the enriched_prompt → score = 1.0 (N/A).
    If KB context present → measure vocabulary overlap with the context.

    Cross-language note: the KB context may be in a different language
    than the answer (e.g. FR context, DE answer). We handle this by:
      1. Checking direct token overlap (same-language cases)
      2. Checking if the answer covers the core topic of the context
         (different-language cases) via ASTRA domain keywords
    """
    if not answer:
        return 0.0, {"reason": "empty answer"}

    has_context = bool(
        enriched_prompt and
        "[CONTEXT — KNOWLEDGE BASE" in enriched_prompt
    )

    if not has_context:
        return 1.0, {
            "has_context": False,
            "reason":      "no KB context — faithfulness N/A",
            "score":       1.0,
        }

    # Extract the context text
    ctx_match = re.search(
        r"\[CONTEXT[^\]]*\]\n(.*?)\n\n\[WARNING\]",
        enriched_prompt,
        re.DOTALL,
    )
    context_text = ctx_match.group(1) if ctx_match else enriched_prompt

    ctx_tokens = set(_meaningful_tokens(context_text))
    ans_tokens = set(_meaningful_tokens(answer))

    if not ctx_tokens:
        return 0.8, {"has_context": True, "reason": "context empty after tokenisation"}

    # Direct overlap
    direct_overlap = len(ctx_tokens & ans_tokens) / max(len(ctx_tokens), 1)

    # Cross-language fallback: check shared ASTRA domain concepts
    # e.g. "permis"/"Führerausweis", "route"/"Strasse", "bruit"/"Lärm"
    _CROSS_LANG = [
        {"permis","conduire","führerausweis","fahrerlaubnis","licence","licenza"},
        {"route","strasse","autobahn","nationalstrasse","nationale","strade"},
        {"bruit","lärm","lärmschutz","antibruit","rumore","lärmsanierung"},
        {"tunnel","sicherheit","sécurité","sicurezza","safety"},
        {"astra","ofrou","ust","kontakt","contact","contatto"},
        {"kantonal","cantonal","cantonale","kanton","canton"},
        {"melden","signaler","segnalare","kontaktformular","meldung"},
    ]
    concept_hits = sum(
        1 for group in _CROSS_LANG
        if (ctx_tokens & group) and (ans_tokens & group)
    )
    concept_score = min(concept_hits / 2, 1.0)   # 1 hit = 0.5, 2 hits = 1.0

    # Take the better of the two approaches
    score = min(round(max(direct_overlap * 2.5, concept_score), 3), 1.0)

    signals = {
        "has_context":    True,
        "ctx_tokens":     len(ctx_tokens),
        "ans_tokens":     len(ans_tokens),
        "direct_overlap": round(direct_overlap, 3),
        "concept_hits":   concept_hits,
        "concept_score":  round(concept_score, 3),
        "shared_tokens":  sorted(ctx_tokens & ans_tokens)[:8],
        "score":          score,
    }
    return score, signals


# ─────────────────────────────────────────────────────────────
# VERDICT
# ─────────────────────────────────────────────────────────────

def compute_verdict(alignment: float, faithfulness: float) -> str:
    """
    QUALITY_PASSED  — both scores above threshold
    QUALITY_FAILED  — at least one score below threshold, retry allowed
    """
    if alignment >= ALIGNMENT_THRESHOLD and faithfulness >= FAITHFULNESS_THRESHOLD:
        return "QUALITY_PASSED"
    return "QUALITY_FAILED"


# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────

def init_quality_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(QUALITY_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS quality_results (
            question_id       TEXT PRIMARY KEY,
            case_id           TEXT NOT NULL,
            question          TEXT NOT NULL,
            theme             TEXT NOT NULL,
            alignment_score   REAL NOT NULL,
            faithfulness_score REAL NOT NULL,
            verdict           TEXT NOT NULL,
            iteration         INTEGER NOT NULL DEFAULT 0,
            quality_flagged   INTEGER NOT NULL DEFAULT 0,
            alignment_signals TEXT,    -- JSON
            faith_signals     TEXT,    -- JSON
            evaluated_at      TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS quality_log (
            id      TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            event   TEXT NOT NULL,
            detail  TEXT,
            ts      TEXT NOT NULL
        );
    """)
    conn.commit()
    return conn


def open_db_ro(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True,
                           check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def case_already_evaluated(qua_conn: sqlite3.Connection,
                            case_id: str) -> bool:
    return qua_conn.execute(
        "SELECT 1 FROM quality_results WHERE case_id=?", (case_id,)
    ).fetchone() is not None


def log_event(qua_conn: sqlite3.Connection,
              case_id: str, event: str, detail: str = "") -> None:
    qua_conn.execute(
        "INSERT INTO quality_log VALUES (?,?,?,?,?)",
        (str(uuid.uuid4()), case_id, event, detail,
         datetime.now(timezone.utc).isoformat()),
    )
    qua_conn.commit()


# ─────────────────────────────────────────────────────────────
# CORE QUALITY LOGIC
# ─────────────────────────────────────────────────────────────

def run_quality(case: dict[str, Any],
                questions: list[dict[str, Any]],
                dec_db_rw: sqlite3.Connection,
                qua_conn:  sqlite3.Connection) -> list[dict[str, Any]]:
    """
    Evaluate alignment + faithfulness for each answered question.
    Handle the feedback loop (back to Response or forward with flag).
    """
    case_id = case["case_id"]
    results = []
    needs_retry  = False   # any question failed and can still retry
    all_flagged  = True    # all failed questions are already at max iter

    for q in questions:
        q_id       = q["id"]
        q_text     = q["question"]
        answer     = q.get("answer") or ""
        theme      = q["theme"]
        enriched   = q.get("enriched_prompt")
        iteration  = q.get("iteration", 0)

        # ── Score ──────────────────────────────────────────────
        align_score, align_signals = score_alignment(q_text, answer, theme)
        faith_score, faith_signals = score_faithfulness(answer, enriched)
        verdict = compute_verdict(align_score, faith_score)

        log_event(qua_conn, case_id, "SCORED",
                  f"q={q_text[:50]} align={align_score:.2f} "
                  f"faith={faith_score:.2f} verdict={verdict}")

        # ── Feedback loop decision ────────────────────────────
        quality_flagged = False

        if verdict == "QUALITY_FAILED":
            if iteration >= MAX_QUALITY_ITERATIONS:
                # Max retries reached — flag and forward
                quality_flagged = True
                new_status      = "QUALITY_FLAGGED"
                log_event(qua_conn, case_id, "QUALITY_FLAGGED",
                          f"q={q_text[:50]} max_iter={MAX_QUALITY_ITERATIONS}")
            else:
                # Send back to Response
                new_status = "QUALITY_FAILED"
                needs_retry  = True
                all_flagged  = False
                log_event(qua_conn, case_id, "QUALITY_RETRY",
                          f"q={q_text[:50]} iter={iteration}→{iteration+1}")
        else:
            new_status = "QUALITY_PASSED"

        # ── Write scores + status back to questions table ────
        dec_db_rw.execute(
            """UPDATE questions
               SET status=?, iteration=?, quality_flagged=?,
                   alignment_score=?, faithful_score=?
               WHERE id=?""",
            (new_status,
             iteration + (1 if verdict == "QUALITY_FAILED" and not quality_flagged else 0),
             1 if quality_flagged else 0,
             align_score, faith_score,
             q_id),
        )

        # ── Save quality result ───────────────────────────────
        result = {
            "question_id":        q_id,
            "case_id":            case_id,
            "question":           q_text,
            "theme":              theme,
            "alignment_score":    align_score,
            "faithfulness_score": faith_score,
            "verdict":            verdict,
            "iteration":          iteration,
            "quality_flagged":    1 if quality_flagged else 0,
            "alignment_signals":  json.dumps(align_signals),
            "faith_signals":      json.dumps(faith_signals),
            "evaluated_at":       datetime.now(timezone.utc).isoformat(),
        }
        qua_conn.execute("""
            INSERT OR REPLACE INTO quality_results VALUES (
                :question_id, :case_id, :question, :theme,
                :alignment_score, :faithfulness_score, :verdict,
                :iteration, :quality_flagged,
                :alignment_signals, :faith_signals, :evaluated_at
            )
        """, result)
        results.append(result)

    dec_db_rw.commit()
    qua_conn.commit()

    # ── Determine next pipeline step ──────────────────────────
    passed  = sum(1 for r in results if r["verdict"] == "QUALITY_PASSED")
    failed  = sum(1 for r in results if r["verdict"] == "QUALITY_FAILED"
                  and not r["quality_flagged"])
    flagged = sum(1 for r in results if r["quality_flagged"])

    if needs_retry and not all_flagged:
        # At least one question must go back to Response
        next_step = "RESPONSE_DONE"   # re-process (Response will pick it up)
        log_event(qua_conn, case_id, "QUALITY_SENDING_BACK",
                  f"failed={failed} — routing back to Response")
    else:
        # All passed or all max-flagged → proceed to Recomposition
        next_step = "QUALITY_DONE"
        log_event(qua_conn, case_id, "QUALITY_DONE",
                  f"passed={passed} flagged={flagged}")

    # Update ingestion pipeline step
    try:
        ing = sqlite3.connect(str(INGESTION_DB_PATH),
                              check_same_thread=False)
        ing.execute("UPDATE cases SET pipeline_step=? WHERE case_id=?",
                    (next_step, case_id))
        ing.commit()
        ing.close()
    except Exception:
        pass

    return results


# ─────────────────────────────────────────────────────────────
# POLLING LOOP
# ─────────────────────────────────────────────────────────────

def poll_and_evaluate(qua_conn:   sqlite3.Connection,
                      stop_event: threading.Event) -> None:
    print("  [Quality] polling Response DB every 5 seconds...")

    while not stop_event.is_set():
        ing_db = open_db_ro(INGESTION_DB_PATH)
        if not ing_db:
            print("  [Quality] waiting for upstream DBs...")
            stop_event.wait(5)
            continue

        dec_db_rw = sqlite3.connect(str(DECOMP_DB_PATH),
                                    check_same_thread=False)
        dec_db_rw.row_factory = sqlite3.Row

        # Ensure score columns exist (added by Phase 09, not in original schema)
        for col in ["alignment_score REAL", "faithful_score REAL"]:
            try:
                dec_db_rw.execute(f"ALTER TABLE questions ADD COLUMN {col}")
                dec_db_rw.commit()
            except Exception:
                pass  # column already exists

        try:
            pending = ing_db.execute(
                "SELECT * FROM cases WHERE pipeline_step='RESPONSE_DONE'"
            ).fetchall()
        finally:
            ing_db.close()

        new_count = 0
        for row in pending:
            case = dict(row)
            if case_already_evaluated(qua_conn, case["case_id"]):
                continue

            qs = [dict(r) for r in dec_db_rw.execute(
                """SELECT * FROM questions
                   WHERE case_id=? AND scope='IN_SCOPE'
                     AND status='ANSWERED'""",
                (case["case_id"],),
            ).fetchall()]

            if not qs:
                continue

            results = run_quality(case, qs, dec_db_rw, qua_conn)
            new_count += 1
            _print_result(case, results)

        if new_count:
            print(f"\n  [Quality] ✓ {new_count} case(s) evaluated.\n")

        try:
            dec_db_rw.close()
        except Exception:
            pass

        stop_event.wait(5)


# ─────────────────────────────────────────────────────────────
# TERMINAL PRINTER
# ─────────────────────────────────────────────────────────────

def _bar(score: float, width: int = 16) -> str:
    filled = int(score * width)
    return "█" * filled + "░" * (width - filled)


def _print_result(case: dict, results: list[dict]) -> None:
    passed  = sum(1 for r in results if r["verdict"] == "QUALITY_PASSED")
    failed  = sum(1 for r in results if r["verdict"] == "QUALITY_FAILED")
    flagged = sum(1 for r in results if r["quality_flagged"])

    print(f"\n{'─'*60}")
    print(f"  📊  Quality evaluation")
    print(f"  Case ID : {case['case_id']}")
    print(f"  Source  : {case['source_type']}")
    print(f"  Results : {passed} passed  {failed} failed  {flagged} flagged")

    for r in results:
        verdict_icon = (
            "✅" if r["verdict"] == "QUALITY_PASSED"
            else "⚑ FLAGGED" if r["quality_flagged"]
            else "🔄 RETRY"
        )
        a  = r["alignment_score"]
        f  = r["faithfulness_score"]
        at = "✓" if a >= ALIGNMENT_THRESHOLD    else "✗"
        ft = "✓" if f >= FAITHFULNESS_THRESHOLD else "✗"

        print(f"\n  Q : {r['question'][:65]}")
        print(f"  Alignment   {_bar(a)} {a:.2f} (≥{ALIGNMENT_THRESHOLD}) {at}")
        print(f"  Faithfulness{_bar(f)} {f:.2f} (≥{FAITHFULNESS_THRESHOLD}) {ft}")
        print(f"  Verdict: {verdict_icon}")

    next_s = "Recomposition" if all(
        r["verdict"] == "QUALITY_PASSED" or r["quality_flagged"]
        for r in results
    ) else "Response (retry)"

    print(f"\n  → Next step: Phase 10 {next_s}")
    print(json.dumps({
        "case_id":  case["case_id"],
        "passed":   passed,
        "failed":   failed,
        "flagged":  flagged,
        "step":     "RECOMPOSITION" if next_s.startswith("Recomp") else "RESPONSE",
    }, indent=4))


# ─────────────────────────────────────────────────────────────
# WEB DASHBOARD  (port 8008)
# ─────────────────────────────────────────────────────────────

CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#f0f2f5;min-height:100vh;padding:32px 20px}
.page{max-width:980px;margin:0 auto}
.header{background:#1a2332;color:white;border-radius:8px;
        padding:20px 28px;margin-bottom:24px;
        display:flex;align-items:center;gap:16px}
.badge{background:#dc2626;color:white;font-size:10px;font-weight:700;
       padding:3px 8px;border-radius:3px;letter-spacing:.06em;text-transform:uppercase}
.header h1{font-size:18px;font-weight:600}
.header p{font-size:12px;color:#8b949e;margin-top:2px}
.hdr-right{margin-left:auto;text-align:right;font-size:11px;color:#8b949e}

.stats{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:24px}
.stat{background:white;border-radius:7px;padding:14px 18px;
      box-shadow:0 1px 3px rgba(0,0,0,.08)}
.stat-num{font-size:24px;font-weight:700;color:#1a2332}
.stat-label{font-size:10px;color:#9ca3af;margin-top:2px;
            text-transform:uppercase;letter-spacing:.05em}

.sec-title{font-size:11px;font-weight:700;color:#9ca3af;
           text-transform:uppercase;letter-spacing:.07em;
           margin-bottom:10px;padding:0 2px;margin-top:20px}

.card{background:white;border-radius:7px;
      box-shadow:0 1px 3px rgba(0,0,0,.08);overflow:hidden;margin-bottom:16px}

/* Per-question quality card */
.q-eval{padding:16px 20px;border-bottom:1px solid #f3f4f6}
.q-eval:last-child{border-bottom:none}

.q-header{display:flex;align-items:flex-start;gap:8px;margin-bottom:12px}
.q-text{font-size:12px;font-style:italic;color:#374151;flex:1}
.verdict-badge{font-size:10px;font-weight:700;padding:3px 8px;
               border-radius:10px;flex-shrink:0;white-space:nowrap}
.vb-pass   {background:#d1fae5;color:#065f46}
.vb-fail   {background:#fee2e2;color:#991b1b}
.vb-flagged{background:#fef3c7;color:#92400e}

.score-row{display:flex;align-items:center;gap:8px;margin:4px 0}
.score-name{font-size:11px;color:#6b7280;width:85px;flex-shrink:0}
.score-bar-track{flex:1;background:#f3f4f6;border-radius:3px;
                 height:10px;overflow:hidden;position:relative}
.score-bar-fill{height:100%;border-radius:3px;transition:width .3s}
.score-bar-threshold{position:absolute;top:0;bottom:0;width:2px;
                     background:#374151;opacity:.4}
.score-val{font-family:monospace;font-size:11px;color:#374151;
           width:38px;text-align:right;flex-shrink:0}
.score-icon{font-size:12px;flex-shrink:0;width:16px}

/* Signals box */
.signals-box{background:#f8f9fb;border:1px solid #e5e7eb;border-radius:4px;
             padding:8px 12px;margin-top:8px;font-size:11px;
             font-family:monospace;color:#6b7280;line-height:1.6}

/* Threshold config */
.threshold-info{display:flex;gap:16px;padding:14px 18px;
                font-size:12px;color:#374151;background:#f8f9fb;
                border-bottom:1px solid #e5e7eb}
.th-item{display:flex;align-items:center;gap:6px}
.th-val{font-family:monospace;font-weight:700;color:#1a2332}
.th-label{color:#6b7280}

/* Feedback loop explainer */
.loop-box{background:#fef3c7;border:1px solid #f59e0b;border-radius:6px;
          padding:12px 16px;font-size:12px;color:#78350f;margin-bottom:16px}
.loop-box strong{display:block;margin-bottom:4px}

.log-panel{background:#1a2332;border-radius:7px;
           padding:16px 20px;max-height:200px;overflow-y:auto}
.log-line{font-family:'Menlo',monospace;font-size:11px;
          color:#8b949e;padding:2px 0;line-height:1.6}
.ts{color:#484f58}
.ev-scored{color:#388bfd}
.ev-pass  {color:#2ea043}
.ev-fail  {color:#d29922}
.ev-flag  {color:#f85149}
.ev-done  {color:#a371f7}

.refresh-note{font-size:11px;color:#9ca3af;text-align:center;margin-top:8px}
.empty{text-align:center;color:#9ca3af;padding:24px;font-size:13px}
"""


def _score_bar_html(score: float, threshold: float,
                    color: str) -> str:
    pct   = int(score * 100)
    th_pct = int(threshold * 100)
    return (
        f'<div class="score-bar-track">'
        f'<div class="score-bar-fill" '
        f'style="width:{pct}%;background:{color}"></div>'
        f'<div class="score-bar-threshold" style="left:{th_pct}%"></div>'
        f'</div>'
    )


def render_dashboard(qua_conn: sqlite3.Connection) -> str:
    results = qua_conn.execute(
        "SELECT * FROM quality_results ORDER BY evaluated_at DESC"
    ).fetchall()
    results = [dict(r) for r in results]

    logs = qua_conn.execute(
        "SELECT * FROM quality_log ORDER BY ts DESC LIMIT 100"
    ).fetchall()
    logs = [dict(r) for r in logs]

    # Case metadata
    case_meta: dict[str, dict] = {}
    ing_db = open_db_ro(INGESTION_DB_PATH)
    if ing_db:
        try:
            for r in ing_db.execute(
                "SELECT case_id, source_type, subject, pipeline_step FROM cases"
            ):
                case_meta[r["case_id"]] = dict(r)
        finally:
            ing_db.close()

    # Stats
    total   = len(results)
    passed  = sum(1 for r in results if r["verdict"] == "QUALITY_PASSED")
    failed  = sum(1 for r in results if r["verdict"] == "QUALITY_FAILED")
    flagged = sum(1 for r in results if r["quality_flagged"])
    avg_align = (
        round(sum(r["alignment_score"] for r in results) / total, 2)
        if total else 0
    )

    stats_html = f"""<div class="stats">
      <div class="stat"><div class="stat-num">{total}</div>
        <div class="stat-label">Evaluated</div></div>
      <div class="stat"><div class="stat-num" style="color:#065f46">{passed}</div>
        <div class="stat-label">✅ Passed</div></div>
      <div class="stat"><div class="stat-num" style="color:#d97706">{failed}</div>
        <div class="stat-label">🔄 Retries</div></div>
      <div class="stat"><div class="stat-num" style="color:#dc2626">{flagged}</div>
        <div class="stat-label">⚑ Flagged</div></div>
      <div class="stat"><div class="stat-num" style="color:#0369a1">{avg_align}</div>
        <div class="stat-label">Avg Alignment</div></div>
    </div>"""

    # Threshold config bar
    th_bar = f"""
    <div class="threshold-info">
      <div class="th-item">
        <span class="th-label">Alignment threshold:</span>
        <span class="th-val">{ALIGNMENT_THRESHOLD}</span>
      </div>
      <div class="th-item">
        <span class="th-label">Faithfulness threshold:</span>
        <span class="th-val">{FAITHFULNESS_THRESHOLD}</span>
      </div>
      <div class="th-item">
        <span class="th-label">Max quality retries:</span>
        <span class="th-val">{MAX_QUALITY_ITERATIONS}</span>
      </div>
      <div style="margin-left:auto;font-size:10px;color:#9ca3af">
        Set by technical team · not configurable by office admin
      </div>
    </div>"""

    _SRC = {"DIRECT_EMAIL":"Email","STAFF_FORWARD":"Staff",
            "POSTAL_SCAN":"Scan","WEB_FORM":"Web Form"}

    # Per-question evaluation cards
    q_cards = ""
    if not results:
        q_cards = '<div class="empty">Waiting for Response to complete...</div>'
    for r in results:
        meta = case_meta.get(r["case_id"], {})
        src  = _SRC.get(meta.get("source_type",""), "")

        if r["quality_flagged"]:
            vb_cls, vb_txt = "vb-flagged", "⚑ FLAGGED"
        elif r["verdict"] == "QUALITY_PASSED":
            vb_cls, vb_txt = "vb-pass", "✅ PASSED"
        else:
            vb_cls, vb_txt = "vb-fail", "🔄 RETRY"

        a_color = "#22c55e" if r["alignment_score"]    >= ALIGNMENT_THRESHOLD    else "#ef4444"
        f_color = "#22c55e" if r["faithfulness_score"] >= FAITHFULNESS_THRESHOLD else "#ef4444"

        a_icon = "✓" if r["alignment_score"]    >= ALIGNMENT_THRESHOLD    else "✗"
        f_icon = "✓" if r["faithfulness_score"] >= FAITHFULNESS_THRESHOLD else "✗"

        # Parse signals
        try:
            a_sig = json.loads(r.get("alignment_signals") or "{}")
        except Exception:
            a_sig = {}
        try:
            f_sig = json.loads(r.get("faith_signals") or "{}")
        except Exception:
            f_sig = {}

        a_detail = (
            f"kw_hits={a_sig.get('kw_hits',0)}/{len(a_sig.get('q_tokens',[]))}  "
            f"theme_hits={a_sig.get('theme_hits',0)}  "
            f"overlap={a_sig.get('kw_overlap',0):.2f}"
        )
        f_detail = (
            f"has_context={f_sig.get('has_context',False)}  "
            f"shared_tokens={f_sig.get('shared_tokens',[])}  "
            f"overlap_rate={f_sig.get('overlap_rate',0):.2f}"
            if f_sig.get("has_context")
            else f"no KB context — faithfulness N/A"
        )

        q_cards += f"""
        <div class="q-eval">
          <div class="q-header">
            <div class="q-text">
              <span style="font-size:10px;color:#9ca3af">{src} · {r['case_id'][:8]}…</span><br>
              {_html.escape(r['question'][:90])}
            </div>
            <span class="verdict-badge {vb_cls}">{vb_txt}</span>
          </div>

          <div class="score-row">
            <div class="score-name">Alignment</div>
            {_score_bar_html(r['alignment_score'], ALIGNMENT_THRESHOLD, a_color)}
            <div class="score-val">{r['alignment_score']:.2f}</div>
            <div class="score-icon">{a_icon}</div>
          </div>
          <div class="score-row">
            <div class="score-name">Faithfulness</div>
            {_score_bar_html(r['faithfulness_score'], FAITHFULNESS_THRESHOLD, f_color)}
            <div class="score-val">{r['faithfulness_score']:.2f}</div>
            <div class="score-icon">{f_icon}</div>
          </div>

          <div class="signals-box">
            alignment:   {a_detail}<br>
            faithfulness: {f_detail}
          </div>
        </div>"""

    # Log
    ev_css = {
        "SCORED":              "ev-scored",
        "QUALITY_PASSED":      "ev-pass",   # unused but kept for completeness
        "QUALITY_RETRY":       "ev-fail",
        "QUALITY_FLAGGED":     "ev-flag",
        "QUALITY_SENDING_BACK":"ev-fail",
        "QUALITY_DONE":        "ev-done",
    }
    log_lines = ""
    for lg in logs:
        ev  = lg["event"]
        css = ev_css.get(ev, "")
        ts  = lg["ts"][11:19]
        det = _html.escape((lg.get("detail") or "")[:70])
        log_lines += (
            f'<div class="log-line"><span class="ts">{ts}</span>  '
            f'<span class="{css}">{ev}</span>  {det}</div>\n'
        )
    if not log_lines:
        log_lines = (
            '<div class="log-line" style="color:#484f58">— no events yet —</div>'
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="5">
<title>Phase 09 — Quality</title>
<style>{CSS}</style>
</head>
<body>
<div class="page">
  <div class="header">
    <div>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
        <span class="badge">Phase 09</span>
        <h1>Quality</h1>
      </div>
      <p>Alignment · Faithfulness · Feedback loop (max {MAX_QUALITY_ITERATIONS} retries)</p>
    </div>
    <div class="hdr-right">
      Polling Response every 5s<br>
      <span style="color:#2ea043;font-weight:600">● Live</span>
    </div>
  </div>

  {stats_html}

  <div class="sec-title">Evaluation results</div>
  <div class="card">
    {th_bar}
    {q_cards}
  </div>

  <div class="sec-title">Audit Log</div>
  <div class="log-panel">{log_lines}</div>
  <p class="refresh-note">Auto-refreshes every 5 seconds</p>
</div>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────
# HTTP SERVER
# ─────────────────────────────────────────────────────────────

class QualityDashboardHandler(http.server.BaseHTTPRequestHandler):
    qua_conn: sqlite3.Connection = None  # type: ignore

    def log_message(self, *args: object) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/favicon.ico":
            self.send_response(204); self.end_headers(); return
        page = render_dashboard(self.qua_conn)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode())


def make_handler(conn: sqlite3.Connection):
    class H(QualityDashboardHandler):
        qua_conn = conn
    return H


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "═"*60)
    print("  CITIZEN PLATFORM — Phase 09: Quality  (DEMO)")
    print("═"*60)

    qua_conn = init_quality_db()
    print(f"\n  ✓  Quality DB   : {QUALITY_DB_PATH}")
    print(f"  ✓  Thresholds   : alignment≥{ALIGNMENT_THRESHOLD}  "
          f"faithfulness≥{FAITHFULNESS_THRESHOLD}")
    print(f"  ✓  Max retries  : {MAX_QUALITY_ITERATIONS}")
    print(f"  ✓  Reading from : Response DB + Decomposition DB")

    stop_event = threading.Event()
    poller = threading.Thread(
        target=poll_and_evaluate,
        args=(qua_conn, stop_event),
        daemon=True,
    )
    poller.start()

    handler    = make_handler(qua_conn)
    server     = http.server.HTTPServer(("", PORT), handler)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    print(f"\n  ✓  Dashboard: http://localhost:{PORT}")
    print(f"\n  Run order:")
    for i, (name, port) in enumerate([
        ("reception.py",8000),("ingestion.py",8001),("security.py",8002),
        ("privacy.py",8003),("analysis.py",8004),("decomposition.py",8005),
        ("prompt_enrichment.py",8006),("response.py",8007),("quality.py",8008),
    ], 1):
        print(f"    Terminal {i} → python3 demo/{name:<25} (port {port})")
    print(f"\n  Press Ctrl-C to stop.\n")
    print("─"*60)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n\n  Stopping quality...")
        stop_event.set()
        server.shutdown()
        print("  Done.\n")


if __name__ == "__main__":
    main()
