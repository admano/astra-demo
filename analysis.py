"""
demo/analysis.py
----------------
Phase 05: Analysis  (DEMO VERSION)

Reads cases at pipeline_step=PRIVACY_DONE, analyses the anonymised
subject + body to produce three metadata values:

  Mood       — citizen's emotional state
               NEUTRAL | FRUSTRATED | ANGRY | DISTRESSED

  Complexity — how complex the request is to handle
               LOW | MEDIUM | HIGH

  Priority   — computed deterministically from Mood × Complexity
               LOW | NORMAL | HIGH | URGENT
               (no LLM call — pure lookup table, as per spec)

In production: two lightweight LLM calls (Ollama, on-premise) for
               Mood and Complexity. Priority is always deterministic.

In demo:       keyword-scoring heuristic for Mood and Complexity.
               Priority matrix is identical to production — same
               12-entry dict, same constants, no approximation.

Run:
    python3 demo/reception.py    ← port 8000
    python3 demo/ingestion.py    ← port 8001
    python3 demo/security.py     ← port 8002
    python3 demo/privacy.py      ← port 8003
    python3 demo/analysis.py     ← port 8004

Dashboard: http://localhost:8004
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
PRIVACY_DB_PATH   = DEMO_DIR / "demo_privacy.db"
ANALYSIS_DB_PATH  = DEMO_DIR / "demo_analysis.db"
PORT              = 8004


# ─────────────────────────────────────────────────────────────
# DOMAIN CONSTANTS  (identical to production — non-negotiable)
# ─────────────────────────────────────────────────────────────

class Mood:
    NEUTRAL    = "NEUTRAL"
    FRUSTRATED = "FRUSTRATED"
    ANGRY      = "ANGRY"
    DISTRESSED = "DISTRESSED"

class Complexity:
    LOW    = "LOW"
    MEDIUM = "MEDIUM"
    HIGH   = "HIGH"

class Priority:
    LOW    = "LOW"
    NORMAL = "NORMAL"
    HIGH   = "HIGH"
    URGENT = "URGENT"

# ── Priority matrix ────────────────────────────────────────
# Deterministic: Mood × Complexity → Priority.
# Identical to the production dict. No LLM. Never changes.
#
#              LOW      MEDIUM   HIGH
# NEUTRAL  →  LOW      NORMAL   HIGH
# FRUSTRATED→ NORMAL   NORMAL   HIGH
# ANGRY    →  HIGH     HIGH     URGENT
# DISTRESSED→ URGENT   URGENT   URGENT

PRIORITY_MATRIX: dict[tuple[str, str], str] = {
    (Mood.NEUTRAL,    Complexity.LOW):    Priority.LOW,
    (Mood.NEUTRAL,    Complexity.MEDIUM): Priority.NORMAL,
    (Mood.NEUTRAL,    Complexity.HIGH):   Priority.HIGH,
    (Mood.FRUSTRATED, Complexity.LOW):    Priority.NORMAL,
    (Mood.FRUSTRATED, Complexity.MEDIUM): Priority.NORMAL,
    (Mood.FRUSTRATED, Complexity.HIGH):   Priority.HIGH,
    (Mood.ANGRY,      Complexity.LOW):    Priority.HIGH,
    (Mood.ANGRY,      Complexity.MEDIUM): Priority.HIGH,
    (Mood.ANGRY,      Complexity.HIGH):   Priority.URGENT,
    (Mood.DISTRESSED, Complexity.LOW):    Priority.URGENT,
    (Mood.DISTRESSED, Complexity.MEDIUM): Priority.URGENT,
    (Mood.DISTRESSED, Complexity.HIGH):   Priority.URGENT,
}

def compute_priority(mood: str, complexity: str) -> str:
    """Pure deterministic lookup. No LLM. Never approximated."""
    return PRIORITY_MATRIX[(mood, complexity)]


# ─────────────────────────────────────────────────────────────
# MOOD DETECTOR  (simulates LlamaGuard / LLM sentiment call)
#
# In production: a single short LLM prompt sent to on-premise
# Ollama, returning one of the four mood labels.
#
# In demo: keyword-scoring heuristic.
# Each word list was chosen to reflect how citizens actually write
# to government offices in DE/FR/IT across the four mood levels.
# ─────────────────────────────────────────────────────────────

# DE / FR / IT / EN keywords per mood level
_MOOD_KEYWORDS: dict[str, list[str]] = {
    Mood.DISTRESSED: [
        # extreme distress, urgency, desperation
        "dringend", "notfall", "verzweifelt", "existenz", "verliere",
        "verlieren", "obdachlos", "suizid", "kann nicht mehr",
        "urgent", "désespéré", "urgence", "je perds", "survie",
        "urgente", "disperato", "emergenza", "sto perdendo",
        "desperate", "emergency", "losing everything",
    ],
    Mood.ANGRY: [
        # direct anger, threats, accusations, profanity-adjacent
        "skandal", "inakzeptabel", "schande", "empört", "wütend",
        "klage", "anwalt", "rechtlich", "beschwerde", "forderung",
        "unverschämt", "unfähig", "versagen", "sofort",
        "scandale", "inadmissible", "honte", "inacceptable",
        "plainte", "avocat", "exige", "immédiatement", "furieux",
        "scandalo", "inaccettabile", "vergogna", "immediatamente",
        "outrageous", "unacceptable", "demand", "lawyer", "sue",
    ],
    Mood.FRUSTRATED: [
        # repeated attempts, no response, waiting, mild complaint
        "wieder", "erneut", "nochmals", "immer noch", "bereits",
        "mehrmals", "keine antwort", "keine reaktion", "warte",
        "wartet", "seit wochen", "seit monaten", "vergessen",
        "encore", "de nouveau", "toujours pas", "aucune réponse",
        "aucune confirmation", "aucune réaction",
        "j'attends", "depuis des semaines", "plusieurs fois",
        "pas de réponse", "sans réponse", "pas encore",
        "ancora", "di nuovo", "nessuna risposta", "aspetto",
        "again", "still no", "no response", "waiting", "weeks",
        "ignored", "multiple times", "no reply", "no answer",
    ],
    Mood.NEUTRAL: [
        # polite, informational, standard government correspondence style
        "bitte", "könnten", "möchte", "anfrage", "frage",
        "information", "auskunft", "formular", "antrag",
        "s'il vous plaît", "pourriez", "souhaite", "demande",
        "renseignement", "formulaire",
        "per favore", "vorrei", "richiesta", "informazione",
        "please", "would like", "request", "inquiry",
    ],
}


def detect_mood(text: str) -> tuple[str, dict[str, int]]:
    """
    Return (mood, scores_dict).
    scores_dict is for display in the dashboard.

    In production: replaced by a single LLM call:
      prompt = f"Classify the mood of this text as one of
                 [NEUTRAL, FRUSTRATED, ANGRY, DISTRESSED].
                 Respond with exactly one word.\n\n{text}"
    """
    lower = text.lower()
    scores: dict[str, int] = {m: 0 for m in [
        Mood.DISTRESSED, Mood.ANGRY, Mood.FRUSTRATED, Mood.NEUTRAL
    ]}

    for mood, keywords in _MOOD_KEYWORDS.items():
        for kw in keywords:
            if kw in lower:
                scores[mood] += 1

    # Priority: DISTRESSED > ANGRY > FRUSTRATED > NEUTRAL
    for mood in [Mood.DISTRESSED, Mood.ANGRY, Mood.FRUSTRATED]:
        if scores[mood] > 0:
            return mood, scores

    return Mood.NEUTRAL, scores


# ─────────────────────────────────────────────────────────────
# COMPLEXITY DETECTOR  (simulates LLM complexity call)
#
# In production: a second short LLM prompt returning LOW/MEDIUM/HIGH.
#
# In demo: three signals combined —
#   1. Text length (longer → more complex)
#   2. Question count (more ?  → more questions to handle)
#   3. Topic complexity keywords (legal, financial, technical)
# ─────────────────────────────────────────────────────────────

_COMPLEX_KEYWORDS: list[str] = [
    # legal / regulatory
    "gesetz", "verordnung", "artikel", "rechtlich", "klage", "einspruch",
    "loi", "règlement", "article", "juridique", "recours",
    "legge", "regolamento", "articolo", "giuridico", "ricorso",
    "regulation", "legislation", "appeal", "legal", "statute",
    # financial / administrative
    "steuer", "rechnung", "zahlung", "entschädigung", "rückerstattung",
    "impôt", "facture", "paiement", "indemnisation", "remboursement",
    "tassa", "fattura", "pagamento", "indennizzo", "rimborso",
    "tax", "invoice", "payment", "compensation", "reimbursement",
    # technical / infrastructure
    "technisch", "infrastruktur", "system", "datenbank", "schnittstelle",
    "technique", "infrastructure", "système", "base de données",
    "tecnico", "infrastruttura", "sistema",
    "technical", "infrastructure", "database", "interface",
    # administrative references / prior correspondence
    "schreiben vom", "mein schreiben", "lettre du", "ma lettre",
    "mia lettera", "beziehe mich", "me réfère", "riferisco",
    "lärmschutz", "massnahmen", "betreffend", "concernant",
    "bezüglich", "betrifft",
]


def detect_complexity(text: str) -> tuple[str, dict[str, Any]]:
    """
    Return (complexity, signals_dict).
    signals_dict is for display in the dashboard.

    In production: replaced by a single LLM call:
      prompt = f"Rate the complexity of this citizen request as
                 LOW, MEDIUM, or HIGH based on number of distinct
                 questions, legal/technical content, and scope.
                 Respond with exactly one word.\n\n{text}"
    """
    lower  = text.lower()
    length = len(text)

    # Signal 1: text length
    length_score = 0
    if length > 800:
        length_score = 2
    elif length > 300:
        length_score = 1

    # Signal 2: number of question marks
    q_count = text.count("?")
    q_score = min(q_count, 2)   # cap at 2

    # Signal 3: complex-topic keywords
    kw_hits = sum(1 for kw in _COMPLEX_KEYWORDS if kw in lower)
    kw_score = min(kw_hits, 2)

    total = length_score + q_score + kw_score

    signals = {
        "length": length,
        "length_score": length_score,
        "question_marks": q_count,
        "q_score": q_score,
        "complex_keywords": kw_hits,
        "kw_score": kw_score,
        "total_score": total,
    }

    if total >= 4:
        return Complexity.HIGH, signals
    elif total >= 2:
        return Complexity.MEDIUM, signals
    else:
        return Complexity.LOW, signals


# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────

def init_analysis_db() -> sqlite3.Connection:
    """
    Tables:
      analysis_results — mood, complexity, priority per case
      analysis_log     — append-only audit events
    """
    conn = sqlite3.connect(str(ANALYSIS_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS analysis_results (
            case_id         TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            mood            TEXT NOT NULL,
            complexity      TEXT NOT NULL,
            priority        TEXT NOT NULL,
            mood_scores     TEXT,   -- JSON: {NEUTRAL:n, FRUSTRATED:n, ...}
            complexity_signals TEXT, -- JSON: {length:n, q_score:n, ...}
            analysed_at     TEXT NOT NULL,
            pipeline_step   TEXT NOT NULL DEFAULT 'ANALYSIS_DONE'
        );

        CREATE TABLE IF NOT EXISTS analysis_log (
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


def already_analysed(ana_conn: sqlite3.Connection, case_id: str) -> bool:
    return ana_conn.execute(
        "SELECT 1 FROM analysis_results WHERE case_id=?", (case_id,)
    ).fetchone() is not None


def log_event(ana_conn: sqlite3.Connection,
              case_id: str, event: str, detail: str = "") -> None:
    ana_conn.execute(
        "INSERT INTO analysis_log VALUES (?,?,?,?,?)",
        (str(uuid.uuid4()), case_id, event, detail,
         datetime.now(timezone.utc).isoformat()),
    )
    ana_conn.commit()


# ─────────────────────────────────────────────────────────────
# CORE ANALYSIS LOGIC
# ─────────────────────────────────────────────────────────────

def run_analysis(case: dict[str, Any],
                 priv: dict[str, Any],
                 ana_conn: sqlite3.Connection) -> dict[str, Any]:
    """
    Run mood + complexity detection on the anonymised text,
    then compute priority deterministically.

    case  — row from ingestion cases table
    priv  — row from privacy_results (has subject_anon, body_anon)
    """
    case_id   = case["case_id"]
    tenant_id = case["tenant_id"]

    # Use anonymised text only — LLM never sees PII
    text = (
        (priv.get("subject_anon") or "") + "\n\n" +
        (priv.get("body_anon")    or "")
    ).strip()

    # ── ① Mood detection ──────────────────────────────────────
    mood, mood_scores = detect_mood(text)
    log_event(ana_conn, case_id, "MOOD_DETECTED",
              f"mood={mood} scores={mood_scores}")

    # ── ② Complexity detection ────────────────────────────────
    complexity, complexity_signals = detect_complexity(text)
    log_event(ana_conn, case_id, "COMPLEXITY_DETECTED",
              f"complexity={complexity} total={complexity_signals['total_score']}")

    # ── ③ Priority — deterministic, no LLM ───────────────────
    priority = compute_priority(mood, complexity)
    log_event(ana_conn, case_id, "PRIORITY_COMPUTED",
              f"priority={priority} ({mood}×{complexity})")

    # ── Save result ───────────────────────────────────────────
    result = {
        "case_id":             case_id,
        "tenant_id":           tenant_id,
        "mood":                mood,
        "complexity":          complexity,
        "priority":            priority,
        "mood_scores":         json.dumps(mood_scores),
        "complexity_signals":  json.dumps(complexity_signals),
        "analysed_at":         datetime.now(timezone.utc).isoformat(),
        "pipeline_step":       "ANALYSIS_DONE",
    }
    ana_conn.execute("""
        INSERT INTO analysis_results VALUES (
            :case_id, :tenant_id, :mood, :complexity, :priority,
            :mood_scores, :complexity_signals,
            :analysed_at, :pipeline_step
        )
    """, result)
    ana_conn.commit()

    # ── Advance pipeline step in Ingestion DB ─────────────────
    try:
        ing = sqlite3.connect(str(INGESTION_DB_PATH),
                              check_same_thread=False)
        # Add columns if they don't exist yet (demo schema evolution)
        for col, typ in [("priority","TEXT"),("complexity","TEXT"),("mood","TEXT")]:
            try:
                ing.execute(f"ALTER TABLE cases ADD COLUMN {col} {typ}")
                ing.commit()
            except Exception:
                pass  # column already exists
        ing.execute(
            "UPDATE cases SET pipeline_step='ANALYSIS_DONE', "
            "priority=?, complexity=?, mood=? WHERE case_id=?",
            (priority, complexity, mood, case_id),
        )
        ing.commit()
        ing.close()
    except Exception:
        pass

    log_event(ana_conn, case_id, "ANALYSIS_DONE",
              f"mood={mood} complexity={complexity} priority={priority}")

    return result


# ─────────────────────────────────────────────────────────────
# POLLING LOOP
# ─────────────────────────────────────────────────────────────

def poll_and_analyse(ana_conn: sqlite3.Connection,
                     stop_event: threading.Event) -> None:
    print("  [Analysis] polling Privacy DB every 5 seconds...")

    while not stop_event.is_set():
        ing_db  = open_db_ro(INGESTION_DB_PATH)
        priv_db = open_db_ro(PRIVACY_DB_PATH)

        if not all([ing_db, priv_db]):
            print("  [Analysis] waiting for upstream DBs...")
            stop_event.wait(5)
            for db in [ing_db, priv_db]:
                try:
                    if db: db.close()
                except Exception:
                    pass
            continue

        try:
            pending = ing_db.execute(
                "SELECT * FROM cases WHERE pipeline_step='PRIVACY_DONE'"
            ).fetchall()
        finally:
            ing_db.close()

        new_count = 0
        for row in pending:
            case = dict(row)
            if already_analysed(ana_conn, case["case_id"]):
                continue

            # Get anonymised text from Privacy
            try:
                priv_row = priv_db.execute(
                    "SELECT * FROM privacy_results WHERE case_id=?",
                    (case["case_id"],),
                ).fetchone()
                priv = dict(priv_row) if priv_row else {}
            except Exception:
                priv = {}

            result = run_analysis(case, priv, ana_conn)
            new_count += 1
            _print_result(case, result)

        if new_count:
            print(f"\n  [Analysis] ✓ {new_count} case(s) analysed.\n")

        try:
            priv_db.close()
        except Exception:
            pass

        stop_event.wait(5)


# ─────────────────────────────────────────────────────────────
# TERMINAL PRINTER
# ─────────────────────────────────────────────────────────────

# Hatchet priority → integer (for display)
_PRIORITY_QUEUE = {
    Priority.LOW: 1, Priority.NORMAL: 2,
    Priority.HIGH: 3, Priority.URGENT: 4,
}

_MOOD_ICON = {
    Mood.NEUTRAL:    "😐",
    Mood.FRUSTRATED: "😤",
    Mood.ANGRY:      "😠",
    Mood.DISTRESSED: "😰",
}

_PRIORITY_ICON = {
    Priority.LOW:    "🔵",
    Priority.NORMAL: "🟢",
    Priority.HIGH:   "🟡",
    Priority.URGENT: "🔴",
}


def _print_result(case: dict, result: dict) -> None:
    mood_icon     = _MOOD_ICON.get(result["mood"], "")
    priority_icon = _PRIORITY_ICON.get(result["priority"], "")
    q_level       = _PRIORITY_QUEUE[result["priority"]]

    print(f"\n{'─'*60}")
    print(f"  📊  Analysis complete")
    print(f"  Case ID    : {result['case_id']}")
    print(f"  Source     : {case['source_type']}")
    print(f"\n  {mood_icon}  Mood       : {result['mood']}")
    print(f"  📈  Complexity : {result['complexity']}")
    print(f"  {priority_icon}  Priority   : {result['priority']}"
          f"  (queue level {q_level}/4)")
    print(f"\n  → Next step: Phase 06 Decomposition")
    print(json.dumps({
        "case_id":    result["case_id"],
        "tenant_id":  result["tenant_id"],
        "mood":       result["mood"],
        "complexity": result["complexity"],
        "priority":   result["priority"],
        "step":       "DECOMPOSITION",
    }, indent=4))


# ─────────────────────────────────────────────────────────────
# WEB DASHBOARD  (port 8004)
# ─────────────────────────────────────────────────────────────

CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#f0f2f5;min-height:100vh;padding:32px 20px}
.page{max-width:960px;margin:0 auto}

.header{background:#1a2332;color:white;border-radius:8px;
        padding:20px 28px;margin-bottom:24px;
        display:flex;align-items:center;gap:16px}
.badge{font-size:10px;font-weight:700;padding:3px 8px;border-radius:3px;
       letter-spacing:.06em;text-transform:uppercase}
.badge-05{background:#0ea5e9;color:white}
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
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;padding:10px 14px;background:#f8f9fb;color:#6b7280;
   font-size:11px;font-weight:600;text-transform:uppercase;
   letter-spacing:.05em;border-bottom:1px solid #e5e7eb}
td{padding:10px 14px;border-bottom:1px solid #f3f4f6;color:#374151;vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:#fafafa}

/* Mood pills */
.mood{display:inline-flex;align-items:center;gap:5px;
      font-size:11px;font-weight:700;padding:3px 10px;border-radius:12px}
.mood-neutral    {background:#f3f4f6;color:#374151}
.mood-frustrated {background:#fef3c7;color:#92400e}
.mood-angry      {background:#fee2e2;color:#991b1b}
.mood-distressed {background:#ede9fe;color:#4c1d95}

/* Complexity pills */
.cplx{display:inline-block;font-size:11px;font-weight:700;
      padding:2px 8px;border-radius:4px;text-transform:uppercase}
.cplx-low   {background:#f0fdf4;color:#166534}
.cplx-medium{background:#fefce8;color:#713f12}
.cplx-high  {background:#fff7ed;color:#9a3412}

/* Priority queue bar */
.queue-bar{display:flex;gap:3px;align-items:center}
.q-pip{width:14px;height:14px;border-radius:3px;flex-shrink:0}
.q-active-low    {background:#3b82f6}
.q-active-normal {background:#22c55e}
.q-active-high   {background:#f59e0b}
.q-active-urgent {background:#ef4444}
.q-inactive{background:#e5e7eb}
.q-label{font-size:11px;font-weight:700;margin-left:4px}

/* Priority matrix heatmap */
.matrix{display:grid;grid-template-columns:90px repeat(3,1fr);
        gap:2px;font-family:'Menlo',monospace;font-size:10px}
.mx-header{background:#f8f9fb;padding:6px 8px;color:#6b7280;
           font-weight:600;text-align:center;border-radius:3px}
.mx-row-label{background:#f8f9fb;padding:6px 8px;color:#374151;
              font-weight:600;border-radius:3px;
              display:flex;align-items:center;gap:4px}
.mx-cell{padding:5px 8px;text-align:center;border-radius:3px;
         font-weight:700;font-size:10px}
.mx-low    {background:#dbeafe;color:#1e40af}
.mx-normal {background:#dcfce7;color:#166534}
.mx-high   {background:#fef9c3;color:#713f12}
.mx-urgent {background:#fee2e2;color:#991b1b}
.mx-active {box-shadow:0 0 0 2px #1a2332;z-index:1;position:relative}

/* Score bars */
.score-bar{display:flex;align-items:center;gap:8px;margin:3px 0}
.score-label{font-size:10px;color:#6b7280;width:80px;flex-shrink:0}
.bar-track{flex:1;background:#f3f4f6;border-radius:3px;height:8px;overflow:hidden}
.bar-fill{height:100%;border-radius:3px;transition:width .3s}
.bar-distressed {background:#7c3aed}
.bar-angry      {background:#dc2626}
.bar-frustrated {background:#d97706}
.bar-neutral    {background:#6b7280}
.score-val{font-size:10px;color:#6b7280;width:16px;text-align:right;flex-shrink:0}

/* Signal grid */
.signals{display:grid;grid-template-columns:1fr 1fr;gap:8px;padding:14px 16px}
.sig-item{background:#f8f9fb;border-radius:5px;padding:8px 12px}
.sig-label{font-size:10px;color:#9ca3af;text-transform:uppercase;
           letter-spacing:.05em;margin-bottom:2px}
.sig-val{font-size:16px;font-weight:700;color:#1a2332}

.log-panel{background:#1a2332;border-radius:7px;
           padding:16px 20px;max-height:220px;overflow-y:auto}
.log-line{font-family:'Menlo',monospace;font-size:11px;
          color:#8b949e;padding:2px 0;line-height:1.6}
.ts{color:#484f58}
.ev-mood{color:#a371f7}
.ev-cplx{color:#388bfd}
.ev-prio{color:#2ea043}
.ev-done{color:#d29922}

.refresh-note{font-size:11px;color:#9ca3af;text-align:center;margin-top:8px}
.empty{text-align:center;color:#9ca3af;padding:24px;font-size:13px}
"""


def _mood_pill(mood: str) -> str:
    icons = {Mood.NEUTRAL: "😐", Mood.FRUSTRATED: "😤",
             Mood.ANGRY: "😠", Mood.DISTRESSED: "😰"}
    css   = {Mood.NEUTRAL: "neutral", Mood.FRUSTRATED: "frustrated",
             Mood.ANGRY:  "angry",   Mood.DISTRESSED: "distressed"}
    icon  = icons.get(mood, "")
    cls   = css.get(mood, "neutral")
    return f'<span class="mood mood-{cls}">{icon} {mood}</span>'


def _cplx_pill(cplx: str) -> str:
    cls = {"LOW": "low", "MEDIUM": "medium", "HIGH": "high"}.get(cplx, "low")
    return f'<span class="cplx cplx-{cls}">{cplx}</span>'


def _queue_bar(priority: str) -> str:
    levels     = [Priority.LOW, Priority.NORMAL, Priority.HIGH, Priority.URGENT]
    active_css = {
        Priority.LOW:    "q-active-low",
        Priority.NORMAL: "q-active-normal",
        Priority.HIGH:   "q-active-high",
        Priority.URGENT: "q-active-urgent",
    }
    pips = ""
    p_idx = levels.index(priority)
    for i, lvl in enumerate(levels):
        css = active_css[lvl] if i <= p_idx else "q-inactive"
        pips += f'<div class="q-pip {css}" title="{lvl}"></div>'
    return (f'<div class="queue-bar">{pips}'
            f'<span class="q-label">{priority}</span></div>')


def _matrix_html(active_mood: str | None = None,
                 active_cplx: str | None = None) -> str:
    moods = [Mood.NEUTRAL, Mood.FRUSTRATED, Mood.ANGRY, Mood.DISTRESSED]
    cplxs = [Complexity.LOW, Complexity.MEDIUM, Complexity.HIGH]
    css_map = {
        Priority.LOW:    "mx-low",
        Priority.NORMAL: "mx-normal",
        Priority.HIGH:   "mx-high",
        Priority.URGENT: "mx-urgent",
    }
    icons = {Mood.NEUTRAL: "😐", Mood.FRUSTRATED: "😤",
             Mood.ANGRY: "😠", Mood.DISTRESSED: "😰"}
    html  = '<div class="matrix">'
    html += '<div class="mx-header"></div>'
    for c in cplxs:
        html += f'<div class="mx-header">{c}</div>'
    for m in moods:
        html += f'<div class="mx-row-label">{icons.get(m,"")} {m}</div>'
        for c in cplxs:
            p     = PRIORITY_MATRIX[(m, c)]
            pcss  = css_map[p]
            active = (m == active_mood and c == active_cplx)
            acss  = " mx-active" if active else ""
            html += f'<div class="mx-cell {pcss}{acss}">{p[:3]}</div>'
    html += '</div>'
    return html


def render_dashboard(ana_conn: sqlite3.Connection) -> str:
    results = ana_conn.execute(
        "SELECT * FROM analysis_results ORDER BY analysed_at DESC"
    ).fetchall()
    results = [dict(r) for r in results]

    logs = ana_conn.execute(
        "SELECT * FROM analysis_log ORDER BY ts DESC LIMIT 80"
    ).fetchall()
    logs = [dict(r) for r in logs]

    # Case metadata from Ingestion
    case_meta: dict[str, dict] = {}
    ing_db = open_db_ro(INGESTION_DB_PATH)
    if ing_db:
        try:
            for r in ing_db.execute(
                "SELECT case_id, source_type, subject, language FROM cases"
            ):
                case_meta[r["case_id"]] = dict(r)
        finally:
            ing_db.close()

    # Stats
    total    = len(results)
    moods    = {m: sum(1 for r in results if r["mood"] == m)
                for m in [Mood.NEUTRAL, Mood.FRUSTRATED,
                           Mood.ANGRY, Mood.DISTRESSED]}
    urgents  = sum(1 for r in results if r["priority"] == Priority.URGENT)

    stats_html = f"""<div class="stats">
      <div class="stat"><div class="stat-num">{total}</div>
        <div class="stat-label">Analysed</div></div>
      <div class="stat"><div class="stat-num">{moods[Mood.NEUTRAL]}</div>
        <div class="stat-label">😐 Neutral</div></div>
      <div class="stat"><div class="stat-num" style="color:#d97706">
        {moods[Mood.FRUSTRATED]}</div>
        <div class="stat-label">😤 Frustrated</div></div>
      <div class="stat"><div class="stat-num" style="color:#dc2626">
        {moods[Mood.ANGRY]}</div>
        <div class="stat-label">😠 Angry</div></div>
      <div class="stat"><div class="stat-num" style="color:#ef4444">
        {urgents}</div>
        <div class="stat-label">🔴 URGENT</div></div>
    </div>"""

    # Results table
    src_labels = {"DIRECT_EMAIL": "Email", "STAFF_FORWARD": "Staff",
                  "POSTAL_SCAN": "Scan",  "WEB_FORM": "Web Form"}
    tbl_rows = ""
    if not results:
        tbl_rows = '<tr><td colspan="6" class="empty">Waiting for Privacy to complete...</td></tr>'
    for r in results:
        meta = case_meta.get(r["case_id"], {})
        src  = src_labels.get(meta.get("source_type", ""), "")
        subj = _html.escape((meta.get("subject") or "")[:40])
        tbl_rows += f"""<tr>
          <td><code style="font-size:11px;color:#6b7280">{r['case_id'][:8]}…</code></td>
          <td>{src}</td>
          <td title="{_html.escape(meta.get('subject',''))}">{subj}</td>
          <td>{_mood_pill(r['mood'])}</td>
          <td>{_cplx_pill(r['complexity'])}</td>
          <td>{_queue_bar(r['priority'])}</td>
        </tr>"""

    # Detail panel for the most recent case
    detail_html = ""
    if results:
        latest  = results[0]
        cid     = latest["case_id"]
        meta    = case_meta.get(cid, {})

        # Mood score bars
        try:
            mood_scores = json.loads(latest.get("mood_scores") or "{}")
        except Exception:
            mood_scores = {}
        max_score = max(mood_scores.values(), default=1) or 1
        bar_css = {Mood.DISTRESSED: "bar-distressed", Mood.ANGRY: "bar-angry",
                   Mood.FRUSTRATED: "bar-frustrated", Mood.NEUTRAL: "bar-neutral"}
        score_bars = ""
        for mood in [Mood.DISTRESSED, Mood.ANGRY, Mood.FRUSTRATED, Mood.NEUTRAL]:
            score = mood_scores.get(mood, 0)
            pct   = int(score / max_score * 100) if max_score else 0
            css   = bar_css[mood]
            score_bars += f"""<div class="score-bar">
              <span class="score-label">{mood}</span>
              <div class="bar-track">
                <div class="bar-fill {css}" style="width:{pct}%"></div>
              </div>
              <span class="score-val">{score}</span>
            </div>"""

        # Complexity signals
        try:
            sigs = json.loads(latest.get("complexity_signals") or "{}")
        except Exception:
            sigs = {}

        detail_html = f"""
        <div class="sec-title">Detail — case {cid[:8]}…
          ({src_labels.get(meta.get('source_type',''), '')})
        </div>
        <div class="card" style="overflow:visible">
          <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:0;
                      border-bottom:1px solid #f3f4f6">

            <div style="padding:16px 20px;border-right:1px solid #f3f4f6">
              <div class="sec-title" style="margin-top:0">Mood scores</div>
              {score_bars}
            </div>

            <div style="padding:16px 20px;border-right:1px solid #f3f4f6">
              <div class="sec-title" style="margin-top:0">Complexity signals</div>
              <div class="signals" style="padding:0;gap:6px">
                <div class="sig-item">
                  <div class="sig-label">Text length</div>
                  <div class="sig-val">{sigs.get('length', 0)}</div>
                </div>
                <div class="sig-item">
                  <div class="sig-label">Questions (?)</div>
                  <div class="sig-val">{sigs.get('question_marks', 0)}</div>
                </div>
                <div class="sig-item">
                  <div class="sig-label">Complex keywords</div>
                  <div class="sig-val">{sigs.get('complex_keywords', 0)}</div>
                </div>
                <div class="sig-item">
                  <div class="sig-label">Total score</div>
                  <div class="sig-val" style="color:#0ea5e9">
                    {sigs.get('total_score', 0)}/6
                  </div>
                </div>
              </div>
            </div>

            <div style="padding:16px 20px">
              <div class="sec-title" style="margin-top:0">Priority matrix</div>
              {_matrix_html(latest['mood'], latest['complexity'])}
              <div style="margin-top:10px;font-size:11px;color:#6b7280">
                Highlighted cell = this case.<br>
                Computed deterministically — no LLM.
              </div>
            </div>

          </div>
        </div>"""

    # Log
    ev_css = {
        "MOOD_DETECTED":      "ev-mood",
        "COMPLEXITY_DETECTED": "ev-cplx",
        "PRIORITY_COMPUTED":   "ev-prio",
        "ANALYSIS_DONE":       "ev-done",
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
        log_lines = '<div class="log-line" style="color:#484f58">— no events yet —</div>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="5">
<title>Phase 05 — Analysis</title>
<style>{CSS}</style>
</head>
<body>
<div class="page">
  <div class="header">
    <div>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
        <span class="badge badge-05">Phase 05</span>
        <h1>Analysis</h1>
      </div>
      <p>Mood · Complexity · Priority  (anonymised text only)</p>
    </div>
    <div class="hdr-right">
      Polling Privacy every 5s<br>
      <span style="color:#2ea043;font-weight:600">● Live</span>
    </div>
  </div>

  {stats_html}

  <div class="sec-title">Results per case</div>
  <div class="card">
    <table>
      <thead>
        <tr>
          <th>Case ID</th><th>Source</th><th>Subject</th>
          <th>Mood</th><th>Complexity</th><th>Priority Queue</th>
        </tr>
      </thead>
      <tbody>{tbl_rows}</tbody>
    </table>
  </div>

  {detail_html}

  <div class="sec-title">Audit Log</div>
  <div class="log-panel">{log_lines}</div>
  <p class="refresh-note">Auto-refreshes every 5 seconds</p>
</div>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────
# HTTP SERVER
# ─────────────────────────────────────────────────────────────

class AnalysisDashboardHandler(http.server.BaseHTTPRequestHandler):
    ana_conn: sqlite3.Connection = None  # type: ignore

    def log_message(self, *args: object) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/favicon.ico":
            self.send_response(204); self.end_headers(); return
        page = render_dashboard(self.ana_conn)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode())


def make_handler(conn: sqlite3.Connection):
    class H(AnalysisDashboardHandler):
        ana_conn = conn
    return H


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "═"*60)
    print("  CITIZEN PLATFORM — Phase 05: Analysis  (DEMO)")
    print("═"*60)

    ana_conn = init_analysis_db()
    print(f"\n  ✓  Analysis DB : {ANALYSIS_DB_PATH}")
    print(f"  ✓  Reading from: Privacy DB + Ingestion DB")

    stop_event = threading.Event()
    poller = threading.Thread(
        target=poll_and_analyse,
        args=(ana_conn, stop_event),
        daemon=True,
    )
    poller.start()

    handler    = make_handler(ana_conn)
    server     = http.server.HTTPServer(("", PORT), handler)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    print(f"\n  ✓  Dashboard: http://localhost:{PORT}")
    print(f"\n  Run order:")
    print(f"    Terminal 1 → python3 demo/reception.py    (port 8000)")
    print(f"    Terminal 2 → python3 demo/ingestion.py    (port 8001)")
    print(f"    Terminal 3 → python3 demo/security.py     (port 8002)")
    print(f"    Terminal 4 → python3 demo/privacy.py      (port 8003)")
    print(f"    Terminal 5 → python3 demo/analysis.py     (port 8004)")
    print(f"\n  Press Ctrl-C to stop.\n")
    print("─"*60)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n\n  Stopping analysis...")
        stop_event.set()
        server.shutdown()
        print("  Done.\n")


if __name__ == "__main__":
    main()
