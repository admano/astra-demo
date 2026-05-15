"""
demo/decomposition.py
---------------------
Phase 06: Decomposition  (DEMO VERSION)

Reads cases at pipeline_step=ANALYSIS_DONE, breaks each message
into atomic questions, checks each question against the office
mandate, assigns a theme, and stores them in the questions table.

Three sub-steps per case:
  ① Question extraction  — split message into atomic questions
                            (≤ max_questions, default 5)
  ② Scope check          — IN_SCOPE if within ASTRA mandate,
                            OUT_OF_SCOPE otherwise (with redirect)
  ③ Theme assignment     — one of the ASTRA topic categories

In production: two LLM calls per question (scope + theme).
In demo:       rule-based extraction + keyword scope/theme lookup.
               The questions table schema is identical to production.

Run:
    python3 demo/reception.py        ← port 8000
    python3 demo/ingestion.py        ← port 8001
    python3 demo/security.py         ← port 8002
    python3 demo/privacy.py          ← port 8003
    python3 demo/analysis.py         ← port 8004
    python3 demo/decomposition.py    ← port 8005

Dashboard: http://localhost:8005
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
DECOMP_DB_PATH    = DEMO_DIR / "demo_decomposition.db"
PORT              = 8005

# Tenant configuration (in production: read from tenant_config table)
MAX_QUESTIONS = 5


# ─────────────────────────────────────────────────────────────
# ASTRA MANDATE + THEMES
#
# ASTRA (Bundesamt für Strassen) mandate covers:
#   national roads, tunnels, road safety, noise barriers,
#   driver licensing, vehicle registration, road infrastructure.
#
# In production: LLM uses the full mandate description from
#   tenant_config.mandate_description to decide IN/OUT of scope.
# In demo:       keyword lookup against these theme definitions.
# ─────────────────────────────────────────────────────────────

THEMES: dict[str, dict[str, Any]] = {
    "DRIVERS_LICENSE": {
        "label":    "Führerausweis / Permis de conduire",
        "keywords": [
            "führerausweis", "fahrausweis", "fahrerlaubnis",
            "permis de conduire", "permis", "licenza di condurre",
            "driver", "driving licence", "license",
            "verloren", "ersatz", "erneuerung", "verlängerung",
            "renouvellement", "rinnovo", "annulation", "entzug",
        ],
    },
    "ROAD_INFRASTRUCTURE": {
        "label":    "Strasseninfrastruktur / Infrastructure routière",
        "keywords": [
            "strasse", "autobahn", "nationalstrasse", "a1", "a2", "a3",
            "route nationale", "autoroute", "strada nazionale",
            "markierung", "strassenmarkierung", "fahrbahnmarkierung",
            "panneau", "signalisation", "segnaletica",
            "beschädigt", "schaden", "defekt", "reparatur",
            "endommagé", "dommage", "réparation",
        ],
    },
    "NOISE_PROTECTION": {
        "label":    "Lärmschutz / Protection contre le bruit",
        "keywords": [
            "lärm", "lärmschutz", "lärmschutzwand", "lärmschutzmassnahme",
            "bruit", "protection bruit", "mur antibruit",
            "rumore", "barriera antirumore", "fonoassorbente",
            "noise", "sound barrier",
        ],
    },
    "TUNNEL_SAFETY": {
        "label":    "Tunnelsicherheit / Sécurité des tunnels",
        "keywords": [
            "tunnel", "gotthard", "san bernardino", "mont blanc",
            "sicherheit", "sécurité", "sicurezza", "safety",
            "belüftung", "ventilation", "ventilazione",
            "brand", "incendie", "incendio", "fire",
        ],
    },
    "VEHICLE_REGISTRATION": {
        "label":    "Fahrzeugzulassung / Immatriculation",
        "keywords": [
            "fahrzeug", "auto", "motorrad", "zulassung", "immatrikulation",
            "véhicule", "voiture", "immatriculation", "carte grise",
            "veicolo", "immatricolazione", "targa",
            "kontrollschild", "nummernschild", "plaque",
        ],
    },
    "GENERAL_INQUIRY": {
        "label":    "Allgemeine Anfrage / Demande générale",
        "keywords": [
            "information", "auskunft", "renseignement", "informazione",
            "frage", "question", "domanda", "anfrage", "demande",
            "zuständigkeit", "compétence", "competenza",
        ],
    },
}

# Topics explicitly OUT of ASTRA scope — with redirect targets
OUT_OF_SCOPE_REDIRECTS: dict[str, dict[str, str]] = {
    "TAXES": {
        "keywords": ["steuer", "steuern", "impôt", "impôts", "tassa", "steuererklärung"],
        "redirect": "Eidgenössische Steuerverwaltung (ESTV) — www.estv.admin.ch",
    },
    "PASSPORTS": {
        "keywords": ["pass", "reisepass", "passeport", "passaporto", "passport"],
        "redirect": "Zuständige Gemeindeverwaltung / Administration communale",
    },
    "HEALTH": {
        "keywords": ["gesundheit", "santé", "salute", "krankenversicherung",
                     "assurance maladie", "assicurazione malattia", "health"],
        "redirect": "Bundesamt für Gesundheit (BAG) — www.bag.admin.ch",
    },
    "IMMIGRATION": {
        "keywords": ["visum", "visa", "aufenthaltsbewilligung", "permis séjour",
                     "permesso soggiorno", "migration", "asyl", "asile", "asilo"],
        "redirect": "Staatssekretariat für Migration (SEM) — www.sem.admin.ch",
    },
}


# ─────────────────────────────────────────────────────────────
# ① QUESTION EXTRACTION
#
# In production: LLM prompt asking the model to list all
# distinct questions the citizen is asking, each on one line.
#
# In demo: three extraction strategies applied in sequence:
#   a) Explicit question sentences (ending in ?)
#   b) Implicit questions from key phrases ("ich möchte wissen",
#      "j'aimerais savoir", "could you tell me")
#   c) If nothing found: treat the whole message as one question
# ─────────────────────────────────────────────────────────────

# Sentence splitter — splits on . ? ! followed by whitespace/end
_SENT_RE = re.compile(r"(?<=[.?!])\s+")

# Implicit question markers across DE/FR/IT/EN
_IMPLICIT_MARKERS = [
    # German
    r"könnten sie (?:mir )?(?:bitte )?(?:mitteilen|sagen|informieren|erklären)",
    r"ich (?:möchte|würde gern(?:e)?)\s+(?:wissen|erfahren|verstehen)",
    r"bitte (?:teilen sie mir mit|informieren sie mich)",
    r"was (?:ist|sind|wird|wurde|geschieht|passiert)",
    r"wie (?:kann|könnte|wird|ist)",
    r"wann (?:wird|kann|ist)",
    r"wo (?:kann|ist|finde ich)",
    # French
    r"pourriez[- ]vous (?:me )?(?:dire|informer|expliquer|indiquer)",
    r"j['']aimerais (?:savoir|connaître|comprendre)",
    r"je (?:voudrais|souhaite) (?:savoir|connaître)",
    r"comment (?:puis[- ]je|faut[- ]il|dois[- ]je)",
    r"quand (?:puis[- ]je|sera|est)",
    r"où (?:puis[- ]je|est|se trouve)",
    # Italian
    r"potreste (?:dirmi|informarmi|spiegarmi)",
    r"vorrei (?:sapere|conoscere|capire)",
    r"come (?:posso|si può|devo)",
    # English
    r"could you (?:please )?(?:tell|inform|explain|let me know)",
    r"i would like to (?:know|understand|find out)",
    r"how (?:can i|do i|should i)",
    r"when (?:will|can|is)",
    r"where (?:can i|is|are)",
]
_IMPLICIT_RE = re.compile("|".join(_IMPLICIT_MARKERS), re.IGNORECASE)


def extract_questions(subject: str, body: str,
                      max_q: int = MAX_QUESTIONS) -> list[str]:
    """
    Extract atomic questions from the anonymised subject + body.

    Returns a list of question strings, capped at max_q.
    Always returns at least one question.
    """
    text = ((subject or "") + "\n\n" + (body or "")).strip()
    found: list[str] = []

    # Strategy a: explicit questions (contain ?)
    sentences = _SENT_RE.split(text)
    for sent in sentences:
        sent = sent.strip()
        if "?" in sent and len(sent) > 15:
            # Clean up and normalise
            q = _clean_question(sent)
            if q and q not in found:
                found.append(q)

    # Strategy b: implicit question sentences
    if not found:
        for sent in sentences:
            sent = sent.strip()
            if _IMPLICIT_RE.search(sent) and len(sent) > 20:
                q = _clean_question(sent)
                if q and q not in found:
                    found.append(q)

    # Strategy c: fallback — use subject as the single question
    if not found:
        fallback = (subject or "").strip()
        if not fallback:
            # Extract first meaningful sentence from body
            for sent in sentences:
                sent = sent.strip()
                if len(sent) > 20:
                    fallback = sent
                    break
        if fallback:
            found.append(_clean_question(fallback) or fallback)

    # Deduplicate and cap
    seen: set[str] = set()
    unique: list[str] = []
    for q in found:
        key = q.lower()[:60]
        if key not in seen:
            seen.add(key)
            unique.append(q)

    return unique[:max_q] if unique else [subject or "General inquiry"]


def _clean_question(text: str) -> str:
    """Normalise a question string: strip noise, ensure it ends with ?"""
    q = text.strip()
    # Remove common salutation prefixes
    q = re.sub(
        r"^(?:sehr geehrte[rn]?\s+\w+[,.]?\s*|madame[,.]?\s*monsieur[,.]?\s*"
        r"|gentile\s+\w+[,.]?\s*|dear\s+\w+[,.]?\s*)",
        "", q, flags=re.IGNORECASE,
    ).strip()
    # Remove [BBL-SCAN] and similar prefixes
    q = re.sub(r"^\[.*?\]\s*", "", q).strip()
    if not q:
        return ""
    # Ensure ends with ?
    if not q.endswith("?"):
        q = q.rstrip(".!,;") + "?"
    # Capitalise first letter
    q = q[0].upper() + q[1:] if q else q
    return q


# ─────────────────────────────────────────────────────────────
# ② SCOPE CHECK
#
# In production: LLM call with tenant mandate description.
# In demo: keyword matching against OUT_OF_SCOPE_REDIRECTS first,
#          then require at least one ASTRA theme keyword hit.
# ─────────────────────────────────────────────────────────────

def check_scope(question: str) -> tuple[str, str]:
    """
    Returns (scope, redirect_info).
    scope = "IN_SCOPE" | "OUT_OF_SCOPE"
    redirect_info = "" or the redirect target string
    """
    lower = question.lower()

    # Explicit out-of-scope check first
    for category, info in OUT_OF_SCOPE_REDIRECTS.items():
        if any(kw in lower for kw in info["keywords"]):
            return "OUT_OF_SCOPE", info["redirect"]

    # Must hit at least one ASTRA theme keyword
    for theme_info in THEMES.values():
        if any(kw in lower for kw in theme_info["keywords"]):
            return "IN_SCOPE", ""

    # No clear match — default to IN_SCOPE for ASTRA
    # (the operator can override during Validation)
    return "IN_SCOPE", ""


# ─────────────────────────────────────────────────────────────
# ③ THEME ASSIGNMENT
#
# In production: LLM call returning one of the configured themes.
# In demo: score each theme by keyword hits, pick highest score.
#          If tie or no match → GENERAL_INQUIRY.
# ─────────────────────────────────────────────────────────────

def assign_theme(question: str, context: str = "") -> str:
    """
    Return the best-matching ASTRA theme key for this question.
    context = subject + body text — used when the question itself
    is a generic phrase that doesn't contain domain keywords.
    """
    # Score against the question first, then fall back to full context
    for text in [question, context]:
        lower  = text.lower()
        scores = {}
        for theme_key, info in THEMES.items():
            score = sum(1 for kw in info["keywords"] if kw in lower)
            if score > 0:
                scores[theme_key] = score
        if scores:
            return max(scores, key=lambda k: scores[k])

    return "GENERAL_INQUIRY"


# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────

def init_decomp_db() -> sqlite3.Connection:
    """
    Tables:
      questions      — one row per atomic question per case
                       (identical schema to production)
      decomp_log     — append-only audit events
    """
    conn = sqlite3.connect(str(DECOMP_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS questions (
            id              TEXT PRIMARY KEY,
            case_id         TEXT NOT NULL,
            question        TEXT NOT NULL,
            scope           TEXT NOT NULL,   -- IN_SCOPE | OUT_OF_SCOPE
            theme           TEXT NOT NULL,
            redirect_info   TEXT,            -- populated for OUT_OF_SCOPE
            enriched_prompt TEXT,            -- set by Phase 07
            answer          TEXT,            -- set by Phase 08
            status          TEXT NOT NULL DEFAULT 'PENDING',
            iteration       INTEGER NOT NULL DEFAULT 0,
            quality_flagged INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS decomp_log (
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


def already_decomposed(dec_conn: sqlite3.Connection, case_id: str) -> bool:
    return dec_conn.execute(
        "SELECT 1 FROM questions WHERE case_id=?", (case_id,)
    ).fetchone() is not None


def log_event(dec_conn: sqlite3.Connection,
              case_id: str, event: str, detail: str = "") -> None:
    dec_conn.execute(
        "INSERT INTO decomp_log VALUES (?,?,?,?,?)",
        (str(uuid.uuid4()), case_id, event, detail,
         datetime.now(timezone.utc).isoformat()),
    )
    dec_conn.commit()


# ─────────────────────────────────────────────────────────────
# CORE DECOMPOSITION LOGIC
# ─────────────────────────────────────────────────────────────

def run_decomposition(case: dict[str, Any],
                      priv: dict[str, Any],
                      dec_conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """
    Decompose one case into atomic questions.
    Returns the list of question dicts saved to the DB.
    """
    case_id  = case["case_id"]
    subject  = priv.get("subject_anon") or ""
    body     = priv.get("body_anon")    or ""

    # ── ① Extract questions ───────────────────────────────────
    raw_questions = extract_questions(subject, body, MAX_QUESTIONS)
    log_event(dec_conn, case_id, "QUESTIONS_EXTRACTED",
              f"count={len(raw_questions)}")

    saved: list[dict[str, Any]] = []
    context = f"{subject} {body}"   # full text for theme context fallback

    for q_text in raw_questions:

        # ── ② Scope check ─────────────────────────────────────
        scope, redirect = check_scope(q_text)
        log_event(dec_conn, case_id, "SCOPE_CHECKED",
                  f"scope={scope} q={q_text[:60]}")

        # ── ③ Theme assignment — pass full context as fallback ─
        theme = assign_theme(q_text, context) if scope == "IN_SCOPE" else "OUT_OF_SCOPE"
        log_event(dec_conn, case_id, "THEME_ASSIGNED",
                  f"theme={theme} q={q_text[:60]}")

        row = {
            "id":            str(uuid.uuid4()),
            "case_id":       case_id,
            "question":      q_text,
            "scope":         scope,
            "theme":         theme,
            "redirect_info": redirect or None,
            "enriched_prompt": None,
            "answer":        None,
            "status":        "PENDING",
            "iteration":     0,
            "quality_flagged": 0,
            "created_at":    datetime.now(timezone.utc).isoformat(),
        }
        dec_conn.execute("""
            INSERT INTO questions (
                id, case_id, question, scope, theme,
                redirect_info, enriched_prompt, answer,
                status, iteration, quality_flagged, created_at
            ) VALUES (
                :id, :case_id, :question, :scope, :theme,
                :redirect_info, :enriched_prompt, :answer,
                :status, :iteration, :quality_flagged, :created_at
            )
        """, row)
        saved.append(row)

    dec_conn.commit()

    # ── Advance pipeline step ──────────────────────────────────
    in_scope  = sum(1 for q in saved if q["scope"] == "IN_SCOPE")
    out_scope = len(saved) - in_scope
    try:
        ing = sqlite3.connect(str(INGESTION_DB_PATH),
                              check_same_thread=False)
        ing.execute(
            "UPDATE cases SET pipeline_step='DECOMPOSITION_DONE' WHERE case_id=?",
            (case_id,),
        )
        ing.commit()
        ing.close()
    except Exception:
        pass

    log_event(dec_conn, case_id, "DECOMPOSITION_DONE",
              f"total={len(saved)} in_scope={in_scope} out_scope={out_scope}")

    return saved


# ─────────────────────────────────────────────────────────────
# POLLING LOOP
# ─────────────────────────────────────────────────────────────

def poll_and_decompose(dec_conn: sqlite3.Connection,
                       stop_event: threading.Event) -> None:
    print("  [Decomposition] polling Analysis DB every 5 seconds...")

    while not stop_event.is_set():
        ing_db  = open_db_ro(INGESTION_DB_PATH)
        priv_db = open_db_ro(PRIVACY_DB_PATH)

        if not all([ing_db, priv_db]):
            print("  [Decomposition] waiting for upstream DBs...")
            stop_event.wait(5)
            for db in [ing_db, priv_db]:
                try:
                    if db: db.close()
                except Exception:
                    pass
            continue

        try:
            pending = ing_db.execute(
                "SELECT * FROM cases WHERE pipeline_step='ANALYSIS_DONE'"
            ).fetchall()
        finally:
            ing_db.close()

        new_count = 0
        for row in pending:
            case = dict(row)
            if already_decomposed(dec_conn, case["case_id"]):
                continue

            try:
                priv_row = priv_db.execute(
                    "SELECT * FROM privacy_results WHERE case_id=?",
                    (case["case_id"],),
                ).fetchone()
                priv = dict(priv_row) if priv_row else {}
            except Exception:
                priv = {}

            try:
                questions = run_decomposition(case, priv, dec_conn)
            except Exception as exc:
                print(f"  [Decomposition] ERROR on case {case['case_id'][:8]}: {exc}")
                try:
                    dec_conn.rollback()
                except Exception:
                    pass
                continue
            new_count += 1
            _print_result(case, questions)

        if new_count:
            print(f"\n  [Decomposition] ✓ {new_count} case(s) decomposed.\n")

        try:
            priv_db.close()
        except Exception:
            pass

        stop_event.wait(5)


# ─────────────────────────────────────────────────────────────
# TERMINAL PRINTER
# ─────────────────────────────────────────────────────────────

_SCOPE_ICON  = {"IN_SCOPE": "✅", "OUT_OF_SCOPE": "🚫"}
_THEME_ICONS = {
    "DRIVERS_LICENSE":      "🪪",
    "ROAD_INFRASTRUCTURE":  "🛣",
    "NOISE_PROTECTION":     "🔇",
    "TUNNEL_SAFETY":        "🚇",
    "VEHICLE_REGISTRATION": "🚗",
    "GENERAL_INQUIRY":      "💬",
    "OUT_OF_SCOPE":         "🚫",
}


def _print_result(case: dict, questions: list[dict]) -> None:
    in_scope  = [q for q in questions if q["scope"] == "IN_SCOPE"]
    out_scope = [q for q in questions if q["scope"] == "OUT_OF_SCOPE"]

    print(f"\n{'─'*60}")
    print(f"  🔬  Decomposition complete")
    print(f"  Case ID : {case['case_id']}")
    print(f"  Source  : {case['source_type']}")
    print(f"  Questions: {len(questions)} total  "
          f"({len(in_scope)} in-scope, {len(out_scope)} out-of-scope)")
    print()
    for i, q in enumerate(questions, 1):
        icon  = _SCOPE_ICON.get(q["scope"], "?")
        ticon = _THEME_ICONS.get(q["theme"], "")
        print(f"  Q{i} {icon} [{q['scope']:12}] {ticon} {q['theme']}")
        print(f"      {q['question']}")
        if q.get("redirect_info"):
            print(f"      → Redirect: {q['redirect_info']}")

    print(f"\n  → Next step: Phase 07 Prompt Enrichment")
    print(json.dumps({
        "case_id":      case["case_id"],
        "tenant_id":    case["tenant_id"],
        "in_scope":     len(in_scope),
        "out_of_scope": len(out_scope),
        "themes":       list({q["theme"] for q in in_scope}),
        "step":         "PROMPT_ENRICHMENT",
    }, indent=4))


# ─────────────────────────────────────────────────────────────
# WEB DASHBOARD  (port 8005)
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
       letter-spacing:.06em;text-transform:uppercase;background:#16a34a;color:white}
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
td{padding:10px 14px;border-bottom:1px solid #f3f4f6;color:#374151;vertical-align:top}
tr:last-child td{border-bottom:none}
tr:hover td{background:#fafafa}

.scope-pill{display:inline-block;font-size:10px;font-weight:700;
            padding:2px 8px;border-radius:10px}
.in-scope  {background:#d1fae5;color:#065f46}
.out-scope {background:#fee2e2;color:#991b1b}

.theme-pill{display:inline-block;font-size:10px;font-weight:600;
            padding:2px 7px;border-radius:4px;
            background:#e0f2fe;color:#0369a1;margin-top:3px}

.q-row{padding:10px 16px;border-bottom:1px solid #f3f4f6;display:flex;
       align-items:flex-start;gap:12px}
.q-row:last-child{border-bottom:none}
.q-num{font-family:monospace;font-size:11px;color:#9ca3af;
       flex-shrink:0;padding-top:2px;width:20px}
.q-body{flex:1;min-width:0}
.q-text{font-size:13px;color:#1f2937;line-height:1.5;
        font-style:italic}
.q-text.out{color:#9ca3af;text-decoration:line-through}
.q-meta{display:flex;align-items:center;gap:6px;margin-top:5px;flex-wrap:wrap}
.q-redirect{font-size:10px;color:#d97706;
            background:#fef3c7;padding:2px 7px;border-radius:3px}

.theme-chart{display:grid;grid-template-columns:1fr 1fr;gap:8px;padding:16px}
.theme-bar{display:flex;align-items:center;gap:8px}
.theme-label{font-size:11px;color:#374151;width:180px;flex-shrink:0}
.bar-track{flex:1;background:#f3f4f6;border-radius:3px;height:10px;overflow:hidden}
.bar-fill{height:100%;border-radius:3px;background:#0ea5e9}
.bar-count{font-size:11px;color:#6b7280;width:20px;text-align:right;flex-shrink:0}

.log-panel{background:#1a2332;border-radius:7px;
           padding:16px 20px;max-height:220px;overflow-y:auto}
.log-line{font-family:'Menlo',monospace;font-size:11px;
          color:#8b949e;padding:2px 0;line-height:1.6}
.ts{color:#484f58}
.ev-extract{color:#388bfd}
.ev-scope  {color:#2ea043}
.ev-theme  {color:#a371f7}
.ev-done   {color:#d29922}
.ev-out    {color:#f85149}

.refresh-note{font-size:11px;color:#9ca3af;text-align:center;margin-top:8px}
.empty{text-align:center;color:#9ca3af;padding:24px;font-size:13px}
"""


def _scope_pill(scope: str) -> str:
    cls  = "in-scope" if scope == "IN_SCOPE" else "out-scope"
    icon = "✅" if scope == "IN_SCOPE" else "🚫"
    return f'<span class="scope-pill {cls}">{icon} {scope}</span>'


def _theme_pill(theme: str) -> str:
    label = THEMES.get(theme, {}).get("label", theme)
    icon  = _THEME_ICONS.get(theme, "")
    return f'<span class="theme-pill">{icon} {label}</span>'


def render_dashboard(dec_conn: sqlite3.Connection) -> str:
    questions = dec_conn.execute(
        "SELECT * FROM questions ORDER BY created_at DESC"
    ).fetchall()
    questions = [dict(r) for r in questions]

    logs = dec_conn.execute(
        "SELECT * FROM decomp_log ORDER BY ts DESC LIMIT 100"
    ).fetchall()
    logs = [dict(r) for r in logs]

    # Case metadata
    case_meta: dict[str, dict] = {}
    ing_db = open_db_ro(INGESTION_DB_PATH)
    if ing_db:
        try:
            for r in ing_db.execute(
                "SELECT case_id, source_type, subject, pipeline_step, mood FROM cases"
            ):
                case_meta[r["case_id"]] = dict(r)
        finally:
            ing_db.close()

    # Stats
    cases_done   = len({q["case_id"] for q in questions})
    total_q      = len(questions)
    in_scope_n   = sum(1 for q in questions if q["scope"] == "IN_SCOPE")
    out_scope_n  = total_q - in_scope_n
    themes_used  = len({q["theme"] for q in questions if q["scope"] == "IN_SCOPE"})

    stats_html = f"""<div class="stats">
      <div class="stat"><div class="stat-num">{cases_done}</div>
        <div class="stat-label">Cases</div></div>
      <div class="stat"><div class="stat-num">{total_q}</div>
        <div class="stat-label">Questions</div></div>
      <div class="stat"><div class="stat-num" style="color:#065f46">{in_scope_n}</div>
        <div class="stat-label">In-Scope</div></div>
      <div class="stat"><div class="stat-num" style="color:#dc2626">{out_scope_n}</div>
        <div class="stat-label">Out-of-Scope</div></div>
      <div class="stat"><div class="stat-num" style="color:#0369a1">{themes_used}</div>
        <div class="stat-label">Themes</div></div>
    </div>"""

    # Group questions by case for the main table
    by_case: dict[str, list[dict]] = {}
    for q in questions:
        by_case.setdefault(q["case_id"], []).append(q)

    src_labels = {"DIRECT_EMAIL": "Email", "STAFF_FORWARD": "Staff",
                  "POSTAL_SCAN": "Scan",  "WEB_FORM": "Web Form"}

    case_rows = ""
    if not by_case:
        case_rows = '<tr><td colspan="4" class="empty">Waiting for Analysis to complete...</td></tr>'

    for cid, qs in by_case.items():
        meta  = case_meta.get(cid, {})
        src   = src_labels.get(meta.get("source_type", ""), "")
        subj  = _html.escape((meta.get("subject") or "")[:40])
        q_html = ""
        for i, q in enumerate(qs, 1):
            txt_cls  = "q-text out" if q["scope"] == "OUT_OF_SCOPE" else "q-text"
            redirect = ""
            if q.get("redirect_info"):
                redirect = f'<span class="q-redirect">→ {_html.escape(q["redirect_info"])}</span>'
            q_html += f"""<div class="q-row">
              <div class="q-num">Q{i}</div>
              <div class="q-body">
                <div class="{txt_cls}">{_html.escape(q['question'])}</div>
                <div class="q-meta">
                  {_scope_pill(q['scope'])}
                  {_theme_pill(q['theme']) if q['scope'] == 'IN_SCOPE' else ''}
                  {redirect}
                </div>
              </div>
            </div>"""

        case_rows += f"""<tr>
          <td><code style="font-size:11px;color:#6b7280">{cid[:8]}…</code></td>
          <td>{src}</td>
          <td title="{_html.escape(meta.get('subject',''))}">{subj}</td>
          <td style="padding:0">{q_html}</td>
        </tr>"""

    # Theme distribution bar chart
    theme_counts: dict[str, int] = {}
    for q in questions:
        if q["scope"] == "IN_SCOPE":
            theme_counts[q["theme"]] = theme_counts.get(q["theme"], 0) + 1

    max_count = max(theme_counts.values(), default=1)
    bars = ""
    for theme_key, info in THEMES.items():
        cnt = theme_counts.get(theme_key, 0)
        pct = int(cnt / max_count * 100) if max_count else 0
        icon = _THEME_ICONS.get(theme_key, "")
        bars += f"""<div class="theme-bar">
          <div class="theme-label">{icon} {info['label'][:28]}</div>
          <div class="bar-track"><div class="bar-fill" style="width:{pct}%"></div></div>
          <div class="bar-count">{cnt}</div>
        </div>"""

    # Log
    ev_css = {
        "QUESTIONS_EXTRACTED":  "ev-extract",
        "SCOPE_CHECKED":        "ev-scope",
        "THEME_ASSIGNED":       "ev-theme",
        "DECOMPOSITION_DONE":   "ev-done",
    }
    log_lines = ""
    for lg in logs:
        ev  = lg["event"]
        css = ev_css.get(ev, "")
        if ev == "SCOPE_CHECKED" and "OUT_OF_SCOPE" in (lg.get("detail") or ""):
            css = "ev-out"
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
<title>Phase 06 — Decomposition</title>
<style>{CSS}</style>
</head>
<body>
<div class="page">
  <div class="header">
    <div>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
        <span class="badge">Phase 06</span>
        <h1>Decomposition</h1>
      </div>
      <p>Question extraction · Scope check · Theme assignment</p>
    </div>
    <div class="hdr-right">
      Polling Analysis every 5s<br>
      <span style="color:#2ea043;font-weight:600">● Live</span>
    </div>
  </div>

  {stats_html}

  <div class="sec-title">Questions per case</div>
  <div class="card">
    <table>
      <thead>
        <tr>
          <th>Case ID</th><th>Source</th>
          <th>Subject</th><th>Extracted Questions</th>
        </tr>
      </thead>
      <tbody>{case_rows}</tbody>
    </table>
  </div>

  <div class="sec-title">Theme distribution</div>
  <div class="card">
    <div class="theme-chart">{bars}</div>
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

class DecompDashboardHandler(http.server.BaseHTTPRequestHandler):
    dec_conn: sqlite3.Connection = None  # type: ignore

    def log_message(self, *args: object) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/favicon.ico":
            self.send_response(204); self.end_headers(); return
        page = render_dashboard(self.dec_conn)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode())


def make_handler(conn: sqlite3.Connection):
    class H(DecompDashboardHandler):
        dec_conn = conn
    return H


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "═"*60)
    print("  CITIZEN PLATFORM — Phase 06: Decomposition  (DEMO)")
    print("═"*60)

    dec_conn = init_decomp_db()
    print(f"\n  ✓  Decomposition DB : {DECOMP_DB_PATH}")
    print(f"  ✓  Reading from     : Analysis DB + Privacy DB")

    stop_event = threading.Event()
    poller = threading.Thread(
        target=poll_and_decompose,
        args=(dec_conn, stop_event),
        daemon=True,
    )
    poller.start()

    handler    = make_handler(dec_conn)
    server     = http.server.HTTPServer(("", PORT), handler)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    print(f"\n  ✓  Dashboard: http://localhost:{PORT}")
    print(f"\n  Run order:")
    print(f"    Terminal 1 → python3 demo/reception.py        (port 8000)")
    print(f"    Terminal 2 → python3 demo/ingestion.py        (port 8001)")
    print(f"    Terminal 3 → python3 demo/security.py         (port 8002)")
    print(f"    Terminal 4 → python3 demo/privacy.py          (port 8003)")
    print(f"    Terminal 5 → python3 demo/analysis.py         (port 8004)")
    print(f"    Terminal 6 → python3 demo/decomposition.py    (port 8005)")
    print(f"\n  Press Ctrl-C to stop.\n")
    print("─"*60)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n\n  Stopping decomposition...")
        stop_event.set()
        server.shutdown()
        print("  Done.\n")


if __name__ == "__main__":
    main()
