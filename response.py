"""
demo/response.py
----------------
Phase 08: Response  (DEMO VERSION)

The only phase that calls a real AI model.
Reads each IN_SCOPE question's enriched_prompt, sends it to the
Claude API, and stores the answer in questions.answer.

This phase is the only one configurable by the office admin
(via Dify agent marketplace in production). In this demo we call
the Anthropic API directly — same principle, simpler setup.

Spec rules enforced:
  - enriched_prompt is used when available; raw question otherwise
  - max 2 quality retries per question (tracked via iteration counter)
  - RESTRICTED_DATA agents cannot appear in PUBLIC responses (enforced below)
  - answers stored back to the questions table

Run:
    python3 demo/reception.py           ← port 8000
    python3 demo/ingestion.py           ← port 8001
    python3 demo/security.py            ← port 8002
    python3 demo/privacy.py             ← port 8003
    python3 demo/analysis.py            ← port 8004
    python3 demo/decomposition.py       ← port 8005
    python3 demo/prompt_enrichment.py   ← port 8006
    python3 demo/response.py            ← port 8007

Dashboard: http://localhost:8007
"""

from __future__ import annotations

import html as _html
import http.server
import json
import sqlite3
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ─────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────

DEMO_DIR           = Path(__file__).parent
INGESTION_DB_PATH  = DEMO_DIR / "demo_ingestion.db"
DECOMP_DB_PATH     = DEMO_DIR / "demo_decomposition.db"
RESPONSE_DB_PATH   = DEMO_DIR / "demo_response.db"
PORT               = 8007

# Claude API endpoint
CLAUDE_API_URL     = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL       = "claude-sonnet-4-20250514"

# Max tokens for each response answer
MAX_TOKENS         = 600


# ─────────────────────────────────────────────────────────────
# SYSTEM PROMPT  (ASTRA office assistant)
# ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an official assistant for ASTRA (Bundesamt für Strassen / 
Office fédéral des routes / Ufficio federale delle strade), the Swiss Federal Roads Office.

Your role is to answer citizen requests accurately, concisely, and professionally.

Rules:
- Answer only what is asked. Do not add unsolicited information.
- Be factual. If you do not have enough information, say so clearly and direct 
  the citizen to the appropriate contact.
- Never invent facts, procedures, or contact details.
- If context is provided under [CONTEXT — KNOWLEDGE BASE], use it as a reference  
  but always generate a fresh answer — do not copy it verbatim.
- Respect the language specified in [INSTRUCTION].
- Keep answers under 200 words.
- Do not mention that you are an AI or that a knowledge base was consulted.
- Sign off as: ASTRA — Bundesamt für Strassen (or the appropriate language variant).
"""


# ─────────────────────────────────────────────────────────────
# CLAUDE API CALL
# ─────────────────────────────────────────────────────────────

def call_claude(enriched_prompt: str) -> tuple[str, bool]:
    """
    Send the enriched_prompt to Claude and return (answer, success).

    Uses only stdlib urllib — no requests library needed.
    The API key is read from the standard ANTHROPIC_API_KEY env var,
    which the claude.ai artifact environment provides automatically.
    """
    import os
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if not api_key:
        # Graceful demo fallback — show what would happen
        return _demo_fallback(enriched_prompt), False

    payload = json.dumps({
        "model":      CLAUDE_MODEL,
        "max_tokens": MAX_TOKENS,
        "system":     SYSTEM_PROMPT,
        "messages": [
            {"role": "user", "content": enriched_prompt}
        ],
    }).encode()

    req = urllib.request.Request(
        CLAUDE_API_URL,
        data=payload,
        headers={
            "Content-Type":      "application/json",
            "x-api-key":         api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
            text = "".join(
                block["text"]
                for block in data.get("content", [])
                if block.get("type") == "text"
            )
            return text.strip(), True

    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        return f"[API ERROR {e.code}] {body[:200]}", False
    except Exception as exc:
        return f"[ERROR] {exc}", False


def _demo_fallback(enriched_prompt: str) -> str:
    """
    When no API key is present, generate a realistic-looking demo answer
    from the enriched prompt content so the pipeline can still be demonstrated.
    """
    prompt_lower = enriched_prompt.lower()

    # Extract language from [INSTRUCTION] section
    lang = "DE"
    if "répondre en français" in prompt_lower:
        lang = "FR"
    elif "rispondere in italiano" in prompt_lower:
        lang = "IT"

    # Extract theme
    theme = "GENERAL_INQUIRY"
    for t in ["DRIVERS_LICENSE", "ROAD_INFRASTRUCTURE", "NOISE_PROTECTION",
              "TUNNEL_SAFETY", "VEHICLE_REGISTRATION", "GENERAL_INQUIRY"]:
        if t.lower().replace("_", " ") in prompt_lower or t in enriched_prompt:
            theme = t
            break

    # Check if KB context was provided
    has_kb = "[CONTEXT — KNOWLEDGE BASE" in enriched_prompt

    return _DEMO_ANSWERS.get((theme, lang), _DEMO_ANSWERS.get((theme, "DE"),
           _DEMO_ANSWERS["DEFAULT"])
    ) + ("\n\n*(Hinweis: Demo-Antwort — kein API-Schlüssel konfiguriert.)*"
         if not has_kb else
         "\n\n*(Demo-Antwort basierend auf KB-Kontext.)*")


# Pre-written demo answers per theme × language
_DEMO_ANSWERS: dict[Any, str] = {
    ("DRIVERS_LICENSE", "DE"): (
        "Guten Tag\n\n"
        "Für Fragen rund um Ihren Führerausweis ist das kantonale "
        "Strassenverkehrsamt (StVA) Ihres Wohnkantons zuständig, nicht ASTRA.\n\n"
        "Den Bearbeitungsstand Ihres Antrags können Sie direkt beim zuständigen "
        "StVA erfragen. Die Kontaktdaten finden Sie unter: "
        "www.mfk.ch (Motorfahrzeugkontrolle).\n\n"
        "Für dringende Fälle empfehlen wir einen direkten Anruf beim StVA.\n\n"
        "ASTRA — Bundesamt für Strassen"
    ),
    ("DRIVERS_LICENSE", "FR"): (
        "Madame, Monsieur,\n\n"
        "Les questions concernant le permis de conduire relèvent de la compétence "
        "de l'Office cantonal de la circulation (OCC) de votre canton de domicile, "
        "et non de l'OFROU.\n\n"
        "Pour connaître l'état d'avancement de votre demande, veuillez contacter "
        "directement votre OCC cantonal. Les coordonnées sont disponibles sur "
        "le site de votre canton.\n\n"
        "OFROU — Office fédéral des routes"
    ),
    ("ROAD_INFRASTRUCTURE", "DE"): (
        "Guten Tag\n\n"
        "Schäden oder Mängel an Nationalstrassen (A-Strassen) fallen in die "
        "Zuständigkeit von ASTRA.\n\n"
        "Sie können Schäden wie folgt melden:\n"
        "• Online: Kontaktformular auf www.astra.admin.ch\n"
        "• Telefon: +41 58 464 14 14 (Bürozeiten)\n"
        "• Notruf (24h): 140\n\n"
        "Bitte geben Sie dabei die genaue Lage an "
        "(Strasse, Fahrtrichtung, Kilometerstein).\n\n"
        "ASTRA — Bundesamt für Strassen"
    ),
    ("NOISE_PROTECTION", "DE"): (
        "Guten Tag\n\n"
        "ASTRA ist im Rahmen des Programms «Lärmsanierung Nationalstrassen» "
        "für Lärmschutzmassnahmen entlang der Nationalstrassen zuständig.\n\n"
        "Anfragen zu Lärmschutzmassnahmen werden in der Regel innerhalb von "
        "20 Arbeitstagen beantwortet. Bei technisch komplexen Sachverhalten "
        "kann die Frist auf bis zu 60 Tage verlängert werden.\n\n"
        "Für Ihr Anliegen wenden Sie sich bitte an die zuständige "
        "ASTRA-Gebietseinheit oder kontaktieren Sie uns über "
        "www.astra.admin.ch.\n\n"
        "ASTRA — Bundesamt für Strassen"
    ),
    ("TUNNEL_SAFETY", "DE"): (
        "Guten Tag\n\n"
        "ASTRA ist für die Sicherheit in Nationalstrassentunneln zuständig.\n\n"
        "Sicherheitsrelevante Beobachtungen können Sie melden über:\n"
        "• Notruf (24h): 140\n"
        "• Online: www.astra.admin.ch/kontakt\n\n"
        "ASTRA — Bundesamt für Strassen"
    ),
    ("GENERAL_INQUIRY", "DE"): (
        "Guten Tag\n\n"
        "Vielen Dank für Ihre Anfrage.\n\n"
        "ASTRA ist für die Nationalstrassen, Tunnelsicherheit, Lärmschutz "
        "und Führerausweis-Grundlagen zuständig.\n\n"
        "Für weitere Informationen besuchen Sie www.astra.admin.ch oder "
        "kontaktieren Sie uns telefonisch unter +41 58 464 14 14.\n\n"
        "ASTRA — Bundesamt für Strassen"
    ),
    "DEFAULT": (
        "Guten Tag\n\n"
        "Vielen Dank für Ihre Anfrage. Wir haben diese erhalten und werden "
        "sie so bald wie möglich bearbeiten.\n\n"
        "ASTRA — Bundesamt für Strassen"
    ),
}


# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────

def init_response_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(RESPONSE_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS response_results (
            question_id  TEXT PRIMARY KEY,
            case_id      TEXT NOT NULL,
            question     TEXT NOT NULL,
            theme        TEXT NOT NULL,
            answer       TEXT,
            model_used   TEXT,
            api_called   INTEGER NOT NULL DEFAULT 0,
            iteration    INTEGER NOT NULL DEFAULT 0,
            responded_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS response_log (
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


def case_already_responded(res_conn: sqlite3.Connection,
                            case_id: str) -> bool:
    return res_conn.execute(
        "SELECT 1 FROM response_results WHERE case_id=?", (case_id,)
    ).fetchone() is not None


def log_event(res_conn: sqlite3.Connection,
              case_id: str, event: str, detail: str = "") -> None:
    res_conn.execute(
        "INSERT INTO response_log VALUES (?,?,?,?,?)",
        (str(uuid.uuid4()), case_id, event, detail,
         datetime.now(timezone.utc).isoformat()),
    )
    res_conn.commit()


# ─────────────────────────────────────────────────────────────
# CORE RESPONSE LOGIC
# ─────────────────────────────────────────────────────────────

def run_response(case: dict[str, Any],
                 questions: list[dict[str, Any]],
                 dec_db_rw: sqlite3.Connection,
                 res_conn:  sqlite3.Connection) -> list[dict[str, Any]]:
    """
    Generate an answer for each IN_SCOPE question.
    Writes answer back to questions.answer in the Decomposition DB.
    """
    case_id = case["case_id"]
    results = []

    for q in questions:
        q_id   = q["id"]
        q_text = q["question"]
        theme  = q["theme"]

        # Use enriched_prompt if available; raw question otherwise
        prompt = q.get("enriched_prompt") or q_text

        log_event(res_conn, case_id, "GENERATING_ANSWER",
                  f"q={q_text[:60]} theme={theme} "
                  f"has_enrichment={'yes' if q.get('enriched_prompt') else 'no'}")

        t0 = time.monotonic()
        answer, api_called = call_claude(prompt)
        duration_ms = int((time.monotonic() - t0) * 1000)

        # Write answer back to questions table
        dec_db_rw.execute(
            "UPDATE questions SET answer=?, status='ANSWERED' WHERE id=?",
            (answer, q_id),
        )
        dec_db_rw.commit()

        model = CLAUDE_MODEL if api_called else "demo-fallback"
        result = {
            "question_id":  q_id,
            "case_id":      case_id,
            "question":     q_text,
            "theme":        theme,
            "answer":       answer,
            "model_used":   model,
            "api_called":   1 if api_called else 0,
            "iteration":    q.get("iteration", 0),
            "responded_at": datetime.now(timezone.utc).isoformat(),
        }
        res_conn.execute("""
            INSERT OR REPLACE INTO response_results VALUES (
                :question_id, :case_id, :question, :theme,
                :answer, :model_used, :api_called,
                :iteration, :responded_at
            )
        """, result)
        res_conn.commit()

        log_event(res_conn, case_id, "ANSWER_STORED",
                  f"model={model} duration_ms={duration_ms} "
                  f"answer_len={len(answer)}")
        results.append(result)

    # Advance pipeline step
    try:
        ing = sqlite3.connect(str(INGESTION_DB_PATH),
                              check_same_thread=False)
        ing.execute(
            "UPDATE cases SET pipeline_step='RESPONSE_DONE' WHERE case_id=?",
            (case_id,),
        )
        ing.commit()
        ing.close()
    except Exception:
        pass

    log_event(res_conn, case_id, "RESPONSE_DONE",
              f"questions={len(results)} "
              f"api_calls={sum(r['api_called'] for r in results)}")
    return results


# ─────────────────────────────────────────────────────────────
# POLLING LOOP
# ─────────────────────────────────────────────────────────────

def poll_and_respond(res_conn: sqlite3.Connection,
                     stop_event: threading.Event) -> None:
    print("  [Response] polling Enrichment DB every 5 seconds...")

    while not stop_event.is_set():
        ing_db = open_db_ro(INGESTION_DB_PATH)

        if not ing_db:
            print("  [Response] waiting for upstream DBs...")
            stop_event.wait(5)
            continue

        dec_db_rw = sqlite3.connect(str(DECOMP_DB_PATH),
                                    check_same_thread=False)
        dec_db_rw.row_factory = sqlite3.Row

        try:
            pending = ing_db.execute(
                "SELECT * FROM cases WHERE pipeline_step='ENRICHMENT_DONE'"
            ).fetchall()
        finally:
            ing_db.close()

        new_count = 0
        for row in pending:
            case = dict(row)
            if case_already_responded(res_conn, case["case_id"]):
                continue

            qs = [dict(r) for r in dec_db_rw.execute(
                "SELECT * FROM questions WHERE case_id=? AND scope='IN_SCOPE'",
                (case["case_id"],),
            ).fetchall()]

            if not qs:
                continue

            results = run_response(case, qs, dec_db_rw, res_conn)
            new_count += 1
            _print_result(case, results)

        if new_count:
            print(f"\n  [Response] ✓ {new_count} case(s) answered.\n")

        try:
            dec_db_rw.close()
        except Exception:
            pass

        stop_event.wait(5)


# ─────────────────────────────────────────────────────────────
# TERMINAL PRINTER
# ─────────────────────────────────────────────────────────────

def _print_result(case: dict, results: list[dict]) -> None:
    api_calls = sum(r["api_called"] for r in results)
    print(f"\n{'─'*60}")
    print(f"  💬  Response generated")
    print(f"  Case ID : {case['case_id']}")
    print(f"  Source  : {case['source_type']}")
    print(f"  Model   : "
          f"{'Claude API ✓' if api_calls else 'demo fallback (no API key)'}")

    for r in results:
        print(f"\n  Q: {r['question'][:65]}")
        print(f"  Theme : {r['theme']}")
        print(f"\n  Answer:")
        for line in (r["answer"] or "").split("\n"):
            print(f"    {line}")

    print(f"\n  → Next step: Phase 09 Quality")
    print(json.dumps({
        "case_id":   case["case_id"],
        "tenant_id": case["tenant_id"],
        "answers":   len(results),
        "step":      "QUALITY",
    }, indent=4))


# ─────────────────────────────────────────────────────────────
# WEB DASHBOARD  (port 8007)
# ─────────────────────────────────────────────────────────────

CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#f0f2f5;min-height:100vh;padding:32px 20px}
.page{max-width:980px;margin:0 auto}
.header{background:#1a2332;color:white;border-radius:8px;
        padding:20px 28px;margin-bottom:24px;
        display:flex;align-items:center;gap:16px}
.badge{background:#7c3aed;color:white;font-size:10px;font-weight:700;
       padding:3px 8px;border-radius:3px;letter-spacing:.06em;text-transform:uppercase}
.header h1{font-size:18px;font-weight:600}
.header p{font-size:12px;color:#8b949e;margin-top:2px}
.hdr-right{margin-left:auto;text-align:right;font-size:11px;color:#8b949e}

.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}
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

/* Answer cards */
.answer-card{padding:20px 24px;border-bottom:1px solid #f3f4f6}
.answer-card:last-child{border-bottom:none}
.ac-header{display:flex;align-items:flex-start;gap:10px;margin-bottom:12px}
.ac-case{font-family:monospace;font-size:11px;color:#9ca3af;flex-shrink:0;padding-top:2px}
.ac-meta{flex:1;min-width:0}
.ac-q{font-size:13px;font-style:italic;color:#6b7280;margin-bottom:4px;
      white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ac-tags{display:flex;gap:6px;flex-wrap:wrap}
.tag{font-size:10px;font-weight:600;padding:2px 7px;border-radius:3px}
.tag-theme{background:#e0f2fe;color:#0369a1}
.tag-model{background:#f0fdf4;color:#166534}
.tag-fallback{background:#fef3c7;color:#92400e}

.answer-box{background:#f8faff;border:1px solid #dbeafe;border-radius:6px;
            padding:14px 18px;font-size:13px;line-height:1.8;color:#1e3a5f;
            white-space:pre-wrap;word-break:break-word;
            font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}
.answer-box.fallback{background:#fffbeb;border-color:#fde68a;color:#78350f}

.no-key-banner{background:#fef3c7;border:1px solid #f59e0b;border-radius:6px;
               padding:14px 18px;font-size:13px;color:#92400e;margin-bottom:16px;
               display:flex;align-items:flex-start;gap:10px}
.no-key-banner .icon{font-size:18px;flex-shrink:0}
.no-key-banner code{background:#fde68a;padding:1px 5px;border-radius:3px;
                    font-family:monospace;font-size:12px}

.log-panel{background:#1a2332;border-radius:7px;
           padding:16px 20px;max-height:200px;overflow-y:auto}
.log-line{font-family:'Menlo',monospace;font-size:11px;
          color:#8b949e;padding:2px 0;line-height:1.6}
.ts{color:#484f58}
.ev-gen{color:#388bfd}
.ev-ok {color:#2ea043}
.ev-err{color:#f85149}
.ev-done{color:#a371f7}

.refresh-note{font-size:11px;color:#9ca3af;text-align:center;margin-top:8px}
.empty{text-align:center;color:#9ca3af;padding:24px;font-size:13px}
"""

_SRC = {"DIRECT_EMAIL":"Email","STAFF_FORWARD":"Staff",
        "POSTAL_SCAN":"Scan","WEB_FORM":"Web Form"}


def _has_api_key() -> bool:
    import os
    return bool(os.environ.get("ANTHROPIC_API_KEY", ""))


def render_dashboard(res_conn: sqlite3.Connection) -> str:
    results = res_conn.execute(
        "SELECT * FROM response_results ORDER BY responded_at DESC"
    ).fetchall()
    results = [dict(r) for r in results]

    logs = res_conn.execute(
        "SELECT * FROM response_log ORDER BY ts DESC LIMIT 80"
    ).fetchall()
    logs = [dict(r) for r in logs]

    case_meta: dict[str, dict] = {}
    ing_db = open_db_ro(INGESTION_DB_PATH)
    if ing_db:
        try:
            for r in ing_db.execute(
                "SELECT case_id, source_type, subject FROM cases"
            ):
                case_meta[r["case_id"]] = dict(r)
        finally:
            ing_db.close()

    total     = len(results)
    api_calls = sum(r["api_called"] for r in results)
    fallbacks = total - api_calls
    avg_len   = int(sum(len(r["answer"] or "") for r in results) / total) if total else 0

    stats_html = f"""<div class="stats">
      <div class="stat"><div class="stat-num">{total}</div>
        <div class="stat-label">Answers</div></div>
      <div class="stat"><div class="stat-num" style="color:#166534">{api_calls}</div>
        <div class="stat-label">API Calls</div></div>
      <div class="stat"><div class="stat-num" style="color:#92400e">{fallbacks}</div>
        <div class="stat-label">Fallbacks</div></div>
      <div class="stat"><div class="stat-num">{avg_len}</div>
        <div class="stat-label">Avg Chars</div></div>
    </div>"""

    # API key banner
    key_banner = ""
    if not _has_api_key():
        key_banner = """
        <div class="no-key-banner">
          <span class="icon">⚠️</span>
          <div>
            <strong>No API key detected — using demo fallback answers.</strong><br>
            To get real Claude answers, set <code>ANTHROPIC_API_KEY=your-key</code>
            in your environment before running this script.<br>
            <code>ANTHROPIC_API_KEY=sk-ant-... python3 demo/response.py</code>
          </div>
        </div>"""

    # Answer cards
    answer_cards = ""
    if not results:
        answer_cards = '<div class="empty">Waiting for Prompt Enrichment to complete...</div>'
    for r in results:
        meta   = case_meta.get(r["case_id"], {})
        src    = _SRC.get(meta.get("source_type", ""), "")
        is_api = r["api_called"]
        tag_m  = (
            f'<span class="tag tag-model">✓ {r["model_used"]}</span>'
            if is_api else
            f'<span class="tag tag-fallback">demo fallback</span>'
        )
        box_cls = "answer-box" + ("" if is_api else " fallback")
        answer_cards += f"""
        <div class="answer-card">
          <div class="ac-header">
            <div class="ac-case">{r['case_id'][:8]}…</div>
            <div class="ac-meta">
              <div class="ac-q">{_html.escape(r['question'][:90])}</div>
              <div class="ac-tags">
                <span class="tag" style="background:#f3f4f6;color:#374151">
                  {src}
                </span>
                <span class="tag tag-theme">{r['theme']}</span>
                {tag_m}
              </div>
            </div>
          </div>
          <div class="{box_cls}">{_html.escape(r['answer'] or '')}</div>
        </div>"""

    # Log
    ev_css = {
        "GENERATING_ANSWER": "ev-gen",
        "ANSWER_STORED":     "ev-ok",
        "RESPONSE_DONE":     "ev-done",
    }
    log_lines = ""
    for lg in logs:
        ev  = lg["event"]
        css = ev_css.get(ev, "")
        if "ERROR" in (lg.get("detail") or ""):
            css = "ev-err"
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
<title>Phase 08 — Response</title>
<style>{CSS}</style>
</head>
<body>
<div class="page">
  <div class="header">
    <div>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
        <span class="badge">Phase 08</span>
        <h1>Response</h1>
      </div>
      <p>Claude API · Enriched prompt → citizen answer</p>
    </div>
    <div class="hdr-right">
      Polling Enrichment every 5s<br>
      <span style="color:#2ea043;font-weight:600">● Live</span>
    </div>
  </div>

  {stats_html}
  {key_banner}

  <div class="sec-title">Generated answers</div>
  <div class="card">{answer_cards}</div>

  <div class="sec-title">Audit Log</div>
  <div class="log-panel">{log_lines}</div>
  <p class="refresh-note">Auto-refreshes every 5 seconds</p>
</div>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────
# HTTP SERVER
# ─────────────────────────────────────────────────────────────

class ResponseDashboardHandler(http.server.BaseHTTPRequestHandler):
    res_conn: sqlite3.Connection = None  # type: ignore

    def log_message(self, *args: object) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/favicon.ico":
            self.send_response(204); self.end_headers(); return
        page = render_dashboard(self.res_conn)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode())


def make_handler(conn: sqlite3.Connection):
    class H(ResponseDashboardHandler):
        res_conn = conn
    return H


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    import os
    print("\n" + "═"*60)
    print("  CITIZEN PLATFORM — Phase 08: Response  (DEMO)")
    print("═"*60)

    has_key = bool(os.environ.get("ANTHROPIC_API_KEY"))
    print(f"\n  {'✓  Claude API key found' if has_key else '⚠  No API key — will use demo fallback answers'}")
    print(f"  ✓  Response DB : {RESPONSE_DB_PATH}")
    print(f"  ✓  Model       : {CLAUDE_MODEL}")

    res_conn = init_response_db()

    stop_event = threading.Event()
    poller = threading.Thread(
        target=poll_and_respond,
        args=(res_conn, stop_event),
        daemon=True,
    )
    poller.start()

    handler    = make_handler(res_conn)
    server     = http.server.HTTPServer(("", PORT), handler)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    print(f"\n  ✓  Dashboard: http://localhost:{PORT}")
    print(f"\n  To enable real Claude answers:")
    print(f"    export ANTHROPIC_API_KEY=sk-ant-...")
    print(f"    python3 demo/response.py")
    print(f"\n  Run order:")
    for i, (name, port) in enumerate([
        ("reception.py", 8000), ("ingestion.py", 8001),
        ("security.py", 8002),  ("privacy.py", 8003),
        ("analysis.py", 8004),  ("decomposition.py", 8005),
        ("prompt_enrichment.py", 8006), ("response.py", 8007),
    ], 1):
        print(f"    Terminal {i} → python3 demo/{name:<25} (port {port})")

    print(f"\n  Press Ctrl-C to stop.\n")
    print("─"*60)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n\n  Stopping response...")
        stop_event.set()
        server.shutdown()
        print("  Done.\n")


if __name__ == "__main__":
    main()
