"""
demo/recomposition.py
---------------------
Phase 10: Recomposition  (DEMO VERSION)

Assembles all atomic answers into a single coherent response document
ready for human review in Validation (Phase 11).

Two sub-steps:
  ① Merge answers
      - Salutation, case reference, answers for all IN_SCOPE questions
      - Sorry paragraph for each OUT_OF_SCOPE question with redirect
      - Professional closing statement
      - Quality-flagged answers are included but marked for validator attention

  ② Tone adaptation  (if enabled for the tenant)
      - Reads the citizen's mood from the case record
      - Reformulates the neutral draft with mood-appropriate language
      - NEUTRAL / FRUSTRATED → empathetic acknowledgement
      - ANGRY     → de-escalating, calming tone
      - DISTRESSED → warm, prioritised, urgent-feeling response
      - Factual content is NEVER changed — only phrasing and tone

In production:  LLM call for tone adaptation; S3 storage for both drafts.
In demo:        Rule-based tone reformulation; both drafts stored in SQLite.
                Tenant config: tone_adaptation_enabled = True (all cases).

Run:
    python3 demo/reception.py           ← port 8000
    ...
    python3 demo/quality.py             ← port 8008
    python3 demo/recomposition.py       ← port 8009

Dashboard: http://localhost:8009
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

DEMO_DIR            = Path(__file__).parent
INGESTION_DB_PATH   = DEMO_DIR / "demo_ingestion.db"
DECOMP_DB_PATH      = DEMO_DIR / "demo_decomposition.db"
RECOMP_DB_PATH      = DEMO_DIR / "demo_recomposition.db"
PORT                = 8009

# Tenant config (production: read from tenant_config table)
TONE_ADAPTATION_ENABLED = True


# ─────────────────────────────────────────────────────────────
# DOCUMENT TEMPLATES
# ─────────────────────────────────────────────────────────────

# Salutations per language
_SALUTATION: dict[str, str] = {
    "DE": "Sehr geehrte Damen und Herren",
    "FR": "Madame, Monsieur",
    "IT": "Gentile Signora, Gentile Signore",
    "RM": "Carа Dunna, Car Um",
    "EN": "Dear Sir or Madam",
}

# Closing per language
_CLOSING: dict[str, str] = {
    "DE": "Freundliche Grüsse\nBundesamt für Strassen ASTRA",
    "FR": "Avec nos meilleures salutations\nOffice fédéral des routes OFROU",
    "IT": "Cordiali saluti\nUfficio federale delle strade UST",
    "RM": "Cordials salids\nOfizi federal da vias UST",
    "EN": "Kind regards\nFederal Roads Office ASTRA",
}

# Sorry paragraph intro per language
_SORRY_INTRO: dict[str, str] = {
    "DE": "Zu Ihrer Frage betreffend «{topic}» müssen wir Sie leider darauf hinweisen, dass diese Angelegenheit nicht in den Zuständigkeitsbereich des ASTRA fällt.",
    "FR": "Concernant votre question relative à «{topic}», nous devons malheureusement vous informer que cette question ne relève pas de la compétence de l'OFROU.",
    "IT": "In merito alla sua domanda riguardante «{topic}», siamo purtroppo costretti ad informarla che questa questione non rientra nella competenza dell'UST.",
    "EN": "Regarding your question about «{topic}», we must inform you that this matter does not fall within ASTRA's area of responsibility.",
}

_SORRY_REDIRECT: dict[str, str] = {
    "DE": "Wir empfehlen Ihnen, sich an die zuständige Stelle zu wenden: {redirect}",
    "FR": "Nous vous recommandons de vous adresser à l'autorité compétente: {redirect}",
    "IT": "Le consigliamo di rivolgersi all'autorità competente: {redirect}",
    "EN": "We recommend contacting the responsible authority: {redirect}",
}

# Quality flag notice for validator
_QUALITY_FLAG_NOTICE: dict[str, str] = {
    "DE": "[HINWEIS FÜR PRÜFER: Diese Antwort wurde nach maximaler Qualitätsprüfung mit einer Markierung versehen. Bitte sorgfältig überprüfen.]",
    "FR": "[NOTE POUR LE VALIDATEUR: Cette réponse a été marquée après le maximum de contrôles qualité. Veuillez vérifier attentivement.]",
    "IT": "[NOTA PER IL VALIDATORE: Questa risposta è stata contrassegnata dopo il massimo dei controlli di qualità. Si prega di verificare attentamente.]",
    "EN": "[NOTE FOR VALIDATOR: This answer was flagged after maximum quality checks. Please review carefully.]",
}

# Reference line per language
_REF_LINE: dict[str, str] = {
    "DE": "Ihre Referenz: {ref}",
    "FR": "Votre référence: {ref}",
    "IT": "Il suo riferimento: {ref}",
    "RM": "Vossa referenza: {ref}",
    "EN": "Your reference: {ref}",
}


# ─────────────────────────────────────────────────────────────
# ① MERGE ANSWERS
# ─────────────────────────────────────────────────────────────

def merge_answers(
    case_id:    str,
    language:   str,
    questions:  list[dict[str, Any]],
) -> str:
    """
    Assemble all question answers into one structured neutral document.

    Structure:
      [Salutation]
      [Reference line]
      [Body: one paragraph per IN_SCOPE question]
      [Sorry paragraph per OUT_OF_SCOPE question]
      [Closing]
    """
    lang     = language.upper() if language.upper() in _SALUTATION else "DE"
    ref      = case_id[:8].upper()
    sal      = _SALUTATION[lang]
    closing  = _CLOSING[lang]
    ref_line = _REF_LINE.get(lang, _REF_LINE["DE"]).format(ref=ref)

    body_parts: list[str] = []

    in_scope  = [q for q in questions if q["scope"] == "IN_SCOPE"]
    out_scope = [q for q in questions if q["scope"] == "OUT_OF_SCOPE"]

    # IN_SCOPE answers
    for q in in_scope:
        answer = (q.get("answer") or "").strip()
        if not answer:
            continue

        # Remove any existing salutation in the answer (Response already adds one)
        answer = _strip_inner_salutation(answer, lang)

        if q.get("quality_flagged"):
            flag_notice = _QUALITY_FLAG_NOTICE.get(lang, _QUALITY_FLAG_NOTICE["DE"])
            body_parts.append(f"{flag_notice}\n\n{answer}")
        else:
            body_parts.append(answer)

    # OUT_OF_SCOPE sorry paragraphs
    for q in out_scope:
        topic    = _extract_topic(q["question"])
        redirect = q.get("redirect_info") or ""
        sorry    = _SORRY_INTRO.get(lang, _SORRY_INTRO["DE"]).format(topic=topic)
        if redirect:
            sorry += "\n" + _SORRY_REDIRECT.get(lang, _SORRY_REDIRECT["DE"]).format(
                redirect=redirect
            )
        body_parts.append(sorry)

    if not body_parts:
        # Edge case: no questions at all
        body_parts.append("Vielen Dank für Ihre Anfrage. Wir haben diese bearbeitet.")

    body = "\n\n---\n\n".join(body_parts)

    return f"{sal},\n\n{ref_line}\n\n{body}\n\n{closing}"


def _strip_inner_salutation(text: str, lang: str) -> str:
    """
    Remove repeated salutation lines that Response may have added.
    e.g. "Guten Tag\n\n..." → "..."
    """
    patterns = [
        r"^Guten Tag[\s,\n]+",
        r"^Sehr geehrte[rn]?\s+\w+[\s,\n]+",
        r"^Madame[,\s]*Monsieur[\s,\n]+",
        r"^Gentile\s+\w+[\s,\n]+",
        r"^Dear\s+\w[^,\n]*[\s,\n]+",
    ]
    for pat in patterns:
        text = re.sub(pat, "", text, flags=re.IGNORECASE).strip()
    return text


def _extract_topic(question: str) -> str:
    """Extract a short topic label from a question string."""
    # Strip anonymisation tokens and leading punctuation
    topic = re.sub(r"<[A-Z_]+_\d+>", "", question)
    topic = re.sub(r"^(FW:|RE:|AW:)\s*", "", topic, flags=re.IGNORECASE)
    topic = topic.strip().rstrip("?!.,")
    return topic[:60] if topic else "Ihre Anfrage"


# ─────────────────────────────────────────────────────────────
# ② TONE ADAPTATION
#
# In production: LLM call with instruction to rephrase without
#   changing facts. Both neutral and toned drafts stored in S3.
#
# In demo: rule-based prefix and suffix injection per mood.
#   The factual body is untouched — only wrapper text changes.
# ─────────────────────────────────────────────────────────────

# Mood-aware opening lines injected after the salutation
_TONE_OPENING: dict[str, dict[str, str]] = {
    "NEUTRAL": {
        "DE": "",   # no change — neutral is the default
        "FR": "",
        "IT": "",
        "EN": "",
    },
    "FRUSTRATED": {
        "DE": "Wir haben Ihre Anfrage erhalten und verstehen, dass Sie bisher keine Rückmeldung erhalten haben. Wir entschuldigen uns für die entstandenen Unannehmlichkeiten und bearbeiten Ihr Anliegen mit Priorität.\n\n",
        "FR": "Nous avons bien reçu votre demande et comprenons que vous n'avez pas encore reçu de réponse. Nous nous excusons pour ce désagrément et traitons votre demande en priorité.\n\n",
        "IT": "Abbiamo ricevuto la sua richiesta e comprendiamo che non ha ancora ricevuto risposta. Ci scusiamo per l'inconveniente e trattiamo la sua richiesta con priorità.\n\n",
        "EN": "We have received your request and understand that you have not yet received a response. We apologise for the inconvenience and are treating your matter as a priority.\n\n",
    },
    "ANGRY": {
        "DE": "Wir haben Ihre Nachricht erhalten und nehmen Ihr Anliegen sehr ernst. Wir möchten die Situation klären und Ihnen so schnell wie möglich eine vollständige Antwort geben.\n\n",
        "FR": "Nous avons bien reçu votre message et prenons votre préoccupation très au sérieux. Nous souhaitons clarifier la situation et vous donner une réponse complète dans les plus brefs délais.\n\n",
        "IT": "Abbiamo ricevuto il suo messaggio e prendiamo la sua preoccupazione molto sul serio. Vogliamo chiarire la situazione e fornirle una risposta completa il prima possibile.\n\n",
        "EN": "We have received your message and take your concern very seriously. We wish to clarify the situation and provide you with a complete response as soon as possible.\n\n",
    },
    "DISTRESSED": {
        "DE": "Wir haben Ihre dringende Anfrage erhalten und bearbeiten diese umgehend. Bitte wissen Sie, dass Ihr Anliegen für uns höchste Priorität hat.\n\n",
        "FR": "Nous avons reçu votre demande urgente et la traitons immédiatement. Sachez que votre demande est pour nous une priorité absolue.\n\n",
        "IT": "Abbiamo ricevuto la sua richiesta urgente e la stiamo elaborando immediatamente. Sappia che la sua richiesta ha per noi la massima priorità.\n\n",
        "EN": "We have received your urgent request and are processing it immediately. Please know that your matter is our absolute priority.\n\n",
    },
}


def apply_tone(neutral_doc: str, mood: str, language: str) -> str:
    """
    Apply mood-adaptive tone to the neutral document.

    The factual content is never changed.
    Only the opening acknowledgement paragraph is inserted/modified.
    Returns the tone-adapted document.
    """
    lang    = language.upper() if language.upper() in _SALUTATION else "DE"
    mood    = mood.upper() if mood else "NEUTRAL"
    opening = _TONE_OPENING.get(mood, _TONE_OPENING["NEUTRAL"]).get(lang, "")

    if not opening:
        return neutral_doc  # NEUTRAL → no change

    # Insert the mood opening right after the salutation + reference line
    # Pattern: find the first blank line after the ref line
    lines   = neutral_doc.split("\n")
    insert_after = 0
    found_ref = False
    for i, line in enumerate(lines):
        if re.search(r"(Ihre Referenz|Votre référence|riferimento|Your reference):", line):
            found_ref = True
        if found_ref and line.strip() == "":
            insert_after = i + 1
            break

    if insert_after > 0:
        lines.insert(insert_after, opening.rstrip())
        lines.insert(insert_after + 1, "")
    else:
        # Fallback: insert after second blank line
        doc_parts = neutral_doc.split("\n\n", 2)
        if len(doc_parts) >= 3:
            neutral_doc = doc_parts[0] + "\n\n" + doc_parts[1] + "\n\n" + opening + doc_parts[2]
            return neutral_doc

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────

def init_recomp_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(RECOMP_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS recomp_results (
            case_id           TEXT PRIMARY KEY,
            language          TEXT NOT NULL,
            mood              TEXT NOT NULL,
            neutral_draft     TEXT NOT NULL,
            toned_draft       TEXT,
            tone_applied      INTEGER NOT NULL DEFAULT 0,
            question_count    INTEGER NOT NULL DEFAULT 0,
            in_scope_count    INTEGER NOT NULL DEFAULT 0,
            out_scope_count   INTEGER NOT NULL DEFAULT 0,
            flagged_count     INTEGER NOT NULL DEFAULT 0,
            composed_at       TEXT NOT NULL,
            pipeline_step     TEXT NOT NULL DEFAULT 'RECOMPOSITION_DONE'
        );

        CREATE TABLE IF NOT EXISTS recomp_log (
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


def already_composed(rec_conn: sqlite3.Connection, case_id: str) -> bool:
    return rec_conn.execute(
        "SELECT 1 FROM recomp_results WHERE case_id=?", (case_id,)
    ).fetchone() is not None


def log_event(rec_conn: sqlite3.Connection,
              case_id: str, event: str, detail: str = "") -> None:
    rec_conn.execute(
        "INSERT INTO recomp_log VALUES (?,?,?,?,?)",
        (str(uuid.uuid4()), case_id, event, detail,
         datetime.now(timezone.utc).isoformat()),
    )
    rec_conn.commit()


# ─────────────────────────────────────────────────────────────
# CORE RECOMPOSITION LOGIC
# ─────────────────────────────────────────────────────────────

def run_recomposition(
    case:      dict[str, Any],
    questions: list[dict[str, Any]],
    rec_conn:  sqlite3.Connection,
) -> dict[str, Any]:
    case_id  = case["case_id"]
    language = case.get("language", "DE")
    mood     = case.get("mood", "NEUTRAL") or "NEUTRAL"

    in_scope  = [q for q in questions if q["scope"] == "IN_SCOPE"]
    out_scope = [q for q in questions if q["scope"] == "OUT_OF_SCOPE"]
    flagged   = [q for q in questions if q.get("quality_flagged")]

    # ── ① Merge answers ───────────────────────────────────────
    neutral_draft = merge_answers(case_id, language, questions)
    log_event(rec_conn, case_id, "NEUTRAL_DRAFT_CREATED",
              f"chars={len(neutral_draft)} questions={len(questions)}")

    # ── ② Tone adaptation ─────────────────────────────────────
    toned_draft  = None
    tone_applied = False

    if TONE_ADAPTATION_ENABLED and mood != "NEUTRAL":
        toned_draft  = apply_tone(neutral_draft, mood, language)
        tone_applied = True
        log_event(rec_conn, case_id, "TONE_APPLIED",
                  f"mood={mood} lang={language}")
    else:
        log_event(rec_conn, case_id, "TONE_SKIPPED",
                  f"mood={mood} enabled={TONE_ADAPTATION_ENABLED}")

    # ── Save result ───────────────────────────────────────────
    result = {
        "case_id":         case_id,
        "language":        language,
        "mood":            mood,
        "neutral_draft":   neutral_draft,
        "toned_draft":     toned_draft,
        "tone_applied":    1 if tone_applied else 0,
        "question_count":  len(questions),
        "in_scope_count":  len(in_scope),
        "out_scope_count": len(out_scope),
        "flagged_count":   len(flagged),
        "composed_at":     datetime.now(timezone.utc).isoformat(),
        "pipeline_step":   "RECOMPOSITION_DONE",
    }
    rec_conn.execute("""
        INSERT INTO recomp_results VALUES (
            :case_id, :language, :mood,
            :neutral_draft, :toned_draft, :tone_applied,
            :question_count, :in_scope_count, :out_scope_count,
            :flagged_count, :composed_at, :pipeline_step
        )
    """, result)
    rec_conn.commit()

    # ── Advance pipeline step ──────────────────────────────────
    try:
        ing = sqlite3.connect(str(INGESTION_DB_PATH),
                              check_same_thread=False)
        ing.execute(
            "UPDATE cases SET pipeline_step='RECOMPOSITION_DONE' WHERE case_id=?",
            (case_id,),
        )
        ing.commit()
        ing.close()
    except Exception:
        pass

    log_event(rec_conn, case_id, "RECOMPOSITION_DONE",
              f"in={len(in_scope)} out={len(out_scope)} "
              f"flagged={len(flagged)} tone={mood}")
    return result


# ─────────────────────────────────────────────────────────────
# POLLING LOOP
# ─────────────────────────────────────────────────────────────

def poll_and_recompose(rec_conn:   sqlite3.Connection,
                       stop_event: threading.Event) -> None:
    print("  [Recomposition] polling Quality DB every 5 seconds...")

    while not stop_event.is_set():
        ing_db = open_db_ro(INGESTION_DB_PATH)
        if not ing_db:
            print("  [Recomposition] waiting for upstream DBs...")
            stop_event.wait(5)
            continue

        dec_db = open_db_ro(DECOMP_DB_PATH)

        try:
            pending = ing_db.execute(
                "SELECT * FROM cases WHERE pipeline_step='QUALITY_DONE'"
            ).fetchall()
        finally:
            ing_db.close()

        new_count = 0
        for row in pending:
            case = dict(row)
            if already_composed(rec_conn, case["case_id"]):
                continue

            if not dec_db:
                continue

            qs = [dict(r) for r in dec_db.execute(
                "SELECT * FROM questions WHERE case_id=?",
                (case["case_id"],),
            ).fetchall()]

            result = run_recomposition(case, qs, rec_conn)
            new_count += 1
            _print_result(case, result)

        if new_count:
            print(f"\n  [Recomposition] ✓ {new_count} case(s) composed.\n")

        try:
            if dec_db: dec_db.close()
        except Exception:
            pass

        stop_event.wait(5)


# ─────────────────────────────────────────────────────────────
# TERMINAL PRINTER
# ─────────────────────────────────────────────────────────────

def _print_result(case: dict, result: dict) -> None:
    tone_icon = "🎨" if result["tone_applied"] else "📄"
    print(f"\n{'─'*60}")
    print(f"  {tone_icon}  Recomposition complete")
    print(f"  Case ID  : {result['case_id']}")
    print(f"  Language : {result['language']}  Mood: {result['mood']}")
    print(f"  Tone     : {'applied (' + result['mood'] + ')' if result['tone_applied'] else 'not applied (NEUTRAL)'}")
    print(f"  Questions: {result['in_scope_count']} in-scope  "
          f"{result['out_scope_count']} out-of-scope  "
          f"{result['flagged_count']} flagged")
    print(f"\n  Neutral draft ({len(result['neutral_draft'])} chars):")
    for line in result["neutral_draft"].split("\n")[:8]:
        print(f"    {line}")
    if result["toned_draft"] and result["toned_draft"] != result["neutral_draft"]:
        print(f"\n  Toned draft ({len(result['toned_draft'])} chars):")
        for line in result["toned_draft"].split("\n")[:8]:
            print(f"    {line}")
    print(f"\n  → Next step: Phase 11 Validation")
    print(json.dumps({
        "case_id":     result["case_id"],
        "tenant_id":   case["tenant_id"],
        "tone_applied": bool(result["tone_applied"]),
        "flagged":     result["flagged_count"] > 0,
        "step":        "VALIDATION",
    }, indent=4))


# ─────────────────────────────────────────────────────────────
# WEB DASHBOARD  (port 8009)
# ─────────────────────────────────────────────────────────────

CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#f0f2f5;min-height:100vh;padding:32px 20px}
.page{max-width:980px;margin:0 auto}
.header{background:#1a2332;color:white;border-radius:8px;
        padding:20px 28px;margin-bottom:24px;
        display:flex;align-items:center;gap:16px}
.badge{background:#0891b2;color:white;font-size:10px;font-weight:700;
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

/* Case composition card */
.comp-card{padding:0;border-bottom:1px solid #f3f4f6}
.comp-card:last-child{border-bottom:none}
.comp-header{display:grid;grid-template-columns:1fr auto;align-items:center;
             gap:12px;padding:14px 20px;background:#f8f9fb;
             border-bottom:1px solid #e5e7eb}
.comp-meta{font-size:12px;color:#374151}
.comp-meta strong{font-size:14px;font-weight:600;display:block;margin-bottom:2px}
.comp-tags{display:flex;gap:6px;flex-wrap:wrap}
.tag{font-size:10px;font-weight:600;padding:2px 7px;border-radius:3px}
.tag-lang{background:#e0f2fe;color:#0369a1}
.tag-tone{background:#fef3c7;color:#92400e}
.tag-neutral{background:#f0fdf4;color:#166534}
.tag-mood-neutral   {background:#f3f4f6;color:#374151}
.tag-mood-frustrated{background:#fef3c7;color:#92400e}
.tag-mood-angry     {background:#fee2e2;color:#991b1b}
.tag-mood-distressed{background:#ede9fe;color:#4c1d95}

/* Draft tabs */
.draft-tabs{display:flex;border-bottom:1px solid #e5e7eb}
.draft-tab{padding:8px 16px;font-size:11px;font-weight:600;cursor:pointer;
           border-bottom:2px solid transparent;color:#6b7280;
           transition:all .15s;white-space:nowrap}
.draft-tab.active{color:#0891b2;border-bottom-color:#0891b2}
.draft-content{padding:16px 20px}

/* Document viewer */
.doc-viewer{background:#fffef7;border:1px solid #e5e7eb;border-radius:6px;
            padding:20px 24px;font-size:13px;line-height:1.9;color:#1e293b;
            white-space:pre-wrap;word-break:break-word;
            font-family:'Georgia','Times New Roman',serif;
            min-height:120px}
.doc-viewer .doc-flag{background:#fef3c7;color:#92400e;
                      font-family:monospace;font-size:11px;
                      padding:2px 6px;border-radius:3px}
.doc-viewer .doc-sorry{color:#6b7280;font-style:italic}

/* Tone diff highlight */
.tone-addition{background:#d1fae5;padding:0 2px;border-radius:2px}

.log-panel{background:#1a2332;border-radius:7px;
           padding:16px 20px;max-height:200px;overflow-y:auto}
.log-line{font-family:'Menlo',monospace;font-size:11px;
          color:#8b949e;padding:2px 0;line-height:1.6}
.ts{color:#484f58}
.ev-neutral{color:#388bfd}
.ev-tone   {color:#d29922}
.ev-done   {color:#2ea043}

.refresh-note{font-size:11px;color:#9ca3af;text-align:center;margin-top:8px}
.empty{text-align:center;color:#9ca3af;padding:24px;font-size:13px}
"""

_MOOD_TAG = {
    "NEUTRAL":    "tag-mood-neutral",
    "FRUSTRATED": "tag-mood-frustrated",
    "ANGRY":      "tag-mood-angry",
    "DISTRESSED": "tag-mood-distressed",
}
_MOOD_ICON = {
    "NEUTRAL":    "😐",
    "FRUSTRATED": "😤",
    "ANGRY":      "😠",
    "DISTRESSED": "😰",
}
_SRC = {"DIRECT_EMAIL":"Email","STAFF_FORWARD":"Staff",
        "POSTAL_SCAN":"Scan","WEB_FORM":"Web Form"}


def _format_doc(text: str) -> str:
    """Format document text for safe HTML display."""
    text = _html.escape(text)
    # Highlight quality flag notices
    text = re.sub(
        r"(\[HINWEIS FÜR PRÜFER:[^\]]+\]|\[NOTE FOR VALIDATOR:[^\]]+\]"
        r"|\[NOTE POUR LE VALIDATEUR:[^\]]+\]|\[NOTA PER IL VALIDATORE:[^\]]+\])",
        r'<span class="doc-flag">\1</span>',
        text,
    )
    return text


def render_dashboard(rec_conn: sqlite3.Connection) -> str:
    results = rec_conn.execute(
        "SELECT * FROM recomp_results ORDER BY composed_at DESC"
    ).fetchall()
    results = [dict(r) for r in results]

    logs = rec_conn.execute(
        "SELECT * FROM recomp_log ORDER BY ts DESC LIMIT 80"
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
    total     = len(results)
    toned     = sum(r["tone_applied"] for r in results)
    flagged   = sum(1 for r in results if r["flagged_count"] > 0)
    out_scope = sum(r["out_scope_count"] for r in results)

    stats_html = f"""<div class="stats">
      <div class="stat"><div class="stat-num">{total}</div>
        <div class="stat-label">Composed</div></div>
      <div class="stat"><div class="stat-num" style="color:#d97706">{toned}</div>
        <div class="stat-label">🎨 Tone Applied</div></div>
      <div class="stat"><div class="stat-num" style="color:#dc2626">{out_scope}</div>
        <div class="stat-label">🚫 Out-of-Scope</div></div>
      <div class="stat"><div class="stat-num" style="color:#92400e">{flagged}</div>
        <div class="stat-label">⚑ Flagged</div></div>
    </div>"""

    # Composition cards
    comp_cards = ""
    if not results:
        comp_cards = '<div class="empty">Waiting for Quality to complete...</div>'

    for r in results:
        meta     = case_meta.get(r["case_id"], {})
        src      = _SRC.get(meta.get("source_type",""), "")
        mood     = r["mood"]
        mood_tag = _MOOD_TAG.get(mood, "tag-mood-neutral")
        mood_icon= _MOOD_ICON.get(mood, "")
        tone_tag = (
            '<span class="tag tag-tone">🎨 tone applied</span>'
            if r["tone_applied"]
            else '<span class="tag tag-neutral">📄 neutral</span>'
        )
        flag_tag = (
            f'<span class="tag" style="background:#fef3c7;color:#92400e">'
            f'⚑ {r["flagged_count"]} flagged</span>'
            if r["flagged_count"] else ""
        )
        out_tag = (
            f'<span class="tag" style="background:#fee2e2;color:#991b1b">'
            f'🚫 {r["out_scope_count"]} out-of-scope</span>'
            if r["out_scope_count"] else ""
        )

        # Build draft panels
        neutral_html = f'<div class="doc-viewer">{_format_doc(r["neutral_draft"])}</div>'
        toned_html   = ""
        tab_html     = ""

        if r["tone_applied"] and r["toned_draft"]:
            tab_html = """
            <div class="draft-tabs">
              <div class="draft-tab active" onclick="showTab(this,'neutral')">
                📄 Neutral draft
              </div>
              <div class="draft-tab" onclick="showTab(this,'toned')">
                🎨 Tone-adapted draft
              </div>
            </div>"""
            toned_html = (
                f'<div class="doc-viewer" style="display:none">'
                f'{_format_doc(r["toned_draft"])}</div>'
            )
            draft_section = f'{tab_html}<div class="draft-content">{neutral_html}{toned_html}</div>'
        else:
            draft_section = f'<div class="draft-content">{neutral_html}</div>'

        subj = _html.escape((meta.get("subject") or "")[:50])
        comp_cards += f"""
        <div class="comp-card">
          <div class="comp-header">
            <div class="comp-meta">
              <strong>{src} — {r['case_id'][:8]}…
                <span style="font-size:11px;color:#9ca3af;font-weight:400">
                  &nbsp;{subj}
                </span>
              </strong>
              <div class="comp-tags">
                <span class="tag tag-lang">{r['language']}</span>
                <span class="tag {mood_tag}">{mood_icon} {mood}</span>
                {tone_tag}{flag_tag}{out_tag}
              </div>
            </div>
            <div style="font-size:11px;color:#9ca3af;text-align:right">
              {r['in_scope_count']} questions<br>
              {len(r['neutral_draft'])} chars
            </div>
          </div>
          {draft_section}
        </div>"""

    # Log
    ev_css = {
        "NEUTRAL_DRAFT_CREATED": "ev-neutral",
        "TONE_APPLIED":          "ev-tone",
        "TONE_SKIPPED":          "ev-neutral",
        "RECOMPOSITION_DONE":    "ev-done",
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
<title>Phase 10 — Recomposition</title>
<style>{CSS}</style>
</head>
<body>
<div class="page">
  <div class="header">
    <div>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
        <span class="badge">Phase 10</span>
        <h1>Recomposition</h1>
      </div>
      <p>Merge answers · Tone adaptation · Ready for Validation</p>
    </div>
    <div class="hdr-right">
      Polling Quality every 5s<br>
      <span style="color:#2ea043;font-weight:600">● Live</span>
    </div>
  </div>

  {stats_html}

  <div class="sec-title">Composed documents</div>
  <div class="card">{comp_cards}</div>

  <div class="sec-title">Audit Log</div>
  <div class="log-panel">{log_lines}</div>
  <p class="refresh-note">Auto-refreshes every 5 seconds</p>
</div>

<script>
function showTab(btn, which) {{
  var card = btn.closest('.comp-card');
  card.querySelectorAll('.draft-tab').forEach(function(t){{
    t.classList.remove('active');
  }});
  btn.classList.add('active');
  var viewers = card.querySelectorAll('.doc-viewer');
  viewers[0].style.display = which === 'neutral' ? '' : 'none';
  if (viewers[1]) viewers[1].style.display = which === 'toned' ? '' : 'none';
}}
</script>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────
# HTTP SERVER
# ─────────────────────────────────────────────────────────────

class RecompDashboardHandler(http.server.BaseHTTPRequestHandler):
    rec_conn: sqlite3.Connection = None  # type: ignore

    def log_message(self, *args: object) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/favicon.ico":
            self.send_response(204); self.end_headers(); return
        page = render_dashboard(self.rec_conn)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode())


def make_handler(conn: sqlite3.Connection):
    class H(RecompDashboardHandler):
        rec_conn = conn
    return H


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "═"*60)
    print("  CITIZEN PLATFORM — Phase 10: Recomposition  (DEMO)")
    print("═"*60)

    rec_conn = init_recomp_db()
    print(f"\n  ✓  Recomposition DB  : {RECOMP_DB_PATH}")
    print(f"  ✓  Tone adaptation   : {'enabled' if TONE_ADAPTATION_ENABLED else 'disabled'}")
    print(f"  ✓  Reading from      : Quality DB + Decomposition DB")

    stop_event = threading.Event()
    poller = threading.Thread(
        target=poll_and_recompose,
        args=(rec_conn, stop_event),
        daemon=True,
    )
    poller.start()

    handler    = make_handler(rec_conn)
    server     = http.server.HTTPServer(("", PORT), handler)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    print(f"\n  ✓  Dashboard: http://localhost:{PORT}")
    print(f"\n  Run order:")
    for i, (name, port) in enumerate([
        ("reception.py",8000),("ingestion.py",8001),
        ("security.py",8002),("privacy.py",8003),
        ("analysis.py",8004),("decomposition.py",8005),
        ("prompt_enrichment.py",8006),("response.py",8007),
        ("quality.py",8008),("recomposition.py",8009),
    ], 1):
        print(f"    Terminal {i:2} → python3 demo/{name:<26} (port {port})")
    print(f"\n  Press Ctrl-C to stop.\n")
    print("─"*60)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n\n  Stopping recomposition...")
        stop_event.set()
        server.shutdown()
        print("  Done.\n")


if __name__ == "__main__":
    main()
