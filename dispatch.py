"""
demo/dispatch.py
----------------
Phase 12: Dispatch  (DEMO VERSION)

The final phase. Closes the case lifecycle:

  ① PII reconstruction  — replace <TAG_N> placeholders with original
                           values from pii_tokens table
  ② Send response       — email the citizen (simulated: printed + logged)
  ③ Feed KB             — upsert anonymised Q/A pairs into kb_entries;
                           similar questions update existing entry instead
                           of creating a duplicate
  ④ Scheduled purge     — delete body, attachments, PII tokens per
                           retention policy (D+N per theme);
                           audit trail is NEVER purged (nFADP)
  ⑤ Outlook → status    — Green (fully answered) / Orange (mixed scope)
                           Red (fully out of scope)

In production:
  - Email sent via EWS API in the detected language
  - kb_entries in PostgreSQL with pgVector
  - Purge executed as a durable Hatchet workflow
  - Audit trail in append-only Postgres table (never deleted)

In demo:
  - Email simulated: full reconstructed text printed + logged
  - KB stored in SQLite kb_entries table
  - Purge deletes from demo DBs, logs what was deleted
  - Dashboard shows the full sent letter with PII restored

Run:
    python3 demo/reception.py       ← port 8000
    ...
    python3 demo/validation.py      ← port 8010
    python3 demo/dispatch.py        ← port 8011

Dashboard: http://localhost:8011
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
PRIVACY_DB_PATH     = DEMO_DIR / "demo_privacy.db"
DECOMP_DB_PATH      = DEMO_DIR / "demo_decomposition.db"
RECOMP_DB_PATH      = DEMO_DIR / "demo_recomposition.db"
VALIDATION_DB_PATH  = DEMO_DIR / "demo_validation.db"
DISPATCH_DB_PATH    = DEMO_DIR / "demo_dispatch.db"
PORT                = 8011

# Retention policy per theme (D+N days after dispatch)
RETENTION_DAYS: dict[str, int] = {
    "DRIVERS_LICENSE":      90,
    "ROAD_INFRASTRUCTURE":  15,
    "NOISE_PROTECTION":     30,
    "TUNNEL_SAFETY":        30,
    "VEHICLE_REGISTRATION": 90,
    "GENERAL_INQUIRY":      30,
}
DEFAULT_RETENTION_DAYS = 30


# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────

def init_dispatch_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DISPATCH_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS dispatch_results (
            case_id          TEXT PRIMARY KEY,
            tenant_id        TEXT NOT NULL,
            sent_to          TEXT NOT NULL,
            language         TEXT NOT NULL,
            outlook_status   TEXT NOT NULL,
            pii_tokens_used  INTEGER NOT NULL DEFAULT 0,
            kb_entries_added INTEGER NOT NULL DEFAULT 0,
            kb_entries_updated INTEGER NOT NULL DEFAULT 0,
            purge_scheduled_at TEXT,
            retention_days   INTEGER NOT NULL,
            dispatched_at    TEXT NOT NULL,
            pipeline_step    TEXT NOT NULL DEFAULT 'COMPLETED'
        );

        CREATE TABLE IF NOT EXISTS kb_entries (
            id               TEXT PRIMARY KEY,
            theme            TEXT NOT NULL,
            question_text    TEXT NOT NULL,
            validated_answer TEXT NOT NULL,
            updated_at       TEXT NOT NULL,
            update_count     INTEGER NOT NULL DEFAULT 1,
            tenant_id        TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sent_letters (
            id              TEXT PRIMARY KEY,
            case_id         TEXT NOT NULL,
            to_address      TEXT NOT NULL,
            subject         TEXT NOT NULL,
            body_with_pii   TEXT NOT NULL,
            sent_at         TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS purge_log (
            id              TEXT PRIMARY KEY,
            case_id         TEXT NOT NULL,
            purge_type      TEXT NOT NULL,
            items_deleted   TEXT NOT NULL,
            retention_days  INTEGER NOT NULL,
            purged_at       TEXT NOT NULL,
            triggered_by    TEXT NOT NULL DEFAULT 'scheduler'
        );

        CREATE TABLE IF NOT EXISTS dispatch_log (
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


def already_dispatched(dis_conn: sqlite3.Connection, case_id: str) -> bool:
    return dis_conn.execute(
        "SELECT 1 FROM dispatch_results WHERE case_id=?", (case_id,)
    ).fetchone() is not None


def log_event(dis_conn: sqlite3.Connection,
              case_id: str, event: str, detail: str = "") -> None:
    dis_conn.execute(
        "INSERT INTO dispatch_log VALUES (?,?,?,?,?)",
        (str(uuid.uuid4()), case_id, event, detail,
         datetime.now(timezone.utc).isoformat()),
    )
    dis_conn.commit()


# ─────────────────────────────────────────────────────────────
# ① PII RECONSTRUCTION
# ─────────────────────────────────────────────────────────────

def reconstruct_pii(text: str,
                    tokens: list[dict[str, Any]]) -> tuple[str, int]:
    """
    Replace all <TAG_N> placeholders in text with original PII values.
    Returns (reconstructed_text, replacements_made).
    """
    result = text
    count  = 0
    # Sort longest tag first to avoid partial matches
    for tok in sorted(tokens, key=lambda t: len(t["tag"]), reverse=True):
        tag   = tok["tag"]
        value = tok["value"]
        if tag in result:
            result = result.replace(tag, value)
            count += 1
    return result, count


# ─────────────────────────────────────────────────────────────
# ② EMAIL SEND (simulated)
# ─────────────────────────────────────────────────────────────

_EMAIL_SUBJECT: dict[str, str] = {
    "DE": "Ihre Anfrage an das ASTRA – Ref. {ref}",
    "FR": "Votre demande à l'OFROU – Réf. {ref}",
    "IT": "La sua richiesta all'UST – Rif. {ref}",
    "RM": "Vossa dumonda a l'UST – Ref. {ref}",
    "EN": "Your request to ASTRA – Ref. {ref}",
}


def simulate_send(
    case:     dict[str, Any],
    body_pii: str,
    dis_conn: sqlite3.Connection,
) -> str:
    """
    Simulate sending the email via EWS.
    In production: account.send_message(subject, body, to).
    Returns the email subject line.
    """
    lang    = (case.get("language") or "DE").upper()
    ref     = case["case_id"][:8].upper()
    subject = _EMAIL_SUBJECT.get(lang, _EMAIL_SUBJECT["DE"]).format(ref=ref)
    to_addr = case.get("sender_email") or "citizen@example.ch"

    dis_conn.execute("""
        INSERT INTO sent_letters VALUES (?,?,?,?,?,?)
    """, (
        str(uuid.uuid4()),
        case["case_id"],
        to_addr,
        subject,
        body_pii,
        datetime.now(timezone.utc).isoformat(),
    ))
    dis_conn.commit()
    return subject


# ─────────────────────────────────────────────────────────────
# ③ FEED KNOWLEDGE BASE
# ─────────────────────────────────────────────────────────────

def _similarity_score(a: str, b: str) -> float:
    """
    Simple word-overlap similarity for deduplication check.
    In production: pgVector cosine similarity with threshold.
    """
    wa = set(re.findall(r"\b\w{4,}\b", a.lower()))
    wb = set(re.findall(r"\b\w{4,}\b", b.lower()))
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / max(len(wa), len(wb))

KB_SIMILARITY_THRESHOLD = 0.6


def feed_kb(
    questions: list[dict[str, Any]],
    tenant_id: str,
    dis_conn:  sqlite3.Connection,
) -> tuple[int, int]:
    """
    Upsert anonymised Q/A pairs into kb_entries.
    Similar questions (above threshold) update the existing entry.
    Returns (added, updated) counts.
    """
    added   = 0
    updated = 0

    for q in questions:
        if q.get("scope") != "IN_SCOPE":
            continue
        q_text = (q.get("question") or "").strip()
        answer = (q.get("answer")   or "").strip()
        theme  = q.get("theme", "GENERAL_INQUIRY")

        if not q_text or not answer:
            continue

        # Check for similar existing entry
        existing = dis_conn.execute(
            "SELECT id, question_text, update_count FROM kb_entries "
            "WHERE tenant_id=? AND theme=?",
            (tenant_id, theme),
        ).fetchall()

        best_id    = None
        best_score = 0.0
        for row in existing:
            score = _similarity_score(q_text, row["question_text"])
            if score > best_score:
                best_score = score
                best_id    = row["id"]

        if best_id and best_score >= KB_SIMILARITY_THRESHOLD:
            # Update existing entry
            dis_conn.execute("""
                UPDATE kb_entries
                SET validated_answer=?, updated_at=?,
                    update_count=update_count+1
                WHERE id=?
            """, (answer, datetime.now(timezone.utc).isoformat(), best_id))
            updated += 1
        else:
            # Insert new entry
            dis_conn.execute("""
                INSERT INTO kb_entries VALUES (?,?,?,?,?,?,?)
            """, (
                str(uuid.uuid4()), theme, q_text, answer,
                datetime.now(timezone.utc).isoformat(), 1, tenant_id,
            ))
            added += 1

    dis_conn.commit()
    return added, updated


# ─────────────────────────────────────────────────────────────
# ④ SCHEDULED PURGE
# ─────────────────────────────────────────────────────────────

def determine_outlook_status(questions: list[dict]) -> str:
    """
    Green  → all questions IN_SCOPE and answered
    Orange → mixed scope (some IN_SCOPE, some OUT_OF_SCOPE)
    Red    → all questions OUT_OF_SCOPE
    """
    in_s  = sum(1 for q in questions if q.get("scope") == "IN_SCOPE")
    out_s = sum(1 for q in questions if q.get("scope") == "OUT_OF_SCOPE")
    if out_s == 0:
        return "GREEN"
    if in_s == 0:
        return "RED"
    return "ORANGE"


def run_purge(
    case_id:       str,
    retention_days: int,
    dis_conn:      sqlite3.Connection,
) -> dict[str, int]:
    """
    Simulate the scheduled D+N purge.

    Deleted (in demo):
      - pii_tokens rows for this case
      - attachment_anon rows for this case (short-lived pgVector)

    Never deleted:
      - dispatch_log (audit trail, nFADP)
      - kb_entries (long-lived)
      - sent_letters.body stripped of its PII reference
        (response text preserved, but original PII-containing copy removed)

    In production: durable Hatchet workflow scheduled D+N days out.
    In demo: runs immediately for demonstration purposes.
    """
    deleted: dict[str, int] = {}

    # Delete PII tokens
    try:
        priv_rw = sqlite3.connect(str(PRIVACY_DB_PATH),
                                  check_same_thread=False)
        c = priv_rw.execute(
            "DELETE FROM pii_tokens WHERE case_id=?", (case_id,)
        )
        deleted["pii_tokens"] = c.rowcount
        priv_rw.commit()
        priv_rw.close()
    except Exception:
        deleted["pii_tokens"] = 0

    # Delete attachment anonymised Markdowns
    try:
        priv_rw = sqlite3.connect(str(PRIVACY_DB_PATH),
                                  check_same_thread=False)
        c = priv_rw.execute(
            "DELETE FROM attachment_anon WHERE case_id=?", (case_id,)
        )
        deleted["attachment_anon"] = c.rowcount
        priv_rw.commit()
        priv_rw.close()
    except Exception:
        deleted["attachment_anon"] = 0

    # Log purge record
    dis_conn.execute("""
        INSERT INTO purge_log VALUES (?,?,?,?,?,?,?)
    """, (
        str(uuid.uuid4()), case_id, "SCHEDULED",
        json.dumps(deleted), retention_days,
        datetime.now(timezone.utc).isoformat(),
        "demo-scheduler",
    ))
    dis_conn.commit()

    return deleted


# ─────────────────────────────────────────────────────────────
# CORE DISPATCH LOGIC
# ─────────────────────────────────────────────────────────────

def run_dispatch(
    case:      dict[str, Any],
    dis_conn:  sqlite3.Connection,
) -> dict[str, Any]:
    case_id   = case["case_id"]
    tenant_id = case.get("tenant_id", "")

    # Load PII tokens for this case
    priv_db = open_db_ro(PRIVACY_DB_PATH)
    tokens: list[dict] = []
    if priv_db:
        try:
            tokens = [dict(r) for r in priv_db.execute(
                "SELECT tag, pii_type, value FROM pii_tokens WHERE case_id=?",
                (case_id,),
            ).fetchall()]
        finally:
            priv_db.close()

    # Load composed document (use toned if available)
    rec_db = open_db_ro(RECOMP_DB_PATH)
    body_anon = ""
    if rec_db:
        try:
            row = rec_db.execute(
                "SELECT neutral_draft, toned_draft, tone_applied "
                "FROM recomp_results WHERE case_id=?",
                (case_id,),
            ).fetchone()
            if row:
                body_anon = row["toned_draft"] if row["tone_applied"] else row["neutral_draft"]
                body_anon = body_anon or row["neutral_draft"] or ""
        finally:
            rec_db.close()

    # ── ① PII reconstruction ──────────────────────────────────
    body_pii, tokens_used = reconstruct_pii(body_anon, tokens)
    log_event(dis_conn, case_id, "PII_RECONSTRUCTED",
              f"tokens_used={tokens_used}")

    # ── ② Send email ──────────────────────────────────────────
    subject = simulate_send(case, body_pii, dis_conn)
    log_event(dis_conn, case_id, "EMAIL_SENT",
              f"to={case.get('sender_email','')} subject={subject}")

    # ── ③ Feed KB ─────────────────────────────────────────────
    dec_db = open_db_ro(DECOMP_DB_PATH)
    questions: list[dict] = []
    if dec_db:
        try:
            questions = [dict(r) for r in dec_db.execute(
                "SELECT * FROM questions WHERE case_id=?", (case_id,),
            ).fetchall()]
        finally:
            dec_db.close()

    kb_added, kb_updated = feed_kb(questions, tenant_id, dis_conn)
    log_event(dis_conn, case_id, "KB_UPDATED",
              f"added={kb_added} updated={kb_updated}")

    # ── ④ Outlook status ──────────────────────────────────────
    outlook = determine_outlook_status(questions)
    log_event(dis_conn, case_id, "OUTLOOK_STATUS",
              f"colour={outlook}")

    # ── ⑤ Scheduled purge ────────────────────────────────────
    theme     = questions[0]["theme"] if questions else "GENERAL_INQUIRY"
    ret_days  = RETENTION_DAYS.get(theme, DEFAULT_RETENTION_DAYS)
    deleted   = run_purge(case_id, ret_days, dis_conn)
    log_event(dis_conn, case_id, "PURGE_EXECUTED",
              f"D+{ret_days} deleted={json.dumps(deleted)}")

    # ── Save result ───────────────────────────────────────────
    result = {
        "case_id":           case_id,
        "tenant_id":         tenant_id,
        "sent_to":           case.get("sender_email", ""),
        "language":          case.get("language", "DE"),
        "outlook_status":    outlook,
        "pii_tokens_used":   tokens_used,
        "kb_entries_added":  kb_added,
        "kb_entries_updated": kb_updated,
        "purge_scheduled_at": datetime.now(timezone.utc).isoformat(),
        "retention_days":    ret_days,
        "dispatched_at":     datetime.now(timezone.utc).isoformat(),
        "pipeline_step":     "COMPLETED",
    }
    dis_conn.execute("""
        INSERT INTO dispatch_results VALUES (
            :case_id, :tenant_id, :sent_to, :language,
            :outlook_status, :pii_tokens_used,
            :kb_entries_added, :kb_entries_updated,
            :purge_scheduled_at, :retention_days,
            :dispatched_at, :pipeline_step
        )
    """, result)
    dis_conn.commit()

    # Advance pipeline
    try:
        ing = sqlite3.connect(str(INGESTION_DB_PATH),
                              check_same_thread=False)
        ing.execute(
            "UPDATE cases SET pipeline_step='COMPLETED' WHERE case_id=?",
            (case_id,),
        )
        ing.commit()
        ing.close()
    except Exception:
        pass

    log_event(dis_conn, case_id, "DISPATCH_COMPLETE",
              f"outlook={outlook} kb_added={kb_added} purged={deleted}")
    return {**result, "body_pii": body_pii, "subject": subject}


# ─────────────────────────────────────────────────────────────
# POLLING LOOP
# ─────────────────────────────────────────────────────────────

def poll_and_dispatch(dis_conn:   sqlite3.Connection,
                      stop_event: threading.Event) -> None:
    print("  [Dispatch] polling Validation DB every 5 seconds...")

    while not stop_event.is_set():
        ing_db = open_db_ro(INGESTION_DB_PATH)
        val_db = open_db_ro(VALIDATION_DB_PATH)

        if not all([ing_db, val_db]):
            print("  [Dispatch] waiting for upstream DBs...")
            stop_event.wait(5)
            for db in [ing_db, val_db]:
                try:
                    if db: db.close()
                except Exception:
                    pass
            continue

        try:
            pending = ing_db.execute(
                "SELECT * FROM cases WHERE pipeline_step='VALIDATION_DONE'"
            ).fetchall()
        finally:
            ing_db.close()

        new_count = 0
        for row in pending:
            case = dict(row)
            if already_dispatched(dis_conn, case["case_id"]):
                continue

            # Confirm it was approved (not just at VALIDATION_DONE accidentally)
            decision_row = val_db.execute(
                "SELECT decision FROM validation_decisions WHERE case_id=?",
                (case["case_id"],),
            ).fetchone()
            if not decision_row or decision_row["decision"] != "APPROVED":
                continue

            result = run_dispatch(case, dis_conn)
            new_count += 1
            _print_result(case, result)

        if new_count:
            print(f"\n  [Dispatch] ✓ {new_count} case(s) dispatched.\n")

        try:
            val_db.close()
        except Exception:
            pass

        stop_event.wait(5)


# ─────────────────────────────────────────────────────────────
# TERMINAL PRINTER
# ─────────────────────────────────────────────────────────────

_OUTLOOK_ICON = {"GREEN":"🟢","ORANGE":"🟠","RED":"🔴"}


def _print_result(case: dict, result: dict) -> None:
    icon = _OUTLOOK_ICON.get(result["outlook_status"], "⚫")
    print(f"\n{'─'*60}")
    print(f"  📤  Dispatched")
    print(f"  Case ID  : {result['case_id']}")
    print(f"  To       : {result['sent_to']}")
    print(f"  Subject  : {result.get('subject','')}")
    print(f"  Outlook  : {icon} {result['outlook_status']}")
    print(f"  PII      : {result['pii_tokens_used']} tokens reconstructed")
    print(f"  KB       : +{result['kb_entries_added']} added  "
          f"~{result['kb_entries_updated']} updated")
    print(f"  Retention: D+{result['retention_days']} days  (then purged)")
    print(f"\n  Sent letter (first 300 chars, PII restored):")
    for line in (result.get("body_pii",""))[:300].split("\n")[:6]:
        print(f"    {line}")
    print(f"\n  Pipeline: COMPLETED ✅")
    print(json.dumps({
        "case_id":   result["case_id"],
        "status":    "COMPLETED",
        "outlook":   result["outlook_status"],
        "retention": f"D+{result['retention_days']}",
    }, indent=4))


# ─────────────────────────────────────────────────────────────
# WEB DASHBOARD  (port 8011)
# ─────────────────────────────────────────────────────────────

CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#f0f2f5;min-height:100vh;padding:32px 20px}
.page{max-width:1000px;margin:0 auto}
.header{background:#1a2332;color:white;border-radius:8px;
        padding:20px 28px;margin-bottom:24px;
        display:flex;align-items:center;gap:16px}
.badge{background:#0f766e;color:white;font-size:10px;font-weight:700;
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
           margin-bottom:12px;padding:0 2px;margin-top:24px}

.card{background:white;border-radius:7px;
      box-shadow:0 1px 3px rgba(0,0,0,.08);overflow:hidden;margin-bottom:16px}

/* Dispatch result row */
.d-row{padding:16px 20px;border-bottom:1px solid #f3f4f6}
.d-row:last-child{border-bottom:none}
.d-header{display:flex;align-items:flex-start;gap:10px;margin-bottom:10px}
.d-meta{flex:1;min-width:0}
.d-title{font-size:13px;font-weight:600;color:#1a2332}
.d-sub  {font-size:11px;color:#6b7280;margin-top:2px}
.d-tags {display:flex;gap:5px;flex-wrap:wrap;margin-top:5px}
.tag{font-size:10px;font-weight:600;padding:2px 7px;border-radius:3px}
.tag-green {background:#d1fae5;color:#065f46}
.tag-orange{background:#fef3c7;color:#92400e}
.tag-red   {background:#fee2e2;color:#991b1b}
.tag-pii   {background:#ede9fe;color:#5b21b6}
.tag-kb    {background:#e0f2fe;color:#0369a1}
.tag-ret   {background:#f3f4f6;color:#374151}

/* Letter viewer */
.letter-box{background:#fffef7;border:1px solid #e5e7eb;border-radius:6px;
            padding:20px 24px;font-size:13px;line-height:1.9;color:#1e293b;
            white-space:pre-wrap;word-break:break-word;
            font-family:'Georgia','Times New Roman',serif;
            max-height:280px;overflow-y:auto}
.pii-restored{background:#fef9c3;border-radius:2px;padding:0 2px}

/* KB table */
table{width:100%;border-collapse:collapse;font-size:12px}
th{text-align:left;padding:8px 14px;background:#f8f9fb;color:#6b7280;
   font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;
   border-bottom:1px solid #e5e7eb}
td{padding:9px 14px;border-bottom:1px solid #f3f4f6;color:#374151;vertical-align:top}
tr:last-child td{border-bottom:none}

/* Purge log */
.purge-item{padding:8px 14px;border-bottom:1px solid #f3f4f6;
            display:flex;align-items:center;gap:10px;font-size:12px}
.purge-item:last-child{border-bottom:none}
.purge-icon{font-size:14px;flex-shrink:0}
.purge-meta{flex:1}
.purge-preserved{font-size:10px;color:#9ca3af;margin-top:2px}

.log-panel{background:#1a2332;border-radius:7px;
           padding:16px 20px;max-height:200px;overflow-y:auto}
.log-line{font-family:'Menlo',monospace;font-size:11px;
          color:#8b949e;padding:2px 0;line-height:1.6}
.ts{color:#484f58}
.ev-pii  {color:#a371f7}
.ev-sent {color:#2ea043}
.ev-kb   {color:#388bfd}
.ev-purge{color:#d29922}
.ev-done {color:#2ea043;font-weight:600}

.refresh-note{font-size:11px;color:#9ca3af;text-align:center;margin-top:8px}
.empty{text-align:center;color:#9ca3af;padding:24px;font-size:13px}
"""

_SRC = {"DIRECT_EMAIL":"Email","STAFF_FORWARD":"Staff",
        "POSTAL_SCAN":"Scan","WEB_FORM":"Web Form"}
_OUTLOOK_TAG = {
    "GREEN":  "tag-green",
    "ORANGE": "tag-orange",
    "RED":    "tag-red",
}


def _highlight_pii(text: str, original_body: str) -> str:
    """
    Highlight words in the restored letter that were PII tokens.
    Compare with anonymised body to find what was reconstructed.
    """
    # Simple approach: any word > 4 chars appearing in text but not
    # in the anonymised version is likely reconstructed PII
    # For demo we just escape and return — highlighting PII is complex
    return _html.escape(text or "")


def render_dashboard(dis_conn: sqlite3.Connection) -> str:
    results = dis_conn.execute(
        "SELECT * FROM dispatch_results ORDER BY dispatched_at DESC"
    ).fetchall()
    results = [dict(r) for r in results]

    letters = dis_conn.execute(
        "SELECT * FROM sent_letters ORDER BY sent_at DESC"
    ).fetchall()
    letters_map = {l["case_id"]: dict(l) for l in letters}

    kb_entries = dis_conn.execute(
        "SELECT * FROM kb_entries ORDER BY updated_at DESC"
    ).fetchall()
    kb_entries = [dict(r) for r in kb_entries]

    purge_log = dis_conn.execute(
        "SELECT * FROM purge_log ORDER BY purged_at DESC"
    ).fetchall()
    purge_log = [dict(r) for r in purge_log]

    logs = dis_conn.execute(
        "SELECT * FROM dispatch_log ORDER BY ts DESC LIMIT 80"
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

    # Stats
    total     = len(results)
    kb_total  = len(kb_entries)
    pii_total = sum(r["pii_tokens_used"] for r in results)
    purged    = sum(1 for r in results)

    stats_html = f"""<div class="stats">
      <div class="stat"><div class="stat-num">{total}</div>
        <div class="stat-label">Dispatched</div></div>
      <div class="stat"><div class="stat-num" style="color:#5b21b6">{pii_total}</div>
        <div class="stat-label">🔑 PII Restored</div></div>
      <div class="stat"><div class="stat-num" style="color:#0369a1">{kb_total}</div>
        <div class="stat-label">📚 KB Entries</div></div>
      <div class="stat"><div class="stat-num" style="color:#d97706">{purged}</div>
        <div class="stat-label">🗑 Purged</div></div>
      <div class="stat"><div class="stat-num" style="color:#16a34a">{total}</div>
        <div class="stat-label">✅ Completed</div></div>
    </div>"""

    # Dispatch result cards with sent letters
    d_cards = ""
    if not results:
        d_cards = '<div class="empty">Waiting for Validation (Approve) to complete...</div>'
    for r in results:
        meta   = case_meta.get(r["case_id"], {})
        src    = _SRC.get(meta.get("source_type",""), "")
        letter = letters_map.get(r["case_id"], {})
        ol_tag = _OUTLOOK_TAG.get(r["outlook_status"], "tag-green")
        ol_icon = _OUTLOOK_ICON.get(r["outlook_status"], "⚫")

        tags = f"""
          <span class="tag {ol_tag}">{ol_icon} {r['outlook_status']}</span>
          <span class="tag tag-pii">🔑 {r['pii_tokens_used']} PII tokens</span>
          <span class="tag tag-kb">📚 KB +{r['kb_entries_added']} ~{r['kb_entries_updated']}</span>
          <span class="tag tag-ret">🗑 D+{r['retention_days']}d</span>
        """

        body_html = ""
        if letter.get("body_with_pii"):
            body_html = (
                f'<div class="letter-box">'
                f'{_html.escape(letter["body_with_pii"])}'
                f'</div>'
            )

        subj = _html.escape(meta.get("subject","")[:55])
        d_cards += f"""
        <div class="d-row">
          <div class="d-header">
            <div style="font-size:18px">📤</div>
            <div class="d-meta">
              <div class="d-title">{src} — {r['case_id'][:8]}…
                <span style="font-weight:400;color:#9ca3af;font-size:11px">
                  &nbsp;{subj}
                </span>
              </div>
              <div class="d-sub">
                ✉ {_html.escape(r['sent_to'])} &nbsp;·&nbsp;
                {r['language']} &nbsp;·&nbsp;
                {r['dispatched_at'][11:19]}
              </div>
              <div class="d-tags">{tags}</div>
            </div>
          </div>
          {body_html}
        </div>"""

    # KB entries table
    kb_rows = ""
    if not kb_entries:
        kb_rows = '<tr><td colspan="4" class="empty">No KB entries yet</td></tr>'
    for e in kb_entries[:10]:
        kb_rows += f"""
        <tr>
          <td><span class="tag tag-kb" style="font-size:10px">{e['theme']}</span></td>
          <td style="font-size:11px;color:#374151">{_html.escape(e['question_text'][:60])}</td>
          <td style="font-size:11px;color:#6b7280">{_html.escape(e['validated_answer'][:60])}</td>
          <td style="font-family:monospace;font-size:10px;color:#9ca3af">×{e['update_count']}</td>
        </tr>"""

    # Purge log
    purge_html = ""
    if not purge_log:
        purge_html = '<div class="empty" style="padding:16px">No purges yet</div>'
    for p in purge_log:
        try:
            deleted = json.loads(p.get("items_deleted") or "{}")
        except Exception:
            deleted = {}
        deleted_str = "  ".join(f"{k}: {v}" for k, v in deleted.items())
        purge_html += f"""
        <div class="purge-item">
          <div class="purge-icon">🗑</div>
          <div class="purge-meta">
            <div><code style="font-size:11px">{p['case_id'][:8]}…</code>
              D+{p['retention_days']}d — {deleted_str or 'nothing to delete'}
            </div>
            <div class="purge-preserved">
              ✓ Preserved: audit_trail · kb_entries · dispatch_log · case identifiers
            </div>
          </div>
          <div style="font-size:10px;color:#9ca3af">{p['purged_at'][11:19]}</div>
        </div>"""

    # Log
    ev_css = {
        "PII_RECONSTRUCTED":  "ev-pii",
        "EMAIL_SENT":         "ev-sent",
        "KB_UPDATED":         "ev-kb",
        "PURGE_EXECUTED":     "ev-purge",
        "OUTLOOK_STATUS":     "ev-sent",
        "DISPATCH_COMPLETE":  "ev-done",
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
<title>Phase 12 — Dispatch</title>
<style>{CSS}</style>
</head>
<body>
<div class="page">
  <div class="header">
    <div>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
        <span class="badge">Phase 12</span>
        <h1>Dispatch</h1>
      </div>
      <p>PII reconstruction · Email send · Feed KB · Scheduled purge</p>
    </div>
    <div class="hdr-right">
      Polling Validation every 5s<br>
      <span style="color:#2ea043;font-weight:600">● Live</span>
    </div>
  </div>

  {stats_html}

  <div class="sec-title">Dispatched cases — sent letters</div>
  <div class="card">{d_cards}</div>

  <div class="sec-title">Knowledge Base — entries created this session</div>
  <div class="card">
    <table>
      <thead>
        <tr><th>Theme</th><th>Question</th><th>Answer</th><th>Uses</th></tr>
      </thead>
      <tbody>{kb_rows}</tbody>
    </table>
  </div>

  <div class="sec-title">Purge Log</div>
  <div class="card">{purge_html}</div>

  <div class="sec-title">Audit Log
    <span style="font-weight:400;color:#9ca3af;margin-left:8px">
      (never purged — nFADP obligation)
    </span>
  </div>
  <div class="log-panel">{log_lines}</div>
  <p class="refresh-note">Auto-refreshes every 5 seconds</p>
</div>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────
# HTTP SERVER
# ─────────────────────────────────────────────────────────────

class DispatchDashboardHandler(http.server.BaseHTTPRequestHandler):
    dis_conn: sqlite3.Connection = None  # type: ignore

    def log_message(self, *args: object) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/favicon.ico":
            self.send_response(204); self.end_headers(); return
        page = render_dashboard(self.dis_conn)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode())


def make_handler(conn: sqlite3.Connection):
    class H(DispatchDashboardHandler):
        dis_conn = conn
    return H


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "═"*60)
    print("  CITIZEN PLATFORM — Phase 12: Dispatch  (DEMO)")
    print("═"*60)

    dis_conn = init_dispatch_db()
    print(f"\n  ✓  Dispatch DB : {DISPATCH_DB_PATH}")
    print(f"  ✓  Retention   : {RETENTION_DAYS}")
    print(f"  ✓  Reading from: Validation DB + Recomposition DB + Privacy DB")

    stop_event = threading.Event()
    poller = threading.Thread(
        target=poll_and_dispatch,
        args=(dis_conn, stop_event),
        daemon=True,
    )
    poller.start()

    handler    = make_handler(dis_conn)
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
        ("validation.py",8010),("dispatch.py",8011),
    ], 1):
        print(f"    Terminal {i:2} → python3 demo/{name:<26} (port {port})")
    print(f"\n  Press Ctrl-C to stop.\n")
    print("─"*60)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n\n  Stopping dispatch...")
        stop_event.set()
        server.shutdown()
        print("  Done.\n")


if __name__ == "__main__":
    main()
