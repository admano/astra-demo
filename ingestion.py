"""
demo/ingestion.py
-----------------
Phase 02: Ingestion  (DEMO VERSION)

Reads raw messages from Reception's SQLite DB and processes each one:

  1. Deduplication   — skip if message_id already ingested
  2. Follow-up check — detect citizen replies via in_reply_to
  3. Case ID         — generate a new UUID for every new case
  4. Language        — carry forward from Reception (already detected)
  5. Receipt ACK     — print the ACK email the citizen would receive
  6. Outlook status  — log "would set Yellow" (no real EWS in demo)
  7. Save case       — write to cases table in ingestion.db

Run:
    # Terminal 1 — start Reception first so its DB exists
    python3 demo/reception.py

    # Terminal 2 — run Ingestion (processes all pending messages once,
    #              then polls for new ones every 5 seconds)
    python3 demo/ingestion.py

Open http://localhost:8001 to see the live case dashboard.
"""

from __future__ import annotations

import http.server
import html as _html
import json
import re
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ─────────────────────────────────────────────────────────────
# PATHS  — Ingestion reads Reception's DB, writes its own
# ─────────────────────────────────────────────────────────────

DEMO_DIR          = Path(__file__).parent
RECEPTION_DB_PATH = DEMO_DIR / "demo_reception.db"
INGESTION_DB_PATH = DEMO_DIR / "demo_ingestion.db"
DEMO_TENANT_ID    = "11111111-1111-1111-1111-111111111111"
PORT              = 8001


# ─────────────────────────────────────────────────────────────
# DATABASE SETUP
# ─────────────────────────────────────────────────────────────

def init_ingestion_db() -> sqlite3.Connection:
    """
    Create the Ingestion database with two tables:
      cases         — one row per case (new or follow-up)
      ingested_ids  — tracks which raw message IDs were already processed
                      (deduplication guard)
    """
    conn = sqlite3.connect(str(INGESTION_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cases (
            case_id         TEXT PRIMARY KEY,
            message_id      TEXT NOT NULL,
            parent_case_id  TEXT,             -- set for FOLLOW-UP cases
            source_type     TEXT NOT NULL,
            language        TEXT NOT NULL,
            language_flagged INTEGER NOT NULL DEFAULT 0,
            status          TEXT NOT NULL DEFAULT 'OPEN',
            iteration       INTEGER NOT NULL DEFAULT 0,
            pipeline_step   TEXT NOT NULL DEFAULT 'INGESTION',
            tenant_id       TEXT NOT NULL,
            sender_email    TEXT,
            sender_name     TEXT,
            subject         TEXT,
            staff_notes     TEXT,
            auth_level      TEXT,
            attachment_names TEXT,
            created_at      TEXT NOT NULL,
            ack_sent        INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS ingested_ids (
            message_id  TEXT PRIMARY KEY,
            case_id     TEXT NOT NULL,
            ingested_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ingestion_log (
            id          TEXT PRIMARY KEY,
            case_id     TEXT NOT NULL,
            event       TEXT NOT NULL,
            detail      TEXT,
            ts          TEXT NOT NULL
        );
    """)
    conn.commit()
    return conn


def open_reception_db() -> sqlite3.Connection | None:
    """Open Reception DB read-only. Returns None if not found yet."""
    if not RECEPTION_DB_PATH.exists():
        return None
    conn = sqlite3.connect(f"file:{RECEPTION_DB_PATH}?mode=ro", uri=True,
                           check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


# ─────────────────────────────────────────────────────────────
# DEDUPLICATION
# ─────────────────────────────────────────────────────────────

def already_ingested(ing_conn: sqlite3.Connection, message_id: str) -> bool:
    """Return True if this message_id was already processed."""
    row = ing_conn.execute(
        "SELECT 1 FROM ingested_ids WHERE message_id = ?", (message_id,)
    ).fetchone()
    return row is not None


def mark_ingested(ing_conn: sqlite3.Connection,
                  message_id: str, case_id: str) -> None:
    ing_conn.execute(
        "INSERT INTO ingested_ids VALUES (?, ?, ?)",
        (message_id, case_id, datetime.now(timezone.utc).isoformat()),
    )
    ing_conn.commit()


# ─────────────────────────────────────────────────────────────
# FOLLOW-UP DETECTION
# ─────────────────────────────────────────────────────────────

def find_parent_case(ing_conn: sqlite3.Connection,
                     in_reply_to: str | None) -> str | None:
    """
    Check if in_reply_to references a message_id we already ingested.
    Returns parent case_id if found, else None.

    In production this also checks the References header chain.
    """
    if not in_reply_to:
        return None
    row = ing_conn.execute(
        "SELECT case_id FROM ingested_ids WHERE message_id = ?",
        (in_reply_to,),
    ).fetchone()
    return row["case_id"] if row else None


# ─────────────────────────────────────────────────────────────
# RECEIPT ACK TEMPLATES
# ─────────────────────────────────────────────────────────────

_ACK: dict[str, tuple[str, str]] = {
    "DE": (
        "Ihre Anfrage wurde erhalten",
        "Guten Tag,\n\nWir bestätigen den Eingang Ihrer Anfrage.\n"
        "Referenz: {ref}\n\nMit freundlichen Grüssen\nASTRA",
    ),
    "FR": (
        "Votre demande a été reçue",
        "Madame, Monsieur,\n\nNous accusons réception de votre demande.\n"
        "Référence: {ref}\n\nCordialement,\nOFROU",
    ),
    "IT": (
        "La sua richiesta è stata ricevuta",
        "Gentile Signora/Signore,\n\nConfermamo la ricezione della sua richiesta.\n"
        "Riferimento: {ref}\n\nCordiali saluti,\nUST",
    ),
    "RM": (
        "Vossa dumonda è vegnida survegnida",
        "Bun di,\n\nNus confermain la retschavida da vossa dumonda.\n"
        "Referenza: {ref}\n\nCordials salids,\nUST",
    ),
    "EN": (
        "Your request has been received",
        "Dear Sir or Madam,\n\nWe confirm receipt of your request.\n"
        "Reference: {ref}\n\nKind regards,\nASTRA",
    ),
}


def build_ack(language: str, case_ref: str) -> tuple[str, str]:
    """Return (subject, body) for the receipt ACK in the given language."""
    lang = language.upper() if language.upper() in _ACK else "EN"
    subject, body_tpl = _ACK[lang]
    return subject, body_tpl.format(ref=case_ref)


# ─────────────────────────────────────────────────────────────
# CORE INGESTION LOGIC
# ─────────────────────────────────────────────────────────────

def log_event(ing_conn: sqlite3.Connection, case_id: str,
              event: str, detail: str = "") -> None:
    ing_conn.execute(
        "INSERT INTO ingestion_log VALUES (?, ?, ?, ?, ?)",
        (str(uuid.uuid4()), case_id, event, detail,
         datetime.now(timezone.utc).isoformat()),
    )
    ing_conn.commit()


def ingest_message(raw: dict[str, Any],
                   ing_conn: sqlite3.Connection) -> dict[str, Any] | None:
    """
    Process one raw message through all Ingestion sub-steps.

    Returns the created case dict, or None if the message was a duplicate.

    Sub-steps (in order):
      1. Deduplication
      2. Follow-up detection
      3. Case ID generation
      4. Language resolution
      5. Case record save
      6. Receipt ACK (printed, not emailed)
      7. Outlook status log (Yellow)
      8. Mark as ingested
    """
    message_id = raw["message_id"]

    # ── 1. Deduplication ─────────────────────────────────────
    if already_ingested(ing_conn, message_id):
        return None   # silently skip duplicates

    # ── 2. Follow-up detection ────────────────────────────────
    parent_case_id = find_parent_case(ing_conn, raw.get("in_reply_to"))
    is_follow_up   = parent_case_id is not None
    status         = "FOLLOW-UP" if is_follow_up else "OPEN"

    # ── 3. Case ID ────────────────────────────────────────────
    case_id = str(uuid.uuid4())

    # ── 4. Language ───────────────────────────────────────────
    # Reception already detected it; we carry it forward.
    # If unknown/other → fall back to tenant default (DE) and flag.
    language = (raw.get("detected_language") or "").upper()
    language_flagged = bool(raw.get("language_flagged", False))

    if language not in ("DE", "FR", "IT", "RM", "EN"):
        language         = "DE"   # tenant default for ASTRA demo
        language_flagged = True

    # ── 5. Save case ──────────────────────────────────────────
    case = {
        "case_id":          case_id,
        "message_id":       message_id,
        "parent_case_id":   parent_case_id,
        "source_type":      raw["source_type"],
        "language":         language,
        "language_flagged": 1 if language_flagged else 0,
        "status":           status,
        "iteration":        0,
        "pipeline_step":    "INGESTION",
        "tenant_id":        raw["tenant_id"],
        "sender_email":     raw.get("sender_email", ""),
        "sender_name":      raw.get("sender_name", ""),
        "subject":          raw.get("subject", ""),
        "staff_notes":      raw.get("staff_notes", ""),
        "auth_level":       raw.get("auth_level", "NONE"),
        "attachment_names": raw.get("attachment_names", ""),
        "created_at":       datetime.now(timezone.utc).isoformat(),
        "ack_sent":         0,
    }

    ing_conn.execute("""
        INSERT INTO cases VALUES (
            :case_id, :message_id, :parent_case_id, :source_type,
            :language, :language_flagged, :status, :iteration,
            :pipeline_step, :tenant_id, :sender_email, :sender_name,
            :subject, :staff_notes, :auth_level, :attachment_names,
            :created_at, :ack_sent
        )
    """, case)
    ing_conn.commit()

    log_event(ing_conn, case_id, "CASE_CREATED",
              f"source={raw['source_type']} lang={language} status={status}")

    # ── 6. Receipt ACK ────────────────────────────────────────
    ack_subject, ack_body = build_ack(language, case_id[:8].upper())

    # In production: send via EWS API.
    # In demo: log it so we can display it.
    log_event(ing_conn, case_id, "ACK_SENT",
              json.dumps({
                  "to":      raw.get("sender_email", ""),
                  "subject": ack_subject,
                  "body":    ack_body,
              }, ensure_ascii=False))

    # Update ack_sent flag
    ing_conn.execute(
        "UPDATE cases SET ack_sent = 1 WHERE case_id = ?", (case_id,)
    )
    ing_conn.commit()

    # ── 7. Outlook status (Yellow) ────────────────────────────
    log_event(ing_conn, case_id, "OUTLOOK_STATUS",
              f"colour=YELLOW message_id={message_id}")

    # ── 8. Mark ingested (dedup guard) ────────────────────────
    mark_ingested(ing_conn, message_id, case_id)

    return case


# ─────────────────────────────────────────────────────────────
# POLLING LOOP
# Reads pending raw messages from Reception DB every N seconds.
# ─────────────────────────────────────────────────────────────

def poll_and_ingest(ing_conn: sqlite3.Connection,
                    stop_event: threading.Event) -> None:
    """
    Background thread: poll Reception DB for new messages and ingest them.
    Runs until stop_event is set.
    """
    print("  [Ingestion] polling Reception DB every 5 seconds...")

    while not stop_event.is_set():
        rec_conn = open_reception_db()

        if rec_conn is None:
            print("  [Ingestion] waiting for Reception DB...")
            stop_event.wait(5)
            continue

        try:
            rows = rec_conn.execute(
                "SELECT * FROM raw_messages ORDER BY received_at ASC"
            ).fetchall()
        finally:
            rec_conn.close()

        new_count = 0
        for row in rows:
            raw = dict(row)
            result = ingest_message(raw, ing_conn)
            if result is not None:
                new_count += 1
                _print_ingested(result)

        if new_count:
            print(f"\n  [Ingestion] ✓ {new_count} new case(s) created.\n")

        stop_event.wait(5)   # wait 5s or until stop_event


# ─────────────────────────────────────────────────────────────
# TERMINAL PRINTER
# ─────────────────────────────────────────────────────────────

def _print_ingested(case: dict[str, Any]) -> None:
    status_icon = "↩" if case["status"] == "FOLLOW-UP" else "✨"
    flag        = " ⚑ FLAGGED" if case["language_flagged"] else ""

    print(f"\n{'─'*60}")
    print(f"  {status_icon}  Case created  [{case['status']}]")
    print(f"  Case ID  : {case['case_id']}")
    print(f"  Source   : {case['source_type']}")
    print(f"  Language : {case['language']}{flag}")
    print(f"  Subject  : {(case.get('subject') or '')[:55]}")
    if case["parent_case_id"]:
        print(f"  Parent   : {case['parent_case_id']}  (follow-up)")
    print(f"\n  → Next step: Phase 03 Security")
    print(f"    Payload for Security:")
    print(json.dumps({
        "case_id":    case["case_id"],
        "tenant_id":  case["tenant_id"],
        "language":   case["language"],
        "step":       "SECURITY",
    }, indent=4))


# ─────────────────────────────────────────────────────────────
# WEB DASHBOARD  (port 8001)
# ─────────────────────────────────────────────────────────────

CSS = """
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: #f0f2f5; min-height: 100vh; padding: 32px 20px;
}
.page { max-width: 900px; margin: 0 auto; }
.header {
  background: #1a2332; color: white; border-radius: 8px;
  padding: 20px 28px; margin-bottom: 24px;
  display: flex; align-items: center; gap: 16px;
}
.header-badge {
  background: #2ea043; color: white; font-size: 10px; font-weight: 700;
  padding: 3px 8px; border-radius: 3px; letter-spacing: .06em; text-transform: uppercase;
}
.header h1 { font-size: 18px; font-weight: 600; }
.header p  { font-size: 12px; color: #8b949e; margin-top: 2px; }
.header-right { margin-left: auto; text-align: right; }
.header-right span { font-size: 11px; color: #8b949e; }

.stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 24px; }
.stat-card {
  background: white; border-radius: 7px; padding: 16px 20px;
  box-shadow: 0 1px 3px rgba(0,0,0,.08);
}
.stat-num  { font-size: 28px; font-weight: 700; color: #1a2332; }
.stat-label { font-size: 11px; color: #8b949e; margin-top: 2px; text-transform: uppercase; letter-spacing: .05em; }

.section-title {
  font-size: 11px; font-weight: 700; color: #8b949e;
  text-transform: uppercase; letter-spacing: .07em;
  margin-bottom: 10px; padding: 0 2px;
}

.case-table {
  background: white; border-radius: 7px;
  box-shadow: 0 1px 3px rgba(0,0,0,.08); overflow: hidden;
  margin-bottom: 24px;
}
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th {
  text-align: left; padding: 10px 14px;
  background: #f8f9fb; color: #6b7280;
  font-size: 11px; font-weight: 600; text-transform: uppercase;
  letter-spacing: .05em; border-bottom: 1px solid #e5e7eb;
}
td { padding: 11px 14px; border-bottom: 1px solid #f3f4f6; color: #374151; vertical-align: middle; }
tr:last-child td { border-bottom: none; }
tr:hover td { background: #fafafa; }

.pill {
  display: inline-block; font-size: 10px; font-weight: 700;
  padding: 2px 8px; border-radius: 10px; text-transform: uppercase;
}
.pill-open     { background: #dbeafe; color: #1d4ed8; }
.pill-followup { background: #fef3c7; color: #92400e; }
.pill-flagged  { background: #fee2e2; color: #dc2626; }

.src-dot {
  display: inline-block; width: 8px; height: 8px;
  border-radius: 50%; margin-right: 5px; vertical-align: middle;
}
.dot-email { background: #3b82f6; }
.dot-staff { background: #f59e0b; }
.dot-scan  { background: #8b5cf6; }
.dot-form  { background: #10b981; }

.log-panel {
  background: #1a2332; border-radius: 7px;
  padding: 16px 20px; max-height: 280px; overflow-y: auto;
}
.log-line {
  font-family: 'Menlo', 'Courier New', monospace; font-size: 11px;
  color: #8b949e; padding: 2px 0; line-height: 1.6;
}
.log-line .ts    { color: #484f58; }
.log-line .event { font-weight: 600; }
.log-line .ev-created { color: #2ea043; }
.log-line .ev-ack     { color: #388bfd; }
.log-line .ev-outlook { color: #d29922; }

.refresh-note {
  font-size: 11px; color: #9ca3af; text-align: center;
  margin-top: 8px;
}

.ack-detail {
  background: #f8f9fb; border-left: 3px solid #388bfd;
  border-radius: 0 5px 5px 0; padding: 12px 16px; margin: 8px 0;
  font-size: 12px; color: #374151;
}
.ack-detail .ack-label {
  font-size: 10px; font-weight: 700; color: #6b7280;
  text-transform: uppercase; letter-spacing: .05em; margin-bottom: 4px;
}
pre.ack-body {
  font-family: 'Menlo', monospace; font-size: 11px;
  white-space: pre-wrap; color: #4b5563; margin-top: 6px;
}
"""

def _src_dot(src: str) -> str:
    cls = {"DIRECT_EMAIL":"dot-email","STAFF_FORWARD":"dot-staff",
           "POSTAL_SCAN":"dot-scan","WEB_FORM":"dot-form"}.get(src,"dot-email")
    label = {"DIRECT_EMAIL":"Email","STAFF_FORWARD":"Staff",
              "POSTAL_SCAN":"Scan","WEB_FORM":"Web Form"}.get(src, src)
    return f'<span class="src-dot {cls}"></span>{label}'


def render_dashboard(ing_conn: sqlite3.Connection) -> str:
    cases = ing_conn.execute(
        "SELECT * FROM cases ORDER BY created_at DESC"
    ).fetchall()
    cases = [dict(r) for r in cases]

    logs = ing_conn.execute(
        "SELECT * FROM ingestion_log ORDER BY ts DESC LIMIT 60"
    ).fetchall()
    logs = [dict(r) for r in logs]

    # Stats
    total     = len(cases)
    open_n    = sum(1 for c in cases if c["status"] == "OPEN")
    followup  = sum(1 for c in cases if c["status"] == "FOLLOW-UP")
    flagged   = sum(1 for c in cases if c["language_flagged"])

    stats_html = f"""
    <div class="stats">
      <div class="stat-card">
        <div class="stat-num">{total}</div>
        <div class="stat-label">Total Cases</div>
      </div>
      <div class="stat-card">
        <div class="stat-num" style="color:#1d4ed8">{open_n}</div>
        <div class="stat-label">Open</div>
      </div>
      <div class="stat-card">
        <div class="stat-num" style="color:#92400e">{followup}</div>
        <div class="stat-label">Follow-Up</div>
      </div>
      <div class="stat-card">
        <div class="stat-num" style="color:#dc2626">{flagged}</div>
        <div class="stat-label">Lang Flagged</div>
      </div>
    </div>"""

    # Case rows
    rows_html = ""
    if not cases:
        rows_html = '<tr><td colspan="6" style="text-align:center;color:#9ca3af;padding:24px">No cases yet — waiting for Reception...</td></tr>'
    for c in cases:
        status_pill = (
            f'<span class="pill pill-followup">Follow-Up</span>'
            if c["status"] == "FOLLOW-UP"
            else f'<span class="pill pill-open">Open</span>'
        )
        flag_pill = (
            ' <span class="pill pill-flagged">⚑ lang</span>'
            if c["language_flagged"] else ""
        )
        subj = _html.escape((c.get("subject") or "")[:45])
        rows_html += f"""
        <tr>
          <td><code style="font-size:11px;color:#6b7280">{c['case_id'][:8]}…</code></td>
          <td>{_src_dot(c['source_type'])}</td>
          <td title="{_html.escape(c.get('subject',''))}">{subj}</td>
          <td><span style="font-weight:600">{c['language']}</span>{flag_pill}</td>
          <td>{status_pill}</td>
          <td style="color:#9ca3af;font-size:11px">{c['created_at'][11:19]}</td>
        </tr>"""

    # ACK detail for the most recent case
    ack_html = ""
    if cases:
        latest = cases[0]
        ack_log = ing_conn.execute(
            "SELECT detail FROM ingestion_log WHERE case_id=? AND event='ACK_SENT'",
            (latest["case_id"],),
        ).fetchone()
        if ack_log:
            try:
                ack = json.loads(ack_log["detail"])
                ack_html = f"""
                <div class="section-title" style="margin-top:24px">Receipt ACK — most recent case</div>
                <div class="ack-detail">
                  <div class="ack-label">To: {_html.escape(ack.get('to',''))}</div>
                  <div class="ack-label" style="margin-top:6px">Subject: {_html.escape(ack.get('subject',''))}</div>
                  <pre class="ack-body">{_html.escape(ack.get('body',''))}</pre>
                </div>"""
            except Exception:
                pass

    # Audit log
    ev_css = {"CASE_CREATED":"ev-created","ACK_SENT":"ev-ack","OUTLOOK_STATUS":"ev-outlook"}
    log_lines = ""
    for lg in logs:
        ev    = lg["event"]
        css   = ev_css.get(ev, "")
        ts    = lg["ts"][11:19]
        detail = (lg.get("detail") or "")[:80]
        log_lines += (
            f'<div class="log-line">'
            f'<span class="ts">{ts}</span>  '
            f'<span class="event {css}">{ev}</span>  '
            f'<span>{_html.escape(detail)}</span>'
            f'</div>\n'
        )
    if not log_lines:
        log_lines = '<div class="log-line" style="color:#484f58">— no events yet —</div>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="5">
<title>Phase 02 — Ingestion</title>
<style>{CSS}</style>
</head>
<body>
<div class="page">

  <div class="header">
    <div>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
        <span class="header-badge">Phase 02</span>
        <h1>Ingestion</h1>
      </div>
      <p>Reads from Reception → deduplicates → creates cases → sends ACK</p>
    </div>
    <div class="header-right">
      <span>Polling Reception every 5s</span><br>
      <span style="color:#2ea043;font-weight:600">● Live</span>
    </div>
  </div>

  {stats_html}

  <div class="section-title">Cases created by Ingestion</div>
  <div class="case-table">
    <table>
      <thead>
        <tr>
          <th>Case ID</th>
          <th>Source</th>
          <th>Subject</th>
          <th>Language</th>
          <th>Status</th>
          <th>Time</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>

  {ack_html}

  <div class="section-title" style="margin-top:24px">Audit Log</div>
  <div class="log-panel">{log_lines}</div>

  <p class="refresh-note">Auto-refreshes every 5 seconds</p>
</div>
</body>
</html>"""


class IngestionDashboardHandler(http.server.BaseHTTPRequestHandler):
    ing_conn: sqlite3.Connection = None  # type: ignore

    def log_message(self, *args: object) -> None:
        pass  # silence HTTP logs

    def do_GET(self) -> None:
        if self.path == "/favicon.ico":
            self.send_response(204); self.end_headers(); return
        html = render_dashboard(self.ing_conn)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html.encode())


def make_dashboard_handler(conn: sqlite3.Connection):
    class H(IngestionDashboardHandler):
        ing_conn = conn
    return H


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "═"*60)
    print("  CITIZEN PLATFORM — Phase 02: Ingestion  (DEMO)")
    print("═"*60)

    ing_conn = init_ingestion_db()
    print(f"\n  ✓  Ingestion DB : {INGESTION_DB_PATH}")
    print(f"  ✓  Reading from : {RECEPTION_DB_PATH}")

    # Start polling thread
    stop_event = threading.Event()
    poller = threading.Thread(
        target=poll_and_ingest,
        args=(ing_conn, stop_event),
        daemon=True,
    )
    poller.start()

    # Start dashboard HTTP server
    handler = make_dashboard_handler(ing_conn)
    server  = http.server.HTTPServer(("", PORT), handler)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    print(f"\n  ✓  Dashboard: http://localhost:{PORT}")
    print(f"\n  Tip: open Reception in another terminal first:")
    print(f"       python3 demo/reception.py")
    print(f"\n  Press Ctrl-C to stop.\n")
    print("─"*60)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n\n  Stopping ingestion...")
        stop_event.set()
        server.shutdown()
        print("  Done.\n")


if __name__ == "__main__":
    main()
