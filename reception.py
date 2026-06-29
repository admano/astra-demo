"""
demo/reception.py
-----------------
Sprint 1 — Phase 01: Reception  (DEMO VERSION)

Simulates all 4 input channels in a single self-contained file.
No external packages required — runs with:

    python3 demo/reception.py

What this demo covers
---------------------
  Source 1 — Direct citizen email     (simulated IMAP)
  Source 2 — Staff forward            (simulated EWS forward with notes)
  Source 3 — Postal / scanned letter  (simulated OCR text upload)
  Source 4 — Web form                 (real HTTP form on localhost:8000)

The web form is live in your browser. The other 3 sources are
pre-loaded as sample messages you can inspect.

Output: every received message is normalised into a RawMessage dict
and printed as JSON — exactly what Ingestion will consume next sprint.

Press Ctrl-C to stop.
"""

from __future__ import annotations

import http.server
import json
import re
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from string import Template
from typing import Any


# ─────────────────────────────────────────────────────────────
# SHARED PIPELINE DATABASE
# All four phases open the same SQLite file and talk to each
# other's tables directly — no per-phase DB files anymore.
# ─────────────────────────────────────────────────────────────
DATABASE_DIR  = Path(__file__).parent
DB_PATH       = DATABASE_DIR / "demo_db" / "demo_pipeline.db"
TEMPLATES_DIR = Path(__file__).parent / "templates"

# Next phase to wake up via HTTP as soon as a message is saved.
# Polling (in ingestion.py) remains as a fallback in case this call
# fails (e.g. Ingestion isn't up yet).
INGESTION_NOTIFY_URL = "http://localhost:8001/notify"


def _load_template(name: str) -> Template:

    return Template((TEMPLATES_DIR / name).read_text(encoding="utf-8"))


def _connect_shared_db(db_path: Path) -> sqlite3.Connection:
    """
    Connect to the pipeline DB shared by all four phases.

    All four phases tend to start at once, and switching a brand-new file
    to WAL mode briefly needs an exclusive lock — so the very first connect
    can race with another phase doing the same thing. Retry a few times
    instead of crashing on that one-time startup race.
    """
    db_path.parent.mkdir(exist_ok=True)
    last_exc: sqlite3.OperationalError | None = None
    for _ in range(10):
        try:
            conn = sqlite3.connect(str(db_path), check_same_thread=False, timeout=5.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            return conn
        except sqlite3.OperationalError as exc:
            last_exc = exc
            time.sleep(0.3)
    raise last_exc


def init_db() -> sqlite3.Connection:
    conn = _connect_shared_db(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_messages (
            id           TEXT PRIMARY KEY,
            received_at  TEXT NOT NULL,
            source_type  TEXT NOT NULL,
            message_id   TEXT NOT NULL,
            sender_email TEXT,
            sender_name  TEXT,
            subject      TEXT,
            body         TEXT,
            in_reply_to  TEXT,
            staff_notes  TEXT,
            auth_level   TEXT,
            attachment_names TEXT,   -- comma-separated filenames
            tenant_id    TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def _notify_next_phase(url: str) -> None:
    """Fire-and-forget HTTP push so the next phase processes immediately
    instead of waiting for its next poll tick. Failure is fine — the
    receiving phase's own polling loop is the fallback."""
    def _fire() -> None:
        try:
            urllib.request.urlopen(urllib.request.Request(url, method="POST"), timeout=2)
        except Exception:
            pass
    threading.Thread(target=_fire, daemon=True).start()


# ─────────────────────────────────────────────────────────────
# LANGUAGE DETECTOR  (lightweight, no external packages)
# ─────────────────────────────────────────────────────────────

# Simple word-frequency heuristic for DE/FR/IT/EN/RM
_LANG_HINTS: dict[str, list[str]] = {
    "DE": ["ich", "sie", "haben", "bitte", "sehr", "geehrte", "anfrage",
           "mit", "freundlichen", "grüssen", "strasse", "und", "der", "die", "das"],
    "FR": ["je", "vous", "monsieur", "madame", "bonjour", "merci", "votre",
           "salutations", "cordialement", "est", "une", "les", "des"],
    "IT": ["sono", "grazie", "gentile", "signore", "signora", "cordiali",
           "saluti", "per", "con", "una", "del", "della"],
    "EN": ["dear", "please", "regards", "sincerely", "hello", "thank",
           "your", "request", "information", "the", "and", "with"],
    "RM": ["grazia", "plaschair", "igl", "ella", "rumantsch",
           "confederaziun", "dumonda", "bun"],
}


def detect_language(text: str, default: str = "DE") -> tuple[str, bool]:
    """
    Returns (language_code, flagged).
    flagged=True means low confidence — needs manual review.
    """
    words = set(re.findall(r"\b\w+\b", text.lower()))
    scores: dict[str, int] = {}
    for lang, hints in _LANG_HINTS.items():
        scores[lang] = sum(1 for h in hints if h in words)

    best_lang = max(scores, key=lambda k: scores[k])
    best_score = scores[best_lang]

    if best_score == 0:
        return default, True   # no signal at all → flag

    # Flag if RM or if top two candidates are very close
    sorted_scores = sorted(scores.values(), reverse=True)
    too_close = len(sorted_scores) > 1 and sorted_scores[0] - sorted_scores[1] <= 1

    flagged = best_lang == "RM" or too_close
    return best_lang, flagged


# ─────────────────────────────────────────────────────────────
# RAW MESSAGE BUILDER
# Normalises any source channel into the same dict shape.
# ─────────────────────────────────────────────────────────────

DEMO_TENANT_ID = "11111111-1111-1111-1111-111111111111"


def build_raw_message(
    source_type: str,
    subject: str,
    body: str,
    sender_email: str = "",
    sender_name: str = "",
    in_reply_to: str | None = None,
    staff_notes: str = "",
    auth_level: str = "NONE",
    attachment_names: list[str] | None = None,
) -> dict[str, Any]:
    """
    Normalise any input into the standard RawMessage shape.
    This is the contract Ingestion will consume.
    """
    msg_id = f"<{uuid.uuid4()}@demo.admin.ch>"
    language, flagged = detect_language(subject + " " + body)

    return {
        "id":               str(uuid.uuid4()),
        "received_at":      datetime.now(timezone.utc).isoformat(),
        "source_type":      source_type,
        "message_id":       msg_id,
        "sender_email":     sender_email,
        "sender_name":      sender_name,
        "subject":          subject,
        "body":             body,
        "in_reply_to":      in_reply_to,
        "staff_notes":      staff_notes,
        "auth_level":       auth_level,
        "attachment_names": ",".join(attachment_names or []),
        "tenant_id":        DEMO_TENANT_ID,
        # — set by this phase —
        "detected_language": language,
        "language_flagged":  flagged,
    }


def save_message(conn: sqlite3.Connection, msg: dict[str, Any]) -> None:
    conn.execute("""
        INSERT INTO raw_messages VALUES (
            :id, :received_at, :source_type, :message_id,
            :sender_email, :sender_name, :subject, :body,
            :in_reply_to, :staff_notes, :auth_level,
            :attachment_names, :tenant_id
        )
    """, msg)
    conn.commit()
    _notify_next_phase(INGESTION_NOTIFY_URL)


def get_all_messages(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM raw_messages ORDER BY received_at DESC"
    ).fetchall()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────
# SOURCE 1 — DIRECT CITIZEN EMAIL  (simulated IMAP poll)
# In production: replaced by a real IMAP client polling the
# ASTRA citizen mailbox every N seconds.
# ─────────────────────────────────────────────────────────────

SAMPLE_EMAILS = [
    {
        "subject": "Frage zu meinem Führerausweis",
        "body": (
            "Sehr geehrte Damen und Herren,\n\n"
            "ich habe vor 3 Wochen meinen Führerausweis verloren und "
            "einen Ersatz beantragt. Bis heute habe ich keine Antwort erhalten.\n\n"
            "Könnten Sie mir bitte den aktuellen Stand mitteilen?\n\n"
            "Mit freundlichen Grüssen\nHans Muster"
        ),
        "sender_email": "h.muster@example.ch",
        "sender_name": "Hans Muster",
    },
    {
        "subject": "Question sur mon permis de conduire",
        "body": (
            "Madame, Monsieur,\n\n"
            "J'ai soumis ma demande de renouvellement de permis il y a "
            "deux semaines mais je n'ai reçu aucune confirmation.\n\n"
            "Merci de bien vouloir me donner des informations.\n\n"
            "Cordiales salutations\nMarie Dupont"
        ),
        "sender_email": "marie.dupont@example.ch",
        "sender_name": "Marie Dupont",
    },
]


def load_source1_samples(conn: sqlite3.Connection) -> list[dict]:
    """Simulate receiving emails from the IMAP mailbox."""
    messages = []
    for email in SAMPLE_EMAILS:
        msg = build_raw_message(
            source_type="DIRECT_EMAIL",
            **email,
            auth_level="NONE",
        )
        save_message(conn, msg)
        messages.append(msg)
        print(f"  [Source 1 / IMAP]  received: {email['subject'][:50]}")
    return messages


# ─────────────────────────────────────────────────────────────
# SOURCE 2 — STAFF FORWARD  (simulated EWS forward with notes)
# In production: employee forwards citizen email via Outlook
# with internal notes. Authenticated via FED LOGIN.
# ─────────────────────────────────────────────────────────────

def load_source2_sample(conn: sqlite3.Connection) -> dict:
    """Simulate a staff member forwarding a citizen email with notes."""
    msg = build_raw_message(
        source_type="STAFF_FORWARD",
        subject="FW: Beschwerde Strassenmarkierung Bern",
        body=(
            "Sehr geehrte Damen und Herren,\n\n"
            "Die Strassenmarkierung auf der A1 bei Bern-Wankdorf "
            "ist seit Wochen beschädigt und stellt eine Gefahr dar.\n\n"
            "Bitte nehmen Sie dies ernst.\n\nDanke"
        ),
        sender_email="buerger@example.ch",
        sender_name="Peter Meier",
        staff_notes="PRIORITÄT: Dieser Bürger hat bereits 2x angerufen. "
                    "Bitte schnell bearbeiten. —Agent Weber",
        auth_level="FED_LOGIN",
    )
    save_message(conn, msg)
    print(f"  [Source 2 / Staff]  forwarded: {msg['subject'][:50]}")
    return msg


# ─────────────────────────────────────────────────────────────
# SOURCE 3 — POSTAL SCAN  (simulated BBL OCR)
# In production: physical letter scanned by BBL, OCR'd to text,
# emailed to ASTRA mailbox as plain text attachment.
# ─────────────────────────────────────────────────────────────

def load_source3_sample(conn: sqlite3.Connection) -> dict:
    """Simulate a scanned letter received via OCR from BBL."""
    msg = build_raw_message(
        source_type="POSTAL_SCAN",
        subject="[BBL-SCAN] Brief vom 2025-01-15",
        body=(
            "[OCR-EXTRAKT]\n\n"
            "Bundesamt für Strassen ASTRA\n"
            "3003 Bern\n\n"
            "15. Januar 2025\n\n"
            "Sehr geehrte Damen und Herren\n\n"
            "Ich beziehe mich auf mein Schreiben vom 10. Dezember 2024 "
            "betreffend der Lärmschutzmassnahmen entlang der N3 "
            "im Bereich Sargans. Bis heute habe ich keine Antwort erhalten.\n\n"
            "Ich bitte Sie höflich, meine Anfrage zu bearbeiten.\n\n"
            "Freundliche Grüsse\nAnna Beispiel\nMusterstrasse 12\n7320 Sargans"
        ),
        sender_email="bbl-scan@admin.ch",
        sender_name="BBL Scan Service",
        auth_level="NONE",
        attachment_names=["scan_brief_2025-01-15.pdf"],
    )
    save_message(conn, msg)
    print(f"  [Source 3 / Scan]   scanned:   {msg['subject'][:50]}")
    return msg


# ─────────────────────────────────────────────────────────────
# SOURCE 4 — WEB FORM  (real HTTP server on localhost:8000)
# ─────────────────────────────────────────────────────────────


def render_inbox(conn: sqlite3.Connection) -> str:
    messages = get_all_messages(conn)
    if not messages:
        return '<p style="color:#aaa;font-size:13px;padding:8px">Noch keine Nachrichten.</p>'

    src_meta = {
        "DIRECT_EMAIL":  ("dot-email",  "lbl-email",  "Email"),
        "STAFF_FORWARD": ("dot-staff",  "lbl-staff",  "Staff"),
        "POSTAL_SCAN":   ("dot-scan",   "lbl-scan",   "Scan"),
        "WEB_FORM":      ("dot-form",   "lbl-form",   "Web Form"),
    }

    rows = []
    for m in messages[:20]:
        dot, lbl, label = src_meta.get(m["source_type"], ("dot-email", "lbl-email", m["source_type"]))
        subject = m["subject"] or "(no subject)"
        import html as _html
        rows.append(f"""
        <div class="msg-row">
          <div class="src-dot {dot}"></div>
          <div class="msg-meta">
            <div class="msg-subject">{_html.escape(subject)}</div>
            <div class="msg-detail">
              {_html.escape(m.get('sender_email') or 'unknown')}
              · {m['received_at'][:19].replace('T',' ')}
            </div>
          </div>
          <span class="src-label {lbl}">{label}</span>
        </div>""")
    return "\n".join(rows)


# ─────────────────────────────────────────────────────────────
# HTTP SERVER  (serves the web form + handles POST)
# ─────────────────────────────────────────────────────────────

class ReceptionHandler(http.server.BaseHTTPRequestHandler):

    # injected by the factory
    db_conn: sqlite3.Connection = None  # type: ignore

    def log_message(self, fmt: str, *args: object) -> None:
        pass  # suppress default access log — we use our own

    def do_GET(self) -> None:
        if self.path not in ("/", "/favicon.ico"):
            self.send_response(404)
            self.end_headers()
            return
        if self.path == "/favicon.ico":
            self.send_response(204)
            self.end_headers()
            return

        inbox_html = render_inbox(self.db_conn)
        page = _load_template("reception_form.html").substitute(inbox_rows=inbox_html)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode())

    def do_POST(self) -> None:
        if self.path != "/submit":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode("utf-8")
        fields = urllib.parse.parse_qs(raw, keep_blank_values=True)

        def f(key: str) -> str:
            vals = fields.get(key, [""])
            return vals[0].strip()

        first = f("first_name")
        last  = f("last_name")
        email = f("email")
        topic = f("topic")
        subj  = f("subject")
        body  = f("body")

        # Build the RawMessage
        msg = build_raw_message(
            source_type="WEB_FORM",
            subject=subj or "(no subject)",
            body=body,
            sender_email=email,
            sender_name=f"{first} {last}".strip(),
            auth_level="EGOV",
        )
        if topic:
            msg["topic"] = topic

        save_message(self.db_conn, msg)

        # Print normalised output (what Ingestion will consume)
        print_received(msg)

        # Send success response
        ref = msg["message_id"]
        page = _load_template("reception_success.html").substitute(ref=ref)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode())


def make_handler(conn: sqlite3.Connection):
    class Handler(ReceptionHandler):
        db_conn = conn
    return Handler


# ─────────────────────────────────────────────────────────────
# PRINT HELPER  (shows the normalised output for each message)
# ─────────────────────────────────────────────────────────────

def print_received(msg: dict[str, Any]) -> None:
    src_labels = {
        "DIRECT_EMAIL":  "Source 1 / IMAP",
        "STAFF_FORWARD": "Source 2 / Staff Forward",
        "POSTAL_SCAN":   "Source 3 / BBL Scan",
        "WEB_FORM":      "Source 4 / Web Form",
    }
    label = src_labels.get(msg["source_type"], msg["source_type"])
    lang  = msg.get("detected_language", "?")
    flag  = " ⚑ FLAGGED" if msg.get("language_flagged") else ""

    print(f"\n{'─'*60}")
    print(f"  📨  {label}")
    print(f"  ID  {msg['message_id']}")
    print(f"  ✉   {msg.get('sender_email','')}")
    print(f"  📋  {msg['subject'][:60]}")
    print(f"  🌐  Language: {lang}{flag}")
    print(f"  ⏰  {msg['received_at'][:19]}")
    if msg.get("staff_notes"):
        print(f"  📝  Staff notes: {msg['staff_notes'][:60]}")
    print(f"\n  → Normalised RawMessage (→ next: Phase 02 Ingestion):")

    # Show only the fields Ingestion cares about
    ingestion_payload = {
        "id":               msg["id"],
        "message_id":       msg["message_id"],
        "source_type":      msg["source_type"],
        "tenant_id":        msg["tenant_id"],
        "detected_language": msg.get("detected_language"),
        "language_flagged": msg.get("language_flagged"),
        "in_reply_to":      msg.get("in_reply_to"),
    }
    print(json.dumps(ingestion_payload, indent=4, ensure_ascii=False))


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    PORT = 8000

    print("\n" + "═"*60)
    print("  CITIZEN PLATFORM — Phase 01: Reception  (DEMO)")
    print("═"*60)

    # Init DB
    conn = init_db()
    print(f"\n  ✓  Shared pipeline DB:  {DB_PATH}")

    # Load the 3 simulated sources on startup
    print("\n  Loading simulated sources...")
    load_source1_samples(conn)
    load_source2_sample(conn)
    load_source3_sample(conn)

    # Start HTTP server for Source 4 (web form)
    handler = make_handler(conn)
    server  = http.server.HTTPServer(("", PORT), handler)
    thread  = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    print(f"\n  ✓  Web form (Source 4) running at:")
    print(f"     http://localhost:{PORT}")
    print(f"\n  Fill the form and submit — you'll see the normalised")
    print(f"  RawMessage printed here, ready for Phase 02 (Ingestion).")
    print(f"\n  Press Ctrl-C to stop.\n")
    print("─"*60)

    try:
        threading.Event().wait()   # block forever until Ctrl-C
    except KeyboardInterrupt:
        print("\n\n  Stopping reception server...")
        server.shutdown()
        print("  Done.\n")


if __name__ == "__main__":
    main()
