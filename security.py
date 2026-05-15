"""
demo/security.py
----------------
Phase 03: Security  (DEMO VERSION)

Reads cases at pipeline_step=INGESTION from Ingestion DB,
runs every security check, writes results to security.db,
and advances each clean case to pipeline_step=SECURITY_DONE.

Four checks — in spec order:
  ① Subject + body toxicity   (LlamaGuard → simulated with keyword rules)
  ② MIME type check           (magic-bytes → simulated from file extension)
  ③ Antimalware scan          (ClamAV     → simulated)
  ④ Attachment toxicity       (Docling→Markdown then LlamaGuard → simulated)

Outcomes per attachment:
  CLEAN   → passes, Markdown stored, ready for Privacy
  BLOCKED → reason recorded (TYPE_NOT_ALLOWED | INFECTED | TOXIC | PARSING_FAILED)

Case outcome:
  CLEAN    → all checks passed, pipeline_step → SECURITY_DONE
  ESCALATE → toxic body OR at least one blocked attachment → Outlook Purple

Run (three terminals):
    python3 demo/reception.py     ← port 8000
    python3 demo/ingestion.py     ← port 8001
    python3 demo/security.py      ← port 8002

Dashboard: http://localhost:8002
"""

from __future__ import annotations

import hashlib
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
RECEPTION_DB_PATH = DEMO_DIR / "demo_reception.db"
INGESTION_DB_PATH = DEMO_DIR / "demo_ingestion.db"
SECURITY_DB_PATH  = DEMO_DIR / "demo_security.db"
PORT              = 8002


# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────

def init_security_db() -> sqlite3.Connection:
    """
    Tables:
      security_results   — one row per case, overall verdict
      attachment_results — one row per attachment per case
      security_log       — append-only audit events
    """
    conn = sqlite3.connect(str(SECURITY_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS security_results (
            case_id         TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            verdict         TEXT NOT NULL,   -- CLEAN | ESCALATE
            body_toxic      INTEGER NOT NULL DEFAULT 0,
            attachment_count INTEGER NOT NULL DEFAULT 0,
            blocked_count   INTEGER NOT NULL DEFAULT 0,
            checked_at      TEXT NOT NULL,
            pipeline_step   TEXT NOT NULL DEFAULT 'SECURITY_DONE'
        );

        CREATE TABLE IF NOT EXISTS attachment_results (
            id              TEXT PRIMARY KEY,
            case_id         TEXT NOT NULL,
            filename        TEXT NOT NULL,
            file_hash       TEXT,
            declared_mime   TEXT,
            verified_mime   TEXT,
            status          TEXT NOT NULL,   -- CLEAN | BLOCKED
            blocked_reason  TEXT,            -- TYPE_NOT_ALLOWED | INFECTED | TOXIC | PARSING_FAILED
            markdown_text   TEXT,            -- simulated Docling output
            checked_at      TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS security_log (
            id       TEXT PRIMARY KEY,
            case_id  TEXT NOT NULL,
            event    TEXT NOT NULL,
            detail   TEXT,
            ts       TEXT NOT NULL
        );
    """)
    conn.commit()
    return conn


def open_db(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True,
                           check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def already_checked(sec_conn: sqlite3.Connection, case_id: str) -> bool:
    row = sec_conn.execute(
        "SELECT 1 FROM security_results WHERE case_id = ?", (case_id,)
    ).fetchone()
    return row is not None


def log_event(sec_conn: sqlite3.Connection,
              case_id: str, event: str, detail: str = "") -> None:
    sec_conn.execute(
        "INSERT INTO security_log VALUES (?,?,?,?,?)",
        (str(uuid.uuid4()), case_id, event, detail,
         datetime.now(timezone.utc).isoformat()),
    )
    sec_conn.commit()


# ─────────────────────────────────────────────────────────────
# ① TOXICITY CHECK  (simulates LlamaGuard)
#
# In production: the subject+body Markdown is sent to a locally
# hosted LlamaGuard model (Meta Llama Guard 3).
#
# In demo: keyword-based heuristic. Flags obvious harmful content.
# The same logic runs again on attachment Markdowns in step ④.
# ─────────────────────────────────────────────────────────────

# Words that would trigger LlamaGuard in a real deployment.
# Kept minimal — this is a demo, not a real classifier.
_TOXIC_PATTERNS = [
    r"\b(bomb|explosive|weapon|hack|kill|threat|attack)\b",
    r"\b(drogu|drogue|droga)\b",         # drug-related (multilingual)
    r"<script[\s>]",                      # XSS attempt in body
    r"you will (die|regret)",
]
_TOXIC_RE = re.compile("|".join(_TOXIC_PATTERNS), re.IGNORECASE)


def is_toxic(text: str) -> tuple[bool, str]:
    """
    Returns (toxic: bool, reason: str).
    In production: LlamaGuard REST call to local Ollama.
    """
    if not text:
        return False, ""
    match = _TOXIC_RE.search(text)
    if match:
        return True, f"toxic pattern matched: '{match.group()[:40]}'"
    return False, ""


# ─────────────────────────────────────────────────────────────
# ② MIME TYPE CHECK  (simulates magic-bytes verification)
#
# In production: python-magic reads the first 512 bytes of the
# attachment stream and returns the real MIME type regardless of
# the declared Content-Type header.
#
# In demo: extension → MIME mapping (good enough to show the logic).
# ─────────────────────────────────────────────────────────────

# Allowed document types per spec (office-configurable in production)
ALLOWED_MIME_TYPES = {
    "application/pdf",
    "image/jpeg",
    "image/png",
    "image/tiff",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "text/plain",
}

_EXT_TO_MIME: dict[str, str] = {
    ".pdf":  "application/pdf",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png":  "image/png",
    ".tiff": "image/tiff",
    ".tif":  "image/tiff",
    ".doc":  "application/msword",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".txt":  "text/plain",
    # Dangerous — should be blocked
    ".exe":  "application/x-msdownload",
    ".sh":   "application/x-sh",
    ".js":   "application/javascript",
    ".bat":  "application/x-bat",
    ".zip":  "application/zip",
}


def check_mime(filename: str) -> tuple[str, bool, str]:
    """
    Returns (verified_mime, allowed: bool, reason: str).
    In production: python-magic on raw bytes stream.
    """
    ext  = Path(filename).suffix.lower()
    mime = _EXT_TO_MIME.get(ext, "application/octet-stream")
    if mime not in ALLOWED_MIME_TYPES:
        return mime, False, f"type not allowed: {mime} ({ext})"
    return mime, True, ""


# ─────────────────────────────────────────────────────────────
# ③ ANTIMALWARE SCAN  (simulates ClamAV)
#
# In production: pyclamd streams the attachment bytes to the
# ClamAV daemon running locally (clamd on port 3310).
#
# In demo: deterministic simulation based on filename patterns.
# A filename containing "virus" or "malware" is treated as infected
# so the demo can show the BLOCKED/INFECTED path.
# ─────────────────────────────────────────────────────────────

_INFECTED_PATTERNS = re.compile(
    r"(virus|malware|trojan|eicar|infected)", re.IGNORECASE
)


def scan_antimalware(filename: str, content: str) -> tuple[bool, str]:
    """
    Returns (clean: bool, threat_name: str).
    In production: pyclamd.ClamdUnixSocket().scan_stream(bytes).
    """
    if _INFECTED_PATTERNS.search(filename):
        return False, "Demo.Virus.Simulation"
    # In production we'd also hash and check against known-bad hashes
    return True, ""


# ─────────────────────────────────────────────────────────────
# ④ DOCLING → MARKDOWN  (simulates document conversion)
#
# In production: the Docling service (sandboxed container) converts
# PDF/image/Office documents to clean Markdown. Images without text
# are described by a vision model (VLM). This runs in an isolated
# container to prevent parser-bomb attacks.
#
# In demo: we generate a plausible Markdown representation from
# the filename and any known content. Real content would come from
# the actual file bytes.
# ─────────────────────────────────────────────────────────────

def docling_to_markdown(filename: str, declared_mime: str) -> tuple[str, bool]:
    """
    Returns (markdown_text: str, success: bool).
    In production: POST to Docling container REST API.
    """
    ext = Path(filename).suffix.lower()

    # Simulate a parsing failure for unsupported types
    if ext in (".exe", ".sh", ".bat"):
        return "", False

    # Simulate plausible Markdown output for known types
    name_stem = Path(filename).stem.replace("_", " ").replace("-", " ").title()

    if ext == ".pdf":
        md = (
            f"# {name_stem}\n\n"
            f"*[Simulated OCR/PDF extraction — {filename}]*\n\n"
            f"Sehr geehrte Damen und Herren,\n\n"
            f"Ich beziehe mich auf mein Schreiben vom 10. Dezember 2024. "
            f"Bitte bearbeiten Sie meine Anfrage.\n\n"
            f"Freundliche Grüsse"
        )
    elif ext in (".jpg", ".jpeg", ".png", ".tiff", ".tif"):
        md = (
            f"# Image: {name_stem}\n\n"
            f"*[VLM description — {filename}]*\n\n"
            f"The image shows a scanned document with handwritten text. "
            f"The content appears to be a formal letter in German."
        )
    else:
        md = f"# {name_stem}\n\n*[Extracted text from {filename}]*\n\nDocument content."

    return md, True


# ─────────────────────────────────────────────────────────────
# SIMULATED FILE HASH  (SHA-256 of filename for demo)
# In production: SHA-256 of actual file bytes from S3 stream.
# ─────────────────────────────────────────────────────────────

def file_hash(filename: str) -> str:
    return hashlib.sha256(filename.encode()).hexdigest()[:16]


# ─────────────────────────────────────────────────────────────
# CORE SECURITY LOGIC — runs all 4 checks for one case
# ─────────────────────────────────────────────────────────────

def run_security(case: dict[str, Any],
                 raw: dict[str, Any],
                 sec_conn: sqlite3.Connection) -> dict[str, Any]:
    """
    Run all security checks for one case.
    Returns the security_result dict.
    """
    case_id   = case["case_id"]
    tenant_id = case["tenant_id"]
    subject   = raw.get("subject", "")
    body      = raw.get("body", "")
    att_names = [
        a.strip()
        for a in (raw.get("attachment_names") or "").split(",")
        if a.strip()
    ]

    verdict     = "CLEAN"
    body_toxic  = False
    att_results : list[dict[str, Any]] = []

    # ── ① Subject + Body Toxicity ─────────────────────────────
    toxic, reason = is_toxic(subject + " " + body)
    if toxic:
        body_toxic = True
        verdict    = "ESCALATE"
        log_event(sec_conn, case_id, "BODY_TOXIC", reason)
    else:
        log_event(sec_conn, case_id, "BODY_CLEAN",
                  f"subject+body checked ({len(body)} chars)")

    # ── ② ③ ④  Per-attachment pipeline ────────────────────────
    for filename in att_names:
        att_id = str(uuid.uuid4())
        att    = {
            "id":            att_id,
            "case_id":       case_id,
            "filename":      filename,
            "file_hash":     file_hash(filename),
            "declared_mime": _EXT_TO_MIME.get(
                                Path(filename).suffix.lower(),
                                "application/octet-stream"),
            "verified_mime": None,
            "status":        "PENDING",
            "blocked_reason": None,
            "markdown_text": None,
            "checked_at":    datetime.now(timezone.utc).isoformat(),
        }

        # ② MIME check
        verified_mime, mime_ok, mime_reason = check_mime(filename)
        att["verified_mime"] = verified_mime

        if not mime_ok:
            att["status"]         = "BLOCKED"
            att["blocked_reason"] = "TYPE_NOT_ALLOWED"
            verdict               = "ESCALATE"
            log_event(sec_conn, case_id, "ATTACHMENT_BLOCKED",
                      f"file={filename} reason=TYPE_NOT_ALLOWED detail={mime_reason}")
            att_results.append(att)
            continue   # no further checks on blocked attachment

        # ③ Antimalware
        clean, threat = scan_antimalware(filename, "")
        if not clean:
            att["status"]         = "BLOCKED"
            att["blocked_reason"] = "INFECTED"
            verdict               = "ESCALATE"
            log_event(sec_conn, case_id, "ATTACHMENT_BLOCKED",
                      f"file={filename} reason=INFECTED threat={threat}")
            att_results.append(att)
            continue

        # ④ Docling → Markdown
        markdown, parsed_ok = docling_to_markdown(filename, verified_mime)
        if not parsed_ok:
            att["status"]         = "BLOCKED"
            att["blocked_reason"] = "PARSING_FAILED"
            log_event(sec_conn, case_id, "ATTACHMENT_BLOCKED",
                      f"file={filename} reason=PARSING_FAILED")
            att_results.append(att)
            continue

        # ④ Markdown toxicity
        toxic_md, toxic_reason = is_toxic(markdown)
        if toxic_md:
            att["status"]         = "BLOCKED"
            att["blocked_reason"] = "TOXIC"
            verdict               = "ESCALATE"
            log_event(sec_conn, case_id, "ATTACHMENT_BLOCKED",
                      f"file={filename} reason=TOXIC detail={toxic_reason}")
            att_results.append(att)
            continue

        # All checks passed
        att["status"]        = "CLEAN"
        att["markdown_text"] = markdown
        log_event(sec_conn, case_id, "ATTACHMENT_CLEAN",
                  f"file={filename} mime={verified_mime} "
                  f"markdown_len={len(markdown)}")
        att_results.append(att)

    # ── Save attachment results ───────────────────────────────
    blocked_count = sum(1 for a in att_results if a["status"] == "BLOCKED")
    for att in att_results:
        sec_conn.execute("""
            INSERT INTO attachment_results VALUES (
                :id, :case_id, :filename, :file_hash,
                :declared_mime, :verified_mime, :status,
                :blocked_reason, :markdown_text, :checked_at
            )
        """, att)
    sec_conn.commit()

    # ── Save overall result ───────────────────────────────────
    result = {
        "case_id":          case_id,
        "tenant_id":        tenant_id,
        "verdict":          verdict,
        "body_toxic":       1 if body_toxic else 0,
        "attachment_count": len(att_names),
        "blocked_count":    blocked_count,
        "checked_at":       datetime.now(timezone.utc).isoformat(),
        "pipeline_step":    "SECURITY_DONE",
    }
    sec_conn.execute("""
        INSERT INTO security_results VALUES (
            :case_id, :tenant_id, :verdict, :body_toxic,
            :attachment_count, :blocked_count, :checked_at, :pipeline_step
        )
    """, result)
    sec_conn.commit()

    # ── Advance pipeline step in Ingestion DB ─────────────────
    # In production: Hatchet advances the step automatically.
    # In demo: we update the cases table directly.
    try:
        ing_conn = sqlite3.connect(str(INGESTION_DB_PATH),
                                   check_same_thread=False)
        next_step = "SECURITY_ESCALATE" if verdict == "ESCALATE" else "SECURITY_DONE"
        ing_conn.execute(
            "UPDATE cases SET pipeline_step = ? WHERE case_id = ?",
            (next_step, case_id),
        )
        ing_conn.commit()
        ing_conn.close()
    except Exception:
        pass  # demo — non-critical

    log_event(sec_conn, case_id, "SECURITY_DONE",
              f"verdict={verdict} attachments={len(att_names)} "
              f"blocked={blocked_count}")

    return result


# ─────────────────────────────────────────────────────────────
# POLLING LOOP
# ─────────────────────────────────────────────────────────────

def poll_and_check(sec_conn: sqlite3.Connection,
                   stop_event: threading.Event) -> None:
    print("  [Security] polling Ingestion DB every 5 seconds...")

    while not stop_event.is_set():
        ing_db  = open_db(INGESTION_DB_PATH)
        rec_db  = open_db(RECEPTION_DB_PATH)

        if ing_db is None or rec_db is None:
            print("  [Security] waiting for upstream DBs...")
            stop_event.wait(5)
            continue

        try:
            # Pick up cases that finished Ingestion and haven't been security-checked
            pending = ing_db.execute("""
                SELECT * FROM cases
                WHERE pipeline_step = 'INGESTION'
            """).fetchall()
        finally:
            ing_db.close()

        new_count = 0
        for row in pending:
            case = dict(row)
            if already_checked(sec_conn, case["case_id"]):
                continue

            # Join to Reception DB for subject + body + attachment_names
            try:
                raw_row = rec_db.execute(
                    "SELECT * FROM raw_messages WHERE message_id = ?",
                    (case["message_id"],),
                ).fetchone()
                raw = dict(raw_row) if raw_row else {}
            except Exception:
                raw = {}

            # Merge attachment_names from both sources
            # (Ingestion carries the filenames; Reception has the body)
            if not raw.get("attachment_names") and case.get("attachment_names"):
                raw["attachment_names"] = case["attachment_names"]

            result = run_security(case, raw, sec_conn)
            new_count += 1
            _print_result(case, result)

        if new_count:
            print(f"\n  [Security] ✓ {new_count} case(s) checked.\n")

        try:
            rec_db.close()
        except Exception:
            pass

        stop_event.wait(5)


# ─────────────────────────────────────────────────────────────
# TERMINAL PRINTER
# ─────────────────────────────────────────────────────────────

def _print_result(case: dict, result: dict) -> None:
    icon = "🚨" if result["verdict"] == "ESCALATE" else "✅"
    print(f"\n{'─'*60}")
    print(f"  {icon}  Security check  [{result['verdict']}]")
    print(f"  Case ID  : {result['case_id']}")
    print(f"  Source   : {case['source_type']}")
    print(f"  Body     : {'TOXIC ⚠' if result['body_toxic'] else 'clean'}")
    print(f"  Attachments: {result['attachment_count']} total, "
          f"{result['blocked_count']} blocked")
    if result["verdict"] == "ESCALATE":
        print(f"  → Outlook: PURPLE (escalate to security team)")
    else:
        print(f"  → Outlook: yellow (processing continues)")
        print(f"  → Next step: Phase 04 Privacy")
    print(f"\n  Payload for Privacy:")
    print(json.dumps({
        "case_id":    result["case_id"],
        "tenant_id":  result["tenant_id"],
        "verdict":    result["verdict"],
        "step":       "PRIVACY",
    }, indent=4))


# ─────────────────────────────────────────────────────────────
# WEB DASHBOARD  (port 8002)
# ─────────────────────────────────────────────────────────────

CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#f0f2f5;min-height:100vh;padding:32px 20px}
.page{max-width:960px;margin:0 auto}
.header{background:#1a2332;color:white;border-radius:8px;
        padding:20px 28px;margin-bottom:24px;
        display:flex;align-items:center;gap:16px}
.phase-badge{background:#e85d04;color:white;font-size:10px;font-weight:700;
             padding:3px 8px;border-radius:3px;letter-spacing:.06em;text-transform:uppercase}
.header h1{font-size:18px;font-weight:600}
.header p{font-size:12px;color:#8b949e;margin-top:2px}
.header-right{margin-left:auto;text-align:right}
.header-right span{font-size:11px;color:#8b949e}

.stats{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:24px}
.stat{background:white;border-radius:7px;padding:14px 18px;
      box-shadow:0 1px 3px rgba(0,0,0,.08)}
.stat-num{font-size:26px;font-weight:700;color:#1a2332}
.stat-label{font-size:10px;color:#9ca3af;margin-top:2px;
            text-transform:uppercase;letter-spacing:.05em}

.section-title{font-size:11px;font-weight:700;color:#9ca3af;
               text-transform:uppercase;letter-spacing:.07em;
               margin-bottom:10px;padding:0 2px}

.card{background:white;border-radius:7px;
      box-shadow:0 1px 3px rgba(0,0,0,.08);overflow:hidden;margin-bottom:20px}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;padding:10px 14px;background:#f8f9fb;color:#6b7280;
   font-size:11px;font-weight:600;text-transform:uppercase;
   letter-spacing:.05em;border-bottom:1px solid #e5e7eb}
td{padding:11px 14px;border-bottom:1px solid #f3f4f6;
   color:#374151;vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:#fafafa}

.pill{display:inline-block;font-size:10px;font-weight:700;
      padding:2px 8px;border-radius:10px;text-transform:uppercase}
.clean    {background:#d1fae5;color:#065f46}
.escalate {background:#fee2e2;color:#991b1b}
.blocked  {background:#fef3c7;color:#92400e}
.pending  {background:#f3f4f6;color:#6b7280}

.step-row{display:flex;align-items:flex-start;gap:12px;padding:12px 16px;
          border-bottom:1px solid #f3f4f6}
.step-row:last-child{border-bottom:none}
.step-icon{font-size:18px;flex-shrink:0;margin-top:1px}
.step-content{flex:1;min-width:0}
.step-title{font-size:13px;font-weight:600;color:#1f2937}
.step-desc{font-size:11px;color:#6b7280;margin-top:2px;line-height:1.5}
.step-result{font-size:11px;font-weight:600;margin-top:4px}
.result-clean   {color:#065f46}
.result-blocked {color:#dc2626}

.att-row{display:flex;align-items:center;gap:8px;
         padding:6px 14px;border-bottom:1px solid #f9fafb;font-size:12px}
.att-row:last-child{border-bottom:none}
.att-name{flex:1;font-family:monospace;font-size:11px;color:#374151}
.att-mime{color:#9ca3af;font-size:10px;font-family:monospace}

.log-panel{background:#1a2332;border-radius:7px;
           padding:16px 20px;max-height:260px;overflow-y:auto}
.log-line{font-family:'Menlo','Courier New',monospace;font-size:11px;
          color:#8b949e;padding:2px 0;line-height:1.6}
.log-line .ts{color:#484f58}
.ev-body-clean   {color:#2ea043}
.ev-body-toxic   {color:#f85149}
.ev-att-clean    {color:#388bfd}
.ev-att-blocked  {color:#d29922}
.ev-done         {color:#a371f7}
.ev-escalate     {color:#f85149;font-weight:700}

.refresh-note{font-size:11px;color:#9ca3af;text-align:center;margin-top:8px}
.empty-note{text-align:center;color:#9ca3af;padding:24px;font-size:13px}
"""


def _verdict_pill(verdict: str) -> str:
    cls = "clean" if verdict == "CLEAN" else "escalate"
    return f'<span class="pill {cls}">{verdict}</span>'


def _src_label(src: str) -> str:
    m = {"DIRECT_EMAIL": "Email", "STAFF_FORWARD": "Staff",
         "POSTAL_SCAN": "Scan", "WEB_FORM": "Web Form"}
    return m.get(src, src)


def render_dashboard(sec_conn: sqlite3.Connection) -> str:
    results = sec_conn.execute(
        "SELECT * FROM security_results ORDER BY checked_at DESC"
    ).fetchall()
    results = [dict(r) for r in results]

    att_rows = sec_conn.execute(
        "SELECT * FROM attachment_results ORDER BY checked_at DESC"
    ).fetchall()
    att_rows = [dict(r) for r in att_rows]

    logs = sec_conn.execute(
        "SELECT * FROM security_log ORDER BY ts DESC LIMIT 80"
    ).fetchall()
    logs = [dict(r) for r in logs]

    # Stats
    total     = len(results)
    clean     = sum(1 for r in results if r["verdict"] == "CLEAN")
    escalated = sum(1 for r in results if r["verdict"] == "ESCALATE")
    atts      = len(att_rows)
    blocked   = sum(1 for a in att_rows if a["status"] == "BLOCKED")

    stats_html = f"""<div class="stats">
      <div class="stat"><div class="stat-num">{total}</div>
        <div class="stat-label">Cases Checked</div></div>
      <div class="stat"><div class="stat-num" style="color:#065f46">{clean}</div>
        <div class="stat-label">Clean</div></div>
      <div class="stat"><div class="stat-num" style="color:#dc2626">{escalated}</div>
        <div class="stat-label">Escalated</div></div>
      <div class="stat"><div class="stat-num">{atts}</div>
        <div class="stat-label">Attachments</div></div>
      <div class="stat"><div class="stat-num" style="color:#d29922">{blocked}</div>
        <div class="stat-label">Blocked</div></div>
    </div>"""

    # Pull case info from ingestion DB for source_type display
    case_meta: dict[str, dict] = {}
    ing_db = open_db(INGESTION_DB_PATH)
    if ing_db:
        try:
            for row in ing_db.execute("SELECT case_id, source_type, subject, pipeline_step FROM cases"):
                case_meta[row["case_id"]] = dict(row)
        finally:
            ing_db.close()

    # Results table
    table_rows = ""
    if not results:
        table_rows = '<tr><td colspan="6" class="empty-note">No cases checked yet — waiting for Ingestion...</td></tr>'
    for r in results:
        meta  = case_meta.get(r["case_id"], {})
        src   = _src_label(meta.get("source_type", ""))
        subj  = _html.escape((meta.get("subject") or "")[:45])
        att_s = f"{r['attachment_count']} att"
        if r["blocked_count"]:
            att_s += f' <span class="pill blocked">{r["blocked_count"]} blocked</span>'
        table_rows += f"""<tr>
          <td><code style="font-size:11px;color:#6b7280">{r['case_id'][:8]}…</code></td>
          <td>{src}</td>
          <td title="{_html.escape(meta.get('subject',''))}">{subj}</td>
          <td>{"🔴 toxic" if r["body_toxic"] else "✅ clean"}</td>
          <td>{att_s}</td>
          <td>{_verdict_pill(r['verdict'])}</td>
        </tr>"""

    # Security checks breakdown (most recent case)
    checks_html = ""
    if results:
        latest = results[0]
        cid    = latest["case_id"]
        meta   = case_meta.get(cid, {})

        checks = [
            ("🛡", "Subject + Body Toxicity (LlamaGuard)",
             "Scanned full text for harmful/toxic content",
             "🔴 TOXIC — escalated" if latest["body_toxic"]
             else "✅ Clean — no toxic content detected",
             "result-blocked" if latest["body_toxic"] else "result-clean"),
        ]

        # Attachment checks
        case_atts = [a for a in att_rows if a["case_id"] == cid]
        if case_atts:
            for att in case_atts:
                reason = att.get("blocked_reason") or ""
                if att["status"] == "CLEAN":
                    desc = (f"MIME: {att['verified_mime']} · "
                            f"ClamAV: clean · "
                            f"Docling: {len(att.get('markdown_text') or '')} chars MD")
                    res_txt, res_cls = "✅ Clean — all checks passed", "result-clean"
                else:
                    desc    = f"Blocked at: {reason}"
                    res_txt = f"🔴 BLOCKED — {reason}"
                    res_cls = "result-blocked"

                checks += [
                    ("📎", f"Attachment: {_html.escape(att['filename'])}",
                     desc, res_txt, res_cls),
                ]
        elif not latest["attachment_count"]:
            checks += [
                ("📎", "Attachments",
                 "No attachments in this case",
                 "— (skipped)", "result-clean"),
            ]

        steps_html = ""
        for icon, title, desc, res_text, res_cls in checks:
            steps_html += f"""<div class="step-row">
              <div class="step-icon">{icon}</div>
              <div class="step-content">
                <div class="step-title">{title}</div>
                <div class="step-desc">{desc}</div>
                <div class="step-result {res_cls}">{res_text}</div>
              </div>
            </div>"""

        checks_html = f"""
        <div class="section-title" style="margin-top:24px">
          Security checks — case {cid[:8]}…
          ({_src_label(meta.get('source_type',''))})
        </div>
        <div class="card">{steps_html}</div>"""

    # Attachment table
    att_table = ""
    if att_rows:
        att_table_rows = ""
        for a in att_rows[:15]:
            s_pill = (
                '<span class="pill clean">CLEAN</span>'
                if a["status"] == "CLEAN"
                else f'<span class="pill blocked">{a.get("blocked_reason","BLOCKED")}</span>'
            )
            att_table_rows += f"""<tr>
              <td><code style="font-size:11px">{a['case_id'][:8]}…</code></td>
              <td class="att-name">{_html.escape(a['filename'])}</td>
              <td class="att-mime">{a.get('verified_mime','')}</td>
              <td>{s_pill}</td>
            </tr>"""
        att_table = f"""
        <div class="section-title" style="margin-top:24px">Attachments</div>
        <div class="card">
          <table>
            <thead><tr><th>Case</th><th>File</th><th>MIME</th><th>Status</th></tr></thead>
            <tbody>{att_table_rows}</tbody>
          </table>
        </div>"""

    # Log
    ev_css = {
        "BODY_CLEAN":        "ev-body-clean",
        "BODY_TOXIC":        "ev-body-toxic",
        "ATTACHMENT_CLEAN":  "ev-att-clean",
        "ATTACHMENT_BLOCKED": "ev-att-blocked",
        "SECURITY_DONE":     "ev-done",
    }
    log_lines = ""
    for lg in logs:
        ev  = lg["event"]
        css = ev_css.get(ev, "")
        if ev == "SECURITY_DONE" and "ESCALATE" in (lg.get("detail") or ""):
            css = "ev-escalate"
        ts  = lg["ts"][11:19]
        det = _html.escape((lg.get("detail") or "")[:70])
        log_lines += (
            f'<div class="log-line">'
            f'<span class="ts">{ts}</span>  '
            f'<span class="{css}">{ev}</span>  {det}'
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
<title>Phase 03 — Security</title>
<style>{CSS}</style>
</head>
<body>
<div class="page">
  <div class="header">
    <div>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
        <span class="phase-badge">Phase 03</span>
        <h1>Security</h1>
      </div>
      <p>Toxicity · MIME check · Antimalware · Docling → Markdown</p>
    </div>
    <div class="header-right">
      <span>Polling Ingestion every 5s</span><br>
      <span style="color:#2ea043;font-weight:600">● Live</span>
    </div>
  </div>

  {stats_html}

  <div class="section-title">Results per case</div>
  <div class="card">
    <table>
      <thead>
        <tr>
          <th>Case ID</th><th>Source</th><th>Subject</th>
          <th>Body</th><th>Attachments</th><th>Verdict</th>
        </tr>
      </thead>
      <tbody>{table_rows}</tbody>
    </table>
  </div>

  {checks_html}
  {att_table}

  <div class="section-title">Audit Log</div>
  <div class="log-panel">{log_lines}</div>
  <p class="refresh-note">Auto-refreshes every 5 seconds</p>
</div>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────
# HTTP SERVER
# ─────────────────────────────────────────────────────────────

class SecurityDashboardHandler(http.server.BaseHTTPRequestHandler):
    sec_conn: sqlite3.Connection = None  # type: ignore

    def log_message(self, *args: object) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/favicon.ico":
            self.send_response(204); self.end_headers(); return
        page = render_dashboard(self.sec_conn)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode())


def make_handler(conn: sqlite3.Connection):
    class H(SecurityDashboardHandler):
        sec_conn = conn
    return H


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "═"*60)
    print("  CITIZEN PLATFORM — Phase 03: Security  (DEMO)")
    print("═"*60)

    sec_conn = init_security_db()
    print(f"\n  ✓  Security DB : {SECURITY_DB_PATH}")
    print(f"  ✓  Reading from: {INGESTION_DB_PATH}")

    stop_event = threading.Event()
    poller = threading.Thread(
        target=poll_and_check,
        args=(sec_conn, stop_event),
        daemon=True,
    )
    poller.start()

    handler    = make_handler(sec_conn)
    server     = http.server.HTTPServer(("", PORT), handler)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    print(f"\n  ✓  Dashboard: http://localhost:{PORT}")
    print(f"\n  Run order:")
    print(f"    Terminal 1 → python3 demo/reception.py   (port 8000)")
    print(f"    Terminal 2 → python3 demo/ingestion.py   (port 8001)")
    print(f"    Terminal 3 → python3 demo/security.py    (port 8002)")
    print(f"\n  Press Ctrl-C to stop.\n")
    print("─"*60)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n\n  Stopping security...")
        stop_event.set()
        server.shutdown()
        print("  Done.\n")


if __name__ == "__main__":
    main()
