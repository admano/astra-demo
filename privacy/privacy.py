"""
demo/privacy.py
---------------
Phase 04: Privacy  (DEMO VERSION — Presidio engine)

Reads cases at pipeline_step=SECURITY_DONE, anonymises all PII
from subject, body, and attachment Markdowns using the production-grade
privacy engine in the `privacy/` package (Presidio + Swiss recognisers +
optional HuggingFace NER + PseudonymVault), stores the token map and
risk score in SQLite, then advances each case to pipeline_step=PRIVACY_DONE.

What this phase does (spec order):
  ① Detect + pseudonymize PII in subject    → anonymised subject
  ② Detect + pseudonymize PII in body       → anonymised body
  ③ Detect + pseudonymize PII in attachment MDs
  ④ Store PII token map                     → pii_tokens table
  ⑤ Risk check (residual leak gate)        → pipeline_status

Engine:
  privacy/detector.py        — Presidio + Swiss recognisers + HuggingFace NER
  privacy/anonymizer.py      — context-preserving pseudonymization
  privacy/vault.py           — PseudonymVault (session-scoped, reversible)
  privacy/scorer.py          — residual risk scoring
  privacy/pipeline.py        — orchestrator (run_privacy_pipeline)

The LLM (Analysis, Response) will ONLY ever see anonymised text.
PII is reconstructed at Dispatch via vault.deanonymize_text(session_id, ai_text).

Run:
    python3 demo/reception.py    ← port 8000
    python3 demo/ingestion.py    ← port 8001
    python3 demo/security.py     ← port 8002
    python3 demo/privacy.py      ← port 8003

Dashboard: http://localhost:8003
"""

from __future__ import annotations

import html as _html
import http.server
import json
import logging
import re
import sqlite3
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from string import Template
from typing import Any

# ── Make the privacy/ package importable when running as a script ──────────
# __file__ is  <demo_root>/privacy/privacy.py
# privacy/     is  <demo_root>/privacy/
# demo root    is  <demo_root>/
_PRIVACY_DIR = Path(__file__).parent          # <demo_root>/privacy/
_DEMO_DIR    = _PRIVACY_DIR.parent            # <demo_root>/
if str(_DEMO_DIR) not in sys.path:
    sys.path.insert(0, str(_DEMO_DIR))

# ── Privacy engine (Presidio-based) ────────────────────────────────────────
# Imported via the qualified `privacy.*` path (not e.g. `from vault import
# vault`) on purpose — an unqualified import here would make Python load
# each submodule a second time under a different module name, creating a
# second PseudonymVault singleton that never sees the sessions pipeline.py
# actually wrote pseudonyms into.
from privacy.pipeline import run_privacy_pipeline
from privacy.models   import PrivacyRunRequest
from privacy.vault    import vault as _vault

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────

DEMO_DIR      = _DEMO_DIR                  # <demo_root>/
TEMPLATES_DIR = DEMO_DIR / "templates"     # <demo_root>/templates/
DB_PATH       = DEMO_DIR / "demo_db" / "demo_pipeline.db"   # shared by every phase
PORT          = 8003

# k-anonymity threshold (configurable per tenant in production)
K_ANONYMITY_MIN = 3

# No downstream phase exists yet (Phase 05 Analysis isn't built). Once it is,
# add a _notify_next_phase() helper here (see reception.py/ingestion.py for
# the pattern) and call it after a case clears PRIVACY_DONE.


# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────

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


def init_privacy_db() -> sqlite3.Connection:
    """
    Tables:
      privacy_results  — one row per case, overall outcome + risk score
      pii_tokens       — reversible pseudonym map (tag → original value)
      attachment_anon  — anonymised attachment Markdowns
      privacy_log      — append-only audit events
    """
    conn = _connect_shared_db(DB_PATH)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS privacy_results (
            case_id          TEXT PRIMARY KEY,
            tenant_id        TEXT NOT NULL,
            tokens_found     INTEGER NOT NULL DEFAULT 0,
            subject_anon     TEXT,
            body_anon        TEXT,
            k_anon_ok        INTEGER NOT NULL DEFAULT 1,
            risk_score       REAL    NOT NULL DEFAULT 0.0,
            pipeline_status  TEXT    NOT NULL DEFAULT 'safe',
            vault_session_id TEXT,
            processed_at     TEXT NOT NULL,
            pipeline_step    TEXT NOT NULL DEFAULT 'PRIVACY_DONE'
        );

        CREATE TABLE IF NOT EXISTS pii_tokens (
            id        TEXT PRIMARY KEY,
            case_id   TEXT NOT NULL,
            tag       TEXT NOT NULL,   -- pseudonym (e.g. "Person_A1")
            pii_type  TEXT NOT NULL,   -- entity type (e.g. "FULL_NAME")
            value     TEXT NOT NULL    -- original real value
        );

        CREATE TABLE IF NOT EXISTS attachment_anon (
            id            TEXT PRIMARY KEY,
            case_id       TEXT NOT NULL,
            filename      TEXT NOT NULL,
            markdown_anon TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS privacy_log (
            id      TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            event   TEXT NOT NULL,
            detail  TEXT,
            ts      TEXT NOT NULL
        );
    """)
    conn.commit()
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def already_processed(priv_conn: sqlite3.Connection, case_id: str) -> bool:
    return priv_conn.execute(
        "SELECT 1 FROM privacy_results WHERE case_id = ?", (case_id,)
    ).fetchone() is not None


def log_event(priv_conn: sqlite3.Connection,
              case_id: str, event: str, detail: str = "") -> None:
    priv_conn.execute(
        "INSERT INTO privacy_log VALUES (?,?,?,?,?)",
        (str(uuid.uuid4()), case_id, event, detail,
         datetime.now(timezone.utc).isoformat()),
    )
    priv_conn.commit()


# ─────────────────────────────────────────────────────────────
# CORE PRIVACY LOGIC  (delegates to privacy/ engine)
# ─────────────────────────────────────────────────────────────

def run_privacy(case: dict[str, Any],
                raw: dict[str, Any],
                sec_att_rows: list[dict[str, Any]],
                priv_conn: sqlite3.Connection) -> dict[str, Any]:
    """
    Anonymise all PII surfaces for one case using the Presidio engine.

    Surfaces processed (shared vault session → consistent pseudonyms):
      1. Subject
      2. Body
      3. Each clean attachment Markdown (from Security)

    Returns the privacy_result dict (mirrors old contract + new fields).
    """
    case_id   = case["case_id"]
    tenant_id = case["tenant_id"]

    subject = raw.get("subject", "") or ""
    body    = raw.get("body",    "") or ""

    # ── ① Anonymise subject ───────────────────────────────────────────────
    subj_req    = PrivacyRunRequest(text=subject, surface="subject")
    subj_result = run_privacy_pipeline(case_id=case_id, request=subj_req)
    subject_anon   = subj_result["anonymized_text"]
    session_id     = subj_result["session_id"]   # reuse across surfaces

    log_event(priv_conn, case_id, "SUBJECT_ANONYMISED",
              f"engine=presidio tokens={len(subj_result['anonymization_actions'])}")

    # ── ② Anonymise body (same vault session) ────────────────────────────
    body_req    = PrivacyRunRequest(text=body, surface="body")
    body_result = run_privacy_pipeline(case_id, body_req, session_id=session_id)
    body_anon   = body_result["anonymized_text"]

    log_event(priv_conn, case_id, "BODY_ANONYMISED",
              f"engine=presidio tokens={len(body_result['anonymization_actions'])}")

    # ── ③ Anonymise attachment Markdowns ──────────────────────────────────
    anon_attachments: list[dict] = []
    for att in sec_att_rows:
        md_raw  = att.get("markdown_text") or ""
        att_req = PrivacyRunRequest(text=md_raw, surface="attachment")
        att_res = run_privacy_pipeline(case_id, att_req, session_id=session_id)
        anon_attachments.append({
            "id":           str(uuid.uuid4()),
            "case_id":      case_id,
            "filename":     att["filename"],
            "markdown_anon": att_res["anonymized_text"],
        })
        log_event(priv_conn, case_id, "ATTACHMENT_ANONYMISED",
                  f"file={att['filename']} "
                  f"tokens={len(att_res['anonymization_actions'])}")

    # ── ④ Collect all pseudonyms across surfaces from the vault ──────────
    vault_mapping = _vault.get_session_mapping(session_id)
    # vault_mapping: { pseudonym → original_value }
    # We need pii_type — recover it from the combined action lists
    all_actions = (
        subj_result["anonymization_actions"]
        + body_result["anonymization_actions"]
    )
    # Build pseudonym → entity_type from the action lists
    pseudonym_to_type: dict[str, str] = {
        a["pseudonym"]: a["entity_type"] for a in all_actions
    }
    # Deduplicate: one row per unique pseudonym
    seen_pseudonyms: set[str] = set()
    total_tokens = 0
    for pseudonym, original_value in vault_mapping.items():
        if pseudonym in seen_pseudonyms:
            continue
        seen_pseudonyms.add(pseudonym)
        pii_type = pseudonym_to_type.get(pseudonym, "UNKNOWN")
        priv_conn.execute(
            "INSERT INTO pii_tokens VALUES (?,?,?,?,?)",
            (str(uuid.uuid4()), case_id, pseudonym, pii_type, original_value),
        )
        total_tokens += 1
    priv_conn.commit()

    log_event(priv_conn, case_id, "TOKENS_STORED",
              f"count={total_tokens} session={session_id}")

    # ── ⑤ Risk check (residual leak gate) ────────────────────────────────
    # Use the highest risk score across all surfaces
    risk_score = max(
        subj_result.get("risk_score", 0.0),
        body_result.get("risk_score", 0.0),
    )
    pipeline_status = subj_result.get("pipeline_status", "safe")
    if body_result.get("pipeline_status") == "blocked":
        pipeline_status = "blocked"
    elif body_result.get("pipeline_status") == "escalated" and pipeline_status == "safe":
        pipeline_status = "escalated"

    k_ok = pipeline_status != "blocked"

    if pipeline_status == "blocked":
        log_event(priv_conn, case_id, "K_ANONYMITY_FAIL",
                  f"risk_score={risk_score:.3f} status=blocked")
    else:
        log_event(priv_conn, case_id, "K_ANONYMITY_OK",
                  f"risk_score={risk_score:.3f} status={pipeline_status}")

    # ── Save anonymised attachments ───────────────────────────────────────
    for att_anon in anon_attachments:
        priv_conn.execute(
            "INSERT INTO attachment_anon VALUES (?,?,?,?)",
            (att_anon["id"], att_anon["case_id"],
             att_anon["filename"], att_anon["markdown_anon"]),
        )
    priv_conn.commit()

    # ── Save privacy result ───────────────────────────────────────────────
    result: dict[str, Any] = {
        "case_id":          case_id,
        "tenant_id":        tenant_id,
        "tokens_found":     total_tokens,
        "subject_anon":     subject_anon,
        "body_anon":        body_anon,
        "k_anon_ok":        1 if k_ok else 0,
        "risk_score":       risk_score,
        "pipeline_status":  pipeline_status,
        "vault_session_id": session_id,
        "processed_at":     datetime.now(timezone.utc).isoformat(),
        "pipeline_step":    "PRIVACY_DONE",
    }
    priv_conn.execute("""
        INSERT INTO privacy_results VALUES (
            :case_id, :tenant_id, :tokens_found,
            :subject_anon, :body_anon, :k_anon_ok,
            :risk_score, :pipeline_status, :vault_session_id,
            :processed_at, :pipeline_step
        )
    """, result)
    priv_conn.commit()

    # ── Advance pipeline step (shared cases table) ────────────────────────
    next_step = "PRIVACY_BLOCKED" if pipeline_status == "blocked" else "PRIVACY_DONE"
    priv_conn.execute("UPDATE cases SET pipeline_step=? WHERE case_id=?",
                      (next_step, case_id))
    priv_conn.commit()

    log_event(priv_conn, case_id, "PRIVACY_DONE",
              f"tokens={total_tokens} risk={risk_score:.3f} status={pipeline_status}")
    return result




# ─────────────────────────────────────────────────────────────
# PROCESSING PASS
# Reads pending cases from the shared DB and anonymises them.
# Triggered immediately via HTTP /notify, and on a 5s timer as a
# fallback in case a notify call ever gets missed.
# ─────────────────────────────────────────────────────────────

_PROCESS_LOCK = threading.Lock()


def process_pending_privacy(priv_conn: sqlite3.Connection) -> int:
    """Anonymise every pending case. Returns how many were processed."""
    with _PROCESS_LOCK:
        if not _table_exists(priv_conn, "cases"):
            return 0

        pending = priv_conn.execute(
            "SELECT * FROM cases WHERE pipeline_step='SECURITY_DONE'"
        ).fetchall()

        new_count = 0
        for row in pending:
            case = dict(row)
            if already_processed(priv_conn, case["case_id"]):
                continue

            raw_row = priv_conn.execute(
                "SELECT * FROM raw_messages WHERE message_id=?",
                (case["message_id"],),
            ).fetchone()
            raw = dict(raw_row) if raw_row else {}

            att_rows = priv_conn.execute(
                "SELECT * FROM attachment_results WHERE case_id=? AND status='CLEAN'",
                (case["case_id"],),
            ).fetchall()
            sec_atts = [dict(r) for r in att_rows]

            result = run_privacy(case, raw, sec_atts, priv_conn)
            new_count += 1
            _print_result(case, result, priv_conn)

        if new_count:
            print(f"\n  [Privacy] ✓ {new_count} case(s) anonymised.\n")

        return new_count


def poll_and_anonymise(priv_conn: sqlite3.Connection,
                       stop_event: threading.Event) -> None:
    """Background thread: re-run the processing pass every 5s as a fallback."""
    print("  [Privacy] polling every 5 seconds (HTTP push is the fast path)...")

    while not stop_event.is_set():
        process_pending_privacy(priv_conn)
        stop_event.wait(5)


# ─────────────────────────────────────────────────────────────
# TERMINAL PRINTER
# ─────────────────────────────────────────────────────────────

def _print_result(case: dict, result: dict,
                  priv_conn: sqlite3.Connection) -> None:
    tokens = priv_conn.execute(
        "SELECT tag, pii_type, value FROM pii_tokens WHERE case_id=?",
        (case["case_id"],),
    ).fetchall()

    status_icon = {"safe": "✅", "escalated": "⚠️", "blocked": "🚨"}.get(
        result.get("pipeline_status", "safe"), "✅"
    )
    print(f"\n{'─'*60}")
    print(f"  🔒  Privacy anonymised  [{result['tokens_found']} tokens]  "
          f"risk={result.get('risk_score', 0):.3f} {status_icon} {result.get('pipeline_status','safe').upper()}")
    print(f"  Case ID  : {result['case_id']}")
    print(f"  Source   : {case['source_type']}")
    print(f"  Session  : {result.get('vault_session_id', '—')}")
    print(f"\n  Subject (anonymised):")
    print(f"    {result['subject_anon']}")
    print(f"\n  Body (first 200 chars, anonymised):")
    body_preview = (result.get("body_anon") or "")[:200].replace("\n", " ")
    print(f"    {body_preview}")
    if tokens:
        print(f"\n  PII pseudonyms created ({len(tokens)}):")
        for t in tokens:
            print(f"    {t['tag']:25} {t['pii_type']:15}  '{t['value'][:40]}'")
    print(f"\n  → Next step: Phase 05 Analysis")
    print(json.dumps({
        "case_id":      result["case_id"],
        "tenant_id":    result["tenant_id"],
        "tokens_found": result["tokens_found"],
        "risk_score":   result.get("risk_score", 0.0),
        "step":         "ANALYSIS",
    }, indent=4))


# ─────────────────────────────────────────────────────────────
# TEMPLATE HELPERS
# ─────────────────────────────────────────────────────────────

def _load_template(name: str) -> Template:
    return Template((TEMPLATES_DIR / name).read_text(encoding="utf-8"))


# ─────────────────────────────────────────────────────────────
# TYPE → CSS MAP  (covers both old regex types and new Presidio types)
# ─────────────────────────────────────────────────────────────

_TYPE_CSS: dict[str, str] = {
    # Presidio / new engine
    "FULL_NAME":    "tp-person",
    "EMAIL":        "tp-email",
    "PHONE":        "tp-phone",
    "CH_PHONE":     "tp-phone",
    "CH_AHV":       "tp-ahv",
    "CH_IBAN":      "tp-iban",
    "IBAN":         "tp-iban",
    "CH_UID":       "tp-zip",
    "IP_ADDRESS":   "tp-date",
    "DATE_TIME":    "tp-date",
    "LOCATION":     "tp-address",
    "CREDIT_CARD":  "tp-iban",
    "URL":          "tp-date",
    # Legacy regex types (backwards compat)
    "PERSON":   "tp-person",
    "ADDRESS":  "tp-address",
    "ZIP":      "tp-zip",
    "AHV":      "tp-ahv",
    "DATE":     "tp-date",
}


def _highlight_tags(text: str) -> str:
    """Wrap pseudonym placeholders in a highlight span for display."""
    escaped = _html.escape(text or "")
    # Match context-preserving pseudonyms like Person_A1, AHV-ID-B3, etc.
    return re.sub(
        r"([A-Za-z][A-Za-z0-9\-_]+_[A-Z][1-9]|"
        r"user_[a-z][1-9]@redacted\.astra|"
        r"https://url-[A-Z][1-9]\.redacted)",
        r'<span class="highlight">\1</span>',
        escaped,
    )


# ─────────────────────────────────────────────────────────────
# WEB DASHBOARD (port 8003)
# ─────────────────────────────────────────────────────────────

def render_dashboard(priv_conn: sqlite3.Connection) -> str:
    results = priv_conn.execute(
        "SELECT * FROM privacy_results ORDER BY processed_at DESC"
    ).fetchall()
    results = [dict(r) for r in results]

    all_tokens = priv_conn.execute(
        "SELECT * FROM pii_tokens ORDER BY case_id, pii_type, tag"
    ).fetchall()
    all_tokens = [dict(r) for r in all_tokens]

    logs = priv_conn.execute(
        "SELECT * FROM privacy_log ORDER BY ts DESC LIMIT 80"
    ).fetchall()
    logs = [dict(r) for r in logs]

    # Pull case metadata from the shared cases table
    case_meta: dict[str, dict] = {}
    if _table_exists(priv_conn, "cases"):
        for r in priv_conn.execute(
            "SELECT case_id, source_type, subject, pipeline_step FROM cases"
        ):
            case_meta[r["case_id"]] = dict(r)

    # Stats
    total        = len(results)
    total_tokens = len(all_tokens)
    types_found  = len({t["pii_type"] for t in all_tokens})
    k_ok         = sum(1 for r in results if r["k_anon_ok"])
    avg_risk     = (
        round(sum(r.get("risk_score", 0.0) for r in results) / total, 3)
        if total else 0.0
    )

    stats_html = f"""<div class="stats">
      <div class="stat"><div class="stat-num">{total}</div>
        <div class="stat-label">Cases Anonymised</div></div>
      <div class="stat"><div class="stat-num" style="color:#7c3aed">{total_tokens}</div>
        <div class="stat-label">PII Tokens</div></div>
      <div class="stat"><div class="stat-num">{types_found}</div>
        <div class="stat-label">PII Types Found</div></div>
      <div class="stat"><div class="stat-num" style="color:#065f46">{k_ok}/{total}</div>
        <div class="stat-label">k-Anon Passed</div></div>
      <div class="stat"><div class="stat-num" style="color:{'#dc2626' if avg_risk > 0.1 else '#065f46'}">{avg_risk:.3f}</div>
        <div class="stat-label">Avg Risk Score</div></div>
    </div>"""

    # Results table
    src_labels = {"DIRECT_EMAIL": "Email", "STAFF_FORWARD": "Staff",
                  "POSTAL_SCAN": "Scan", "WEB_FORM": "Web Form"}
    tbl_rows = ""
    if not results:
        tbl_rows = '<tr><td colspan="7" class="empty">Waiting for Security to complete...</td></tr>'
    for r in results:
        meta   = case_meta.get(r["case_id"], {})
        src    = src_labels.get(meta.get("source_type", ""), "")
        subj   = _html.escape((meta.get("subject") or "")[:40])
        n_tok  = sum(1 for t in all_tokens if t["case_id"] == r["case_id"])
        type_counts: dict[str, int] = {}
        for t in all_tokens:
            if t["case_id"] == r["case_id"]:
                type_counts[t["pii_type"]] = type_counts.get(t["pii_type"], 0) + 1
        pills = " ".join(
            f'<span class="type-pill {_TYPE_CSS.get(tp, "")}">{tp}:{cnt}</span>'
            for tp, cnt in sorted(type_counts.items())
        )
        k_icon = "✅" if r["k_anon_ok"] else "⚠️"
        risk   = r.get("risk_score", 0.0)
        status = r.get("pipeline_status", "safe")
        risk_color = {"safe": "#065f46", "escalated": "#92400e", "blocked": "#dc2626"}.get(status, "#374151")
        case_data = json.dumps({
            "case_id":    r["case_id"],
            "subject":    meta.get("subject", ""),
            "src":        src,
            "body_anon":  r.get("body_anon", ""),
            "risk":       risk,
            "status":     status,
            "session":    r.get("vault_session_id", ""),
        })
        tbl_rows += f"""<tr class="case-row" data-case='{case_data.replace("'", "&#39;")}' style="cursor:pointer">
          <td><span class="mono">{r['case_id'][:8]}…</span></td>
          <td>{src}</td>
          <td title="{_html.escape(meta.get('subject',''))}">{subj}</td>
          <td><strong>{n_tok}</strong> {pills}</td>
          <td>{k_icon}</td>
          <td style="color:{risk_color};font-weight:600">{risk:.3f} {status}</td>
          <td><span class="mono" style="font-size:10px">{r.get('vault_session_id','')[:12]}</span></td>
        </tr>"""

    diff_html = ""

    # Audit log
    ev_css = {
        "SUBJECT_ANONYMISED":    "ev-subj",
        "BODY_ANONYMISED":       "ev-body",
        "ATTACHMENT_ANONYMISED": "ev-att",
        "TOKENS_STORED":         "ev-tok",
        "K_ANONYMITY_OK":        "ev-kanon",
        "K_ANONYMITY_FAIL":      "ev-done",
        "PRIVACY_DONE":          "ev-done",
    }
    log_lines = ""
    for lg in logs:
        ev  = lg["event"]
        css = ev_css.get(ev, "")
        ts  = lg["ts"][11:19]
        det = _html.escape((lg.get("detail") or "")[:80])
        log_lines += (
            f'<div class="log-line"><span class="ts">{ts}</span>  '
            f'<span class="{css}">{ev}</span>  {det}</div>\n'
        )
    if not log_lines:
        log_lines = '<div class="log-line" style="color:#484f58">— no events yet —</div>'

    return _load_template("privacy_dashboard.html").substitute(
        stats=stats_html,
        tbl_rows=tbl_rows,
        diff_html=diff_html,
        log_lines=log_lines,
    )


# ─────────────────────────────────────────────────────────────
# HTTP SERVER
# ─────────────────────────────────────────────────────────────

class PrivacyDashboardHandler(http.server.BaseHTTPRequestHandler):
    priv_conn: sqlite3.Connection = None  # type: ignore

    def log_message(self, *args: object) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/favicon.ico":
            self.send_response(204); self.end_headers(); return
        if self.path == "/data":
            self._serve_data()
            return
        if self.path.startswith("/case/"):
            self._serve_case(self.path[6:])
            return
        page = render_dashboard(self.priv_conn)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode())

    def do_POST(self) -> None:
        if self.path != "/notify":
            self.send_response(404); self.end_headers(); return
        process_pending_privacy(self.priv_conn)
        self.send_response(204)
        self.end_headers()

    def _serve_data(self) -> None:
        payload = render_data_json(self.priv_conn).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _serve_case(self, case_id: str) -> None:
        """Return JSON with original body, anonymised body, and token map for one case."""
        import json as _json
        # Original body — same connection, just a different table now
        orig_body = ""
        mid_row = self.priv_conn.execute(
            "SELECT message_id FROM cases WHERE case_id=?", (case_id,)
        ).fetchone()
        if mid_row:
            row = self.priv_conn.execute(
                "SELECT body FROM raw_messages WHERE message_id=?",
                (mid_row["message_id"],),
            ).fetchone()
            if row:
                orig_body = row["body"] or ""

        # Anonymised result
        result_row = self.priv_conn.execute(
            "SELECT * FROM privacy_results WHERE case_id=?", (case_id,)
        ).fetchone()
        result = dict(result_row) if result_row else {}

        # Token map
        tokens = self.priv_conn.execute(
            "SELECT tag, pii_type, value FROM pii_tokens WHERE case_id=? ORDER BY pii_type, tag",
            (case_id,),
        ).fetchall()
        token_list = [{"tag": t["tag"], "pii_type": t["pii_type"], "value": t["value"]}
                      for t in tokens]

        payload = _json.dumps({
            "case_id":      case_id,
            "orig_body":    orig_body,
            "body_anon":    result.get("body_anon", ""),
            "subject_anon": result.get("subject_anon", ""),
            "risk_score":   result.get("risk_score", 0.0),
            "status":       result.get("pipeline_status", "safe"),
            "tokens":       token_list,
        }).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def render_data_json(priv_conn: sqlite3.Connection) -> str:
    """Return dashboard data as JSON for background polling (no full page reload)."""
    results = priv_conn.execute(
        "SELECT * FROM privacy_results ORDER BY processed_at DESC"
    ).fetchall()
    results = [dict(r) for r in results]

    all_tokens = priv_conn.execute(
        "SELECT case_id, pii_type FROM pii_tokens"
    ).fetchall()
    all_tokens = [dict(r) for r in all_tokens]

    logs = priv_conn.execute(
        "SELECT event, ts, detail FROM privacy_log ORDER BY ts DESC LIMIT 80"
    ).fetchall()
    logs = [dict(r) for r in logs]

    case_meta: dict[str, dict] = {}
    if _table_exists(priv_conn, "cases"):
        for r in priv_conn.execute(
            "SELECT case_id, source_type, subject FROM cases"
        ):
            case_meta[r["case_id"]] = dict(r)

    total        = len(results)
    total_tokens = len(all_tokens)
    types_found  = len({t["pii_type"] for t in all_tokens})
    k_ok         = sum(1 for r in results if r["k_anon_ok"])
    avg_risk     = (
        round(sum(r.get("risk_score", 0.0) for r in results) / total, 3)
        if total else 0.0
    )

    cases_out: list[dict] = []
    for r in results:
        meta = case_meta.get(r["case_id"], {})
        type_counts: dict[str, int] = {}
        n_tok = 0
        for t in all_tokens:
            if t["case_id"] == r["case_id"]:
                n_tok += 1
                type_counts[t["pii_type"]] = type_counts.get(t["pii_type"], 0) + 1
        cases_out.append({
            "case_id":          r["case_id"],
            "source_type":      meta.get("source_type", ""),
            "subject":          meta.get("subject", ""),
            "n_tokens":         n_tok,
            "type_counts":      type_counts,
            "k_anon_ok":        r["k_anon_ok"],
            "risk_score":       r.get("risk_score", 0.0),
            "pipeline_status":  r.get("pipeline_status", "safe"),
            "vault_session_id": r.get("vault_session_id", ""),
        })

    log_out = [
        {
            "event":  lg["event"],
            "ts":     lg["ts"][11:19],
            "detail": (lg.get("detail") or "")[:80],
        }
        for lg in logs
    ]

    return json.dumps({
        "stats": {
            "total":        total,
            "total_tokens": total_tokens,
            "types_found":  types_found,
            "k_ok":         k_ok,
            "avg_risk":     avg_risk,
        },
        "cases": cases_out,
        "logs":  log_out,
    })


def make_handler(conn: sqlite3.Connection):
    class H(PrivacyDashboardHandler):
        priv_conn = conn
    return H


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "═"*60)
    print("  CITIZEN PLATFORM — Phase 04: Privacy  (DEMO)")
    print("  Engine: Presidio + Swiss recognisers + PseudonymVault")
    print("═"*60)

    priv_conn = init_privacy_db()
    print(f"\n  ✓  Shared pipeline DB : {DB_PATH}")
    print(f"  ✓  Engine             : privacy/ package (Presidio)")
    print(f"  ✓  Notify endpoint    : POST /notify (pushed to by Security)")

    stop_event = threading.Event()
    poller = threading.Thread(
        target=poll_and_anonymise,
        args=(priv_conn, stop_event),
        daemon=True,
    )
    poller.start()

    handler    = make_handler(priv_conn)
    server     = http.server.HTTPServer(("", PORT), handler)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    print(f"\n  ✓  Dashboard: http://localhost:{PORT}")
    print(f"\n  Run order:")
    print(f"    Terminal 1 → python3 demo/reception.py    (port 8000)")
    print(f"    Terminal 2 → python3 demo/ingestion.py    (port 8001)")
    print(f"    Terminal 3 → python3 demo/security.py     (port 8002)")
    print(f"    Terminal 4 → python3 demo/privacy.py      (port 8003)")
    print(f"\n  Press Ctrl-C to stop.\n")
    print("─"*60)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n\n  Stopping privacy...")
        stop_event.set()
        server.shutdown()
        print("  Done.\n")


if __name__ == "__main__":
    main()
