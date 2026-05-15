"""
demo/validation.py
------------------
Phase 11: Validation  (DEMO VERSION)

The mandatory human review step. No response reaches a citizen
without sign-off by an authorised agent.

This demo provides a real interactive web UI at port 8010 where
you can:
  - Review each case's composed document (neutral + tone-adapted)
  - See quality scores, mood, source channel
  - Approve → case moves to VALIDATION_DONE (→ Dispatch)
  - Reject  → case routes back to Response with a rejection reason
              (iteration counter incremented; escalates at 3)

In production:
  - Authenticated via eIAM / FED LOGIN
  - Qualified digital signature (ZertES) applied on approval
  - Rejection routes directly to Response (bypasses Prompt Enrichment)
  - Supervisor escalation triggered at MAX_VALIDATION_REJECTIONS=3

In demo:
  - No authentication (open UI — demo only)
  - Signature simulated with a timestamp + agent name
  - Rejection reason required (free text)
  - Full escalation path implemented

Run:
    python3 demo/reception.py           ← port 8000
    ...
    python3 demo/recomposition.py       ← port 8009
    python3 demo/validation.py          ← port 8010

Then open http://localhost:8010 and act as the validating agent.
"""

from __future__ import annotations

import html as _html
import http.server
import json
import re
import sqlite3
import threading
import urllib.parse
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ─────────────────────────────────────────────────────────────
# PATHS & CONSTANTS
# ─────────────────────────────────────────────────────────────

DEMO_DIR            = Path(__file__).parent
INGESTION_DB_PATH   = DEMO_DIR / "demo_ingestion.db"
DECOMP_DB_PATH      = DEMO_DIR / "demo_decomposition.db"
RECOMP_DB_PATH      = DEMO_DIR / "demo_recomposition.db"
VALIDATION_DB_PATH  = DEMO_DIR / "demo_validation.db"
PORT                = 8010

MAX_VALIDATION_REJECTIONS = 3   # non-negotiable constant from spec


# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────

def init_validation_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(VALIDATION_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS validation_decisions (
            id              TEXT PRIMARY KEY,
            case_id         TEXT NOT NULL,
            decision        TEXT NOT NULL,   -- APPROVED | REJECTED | ESCALATED
            agent_name      TEXT NOT NULL,
            rejection_reason TEXT,
            iteration       INTEGER NOT NULL DEFAULT 0,
            decided_at      TEXT NOT NULL,
            signature       TEXT             -- simulated ZertES timestamp
        );

        CREATE TABLE IF NOT EXISTS validation_log (
            id      TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            event   TEXT NOT NULL,
            detail  TEXT,
            ts      TEXT NOT NULL
        );
    """)
    conn.commit()
    return conn


def open_db(path: Path, readonly: bool = True) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    uri = f"file:{path}?mode=ro" if readonly else str(path)
    conn = sqlite3.connect(uri if readonly else str(path),
                           uri=readonly, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def log_event(val_conn: sqlite3.Connection,
              case_id: str, event: str, detail: str = "") -> None:
    val_conn.execute(
        "INSERT INTO validation_log VALUES (?,?,?,?,?)",
        (str(uuid.uuid4()), case_id, event, detail,
         datetime.now(timezone.utc).isoformat()),
    )
    val_conn.commit()


def get_pending_cases(val_conn: sqlite3.Connection) -> list[dict]:
    """Return all cases waiting for validation, with full context."""
    ing_db  = open_db(INGESTION_DB_PATH)
    rec_db  = open_db(RECOMP_DB_PATH)
    dec_db  = open_db(DECOMP_DB_PATH)

    if not all([ing_db, rec_db, dec_db]):
        return []

    try:
        cases = [dict(r) for r in ing_db.execute(
            "SELECT * FROM cases WHERE pipeline_step='RECOMPOSITION_DONE'"
        ).fetchall()]

        # Filter out already decided cases in current session
        decided = {r["case_id"] for r in val_conn.execute(
            "SELECT case_id FROM validation_decisions"
        ).fetchall()}

        result = []
        for case in cases:
            if case["case_id"] in decided:
                continue

            recomp = rec_db.execute(
                "SELECT * FROM recomp_results WHERE case_id=?",
                (case["case_id"],),
            ).fetchone()

            questions = [dict(r) for r in dec_db.execute(
                "SELECT * FROM questions WHERE case_id=?",
                (case["case_id"],),
            ).fetchall()]

            if recomp:
                result.append({
                    **case,
                    "recomp":    dict(recomp),
                    "questions": questions,
                })

        return result
    finally:
        for db in [ing_db, rec_db, dec_db]:
            try: db.close()
            except Exception: pass


def get_all_decisions(val_conn: sqlite3.Connection) -> list[dict]:
    return [dict(r) for r in val_conn.execute(
        "SELECT * FROM validation_decisions ORDER BY decided_at DESC"
    ).fetchall()]


# ─────────────────────────────────────────────────────────────
# DECISION PROCESSING
# ─────────────────────────────────────────────────────────────

def process_approve(case_id: str, agent_name: str,
                    val_conn: sqlite3.Connection) -> dict:
    """Record approval, advance pipeline, simulate ZertES signature."""
    signature = (
        f"DEMO-SIG-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        f"-{agent_name.upper().replace(' ', '-')[:20]}"
    )

    decision = {
        "id":               str(uuid.uuid4()),
        "case_id":          case_id,
        "decision":         "APPROVED",
        "agent_name":       agent_name,
        "rejection_reason": None,
        "iteration":        0,
        "decided_at":       datetime.now(timezone.utc).isoformat(),
        "signature":        signature,
    }
    val_conn.execute("""
        INSERT INTO validation_decisions VALUES
        (:id,:case_id,:decision,:agent_name,:rejection_reason,
         :iteration,:decided_at,:signature)
    """, decision)
    val_conn.commit()

    # Advance pipeline step
    _set_pipeline_step(case_id, "VALIDATION_DONE")

    log_event(val_conn, case_id, "APPROVED",
              f"agent={agent_name} sig={signature}")
    return decision


def process_reject(case_id: str, agent_name: str,
                   rejection_reason: str,
                   val_conn: sqlite3.Connection) -> dict:
    """
    Record rejection, increment iteration counter.
    If MAX_VALIDATION_REJECTIONS reached → ESCALATED.
    Otherwise → route back to Response (bypasses Prompt Enrichment).
    """
    # Get current iteration from ingestion DB
    ing = open_db(INGESTION_DB_PATH)
    current_iter = 0
    if ing:
        try:
            row = ing.execute(
                "SELECT iteration FROM cases WHERE case_id=?", (case_id,)
            ).fetchone()
            if row:
                current_iter = row["iteration"] or 0
        finally:
            ing.close()

    new_iter  = current_iter + 1
    escalated = new_iter >= MAX_VALIDATION_REJECTIONS

    decision = {
        "id":               str(uuid.uuid4()),
        "case_id":          case_id,
        "decision":         "ESCALATED" if escalated else "REJECTED",
        "agent_name":       agent_name,
        "rejection_reason": rejection_reason,
        "iteration":        new_iter,
        "decided_at":       datetime.now(timezone.utc).isoformat(),
        "signature":        None,
    }
    val_conn.execute("""
        INSERT INTO validation_decisions VALUES
        (:id,:case_id,:decision,:agent_name,:rejection_reason,
         :iteration,:decided_at,:signature)
    """, decision)
    val_conn.commit()

    if escalated:
        _set_pipeline_step(case_id, "ESCALATED", iteration=new_iter)
        log_event(val_conn, case_id, "ESCALATED",
                  f"agent={agent_name} iter={new_iter} reason={rejection_reason[:60]}")
    else:
        # Route back to Response — bypass Prompt Enrichment
        _set_pipeline_step(case_id, "ENRICHMENT_DONE", iteration=new_iter)
        log_event(val_conn, case_id, "REJECTED",
                  f"agent={agent_name} iter={new_iter} reason={rejection_reason[:60]}")

    return decision


def _set_pipeline_step(case_id: str, step: str,
                        iteration: int | None = None) -> None:
    try:
        ing = sqlite3.connect(str(INGESTION_DB_PATH),
                              check_same_thread=False)
        if iteration is not None:
            ing.execute(
                "UPDATE cases SET pipeline_step=?, iteration=? WHERE case_id=?",
                (step, iteration, case_id),
            )
        else:
            ing.execute(
                "UPDATE cases SET pipeline_step='VALIDATION_DONE' WHERE case_id=?",
                (case_id,),
            )
        ing.commit()
        ing.close()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# HTML UI
# ─────────────────────────────────────────────────────────────

CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#f0f2f5;min-height:100vh;padding:32px 20px;color:#1a1a2e}
.page{max-width:1000px;margin:0 auto}

/* Header */
.header{background:#1a2332;color:white;border-radius:8px;
        padding:20px 28px;margin-bottom:24px;
        display:flex;align-items:center;gap:16px}
.badge{background:#16a34a;color:white;font-size:10px;font-weight:700;
       padding:3px 8px;border-radius:3px;letter-spacing:.06em;text-transform:uppercase}
.header h1{font-size:18px;font-weight:600}
.header p{font-size:12px;color:#8b949e;margin-top:2px}
.hdr-right{margin-left:auto;text-align:right;font-size:11px;color:#8b949e}

/* Auth bar */
.auth-bar{background:#ecfdf5;border:1px solid #86efac;border-radius:6px;
          padding:10px 18px;margin-bottom:20px;display:flex;
          align-items:center;gap:10px;font-size:12px;color:#166534}
.auth-bar .auth-icon{font-size:16px}

/* Stats */
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}
.stat{background:white;border-radius:7px;padding:14px 18px;
      box-shadow:0 1px 3px rgba(0,0,0,.08)}
.stat-num{font-size:24px;font-weight:700;color:#1a2332}
.stat-label{font-size:10px;color:#9ca3af;margin-top:2px;
            text-transform:uppercase;letter-spacing:.05em}

.sec-title{font-size:11px;font-weight:700;color:#9ca3af;
           text-transform:uppercase;letter-spacing:.07em;
           margin-bottom:12px;padding:0 2px;margin-top:24px}

/* Case validation card */
.case-card{background:white;border-radius:8px;
           box-shadow:0 1px 3px rgba(0,0,0,.08);
           margin-bottom:20px;overflow:hidden}
.case-head{background:#f8f9fb;padding:14px 20px;
           border-bottom:1px solid #e5e7eb;
           display:flex;align-items:flex-start;gap:12px}
.case-id{font-family:monospace;font-size:11px;color:#9ca3af;
         padding-top:2px;flex-shrink:0}
.case-meta{flex:1;min-width:0}
.case-title{font-size:14px;font-weight:600;color:#1a2332;
            white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.case-sub{font-size:11px;color:#6b7280;margin-top:3px}
.case-tags{display:flex;gap:5px;flex-wrap:wrap;margin-top:5px}
.tag{font-size:10px;font-weight:600;padding:2px 7px;border-radius:3px}
.tag-src    {background:#e0f2fe;color:#0369a1}
.tag-lang   {background:#f0fdf4;color:#166534}
.tag-mood-n {background:#f3f4f6;color:#374151}
.tag-mood-f {background:#fef3c7;color:#92400e}
.tag-mood-a {background:#fee2e2;color:#991b1b}
.tag-mood-d {background:#ede9fe;color:#4c1d95}
.tag-tone   {background:#fdf4ff;color:#7e22ce}
.tag-prio   {background:#fff7ed;color:#c2410c}
.tag-flag   {background:#fef3c7;color:#b45309;font-weight:700}

/* Quality scores mini bar */
.q-scores{padding:10px 20px;border-bottom:1px solid #f3f4f6;
          display:flex;gap:20px;align-items:center}
.qs-item{display:flex;align-items:center;gap:6px;font-size:11px;color:#6b7280}
.qs-bar{width:60px;height:6px;background:#f3f4f6;border-radius:3px;overflow:hidden}
.qs-fill{height:100%;border-radius:3px}

/* Document viewer with tabs */
.doc-tabs{display:flex;border-bottom:1px solid #e5e7eb;
          background:#f8f9fb;padding:0 20px}
.doc-tab{padding:8px 14px;font-size:11px;font-weight:600;
         cursor:pointer;border-bottom:2px solid transparent;
         color:#6b7280;white-space:nowrap;transition:all .15s}
.doc-tab:hover{color:#374151}
.doc-tab.active{color:#16a34a;border-bottom-color:#16a34a}

.doc-panels{padding:16px 20px}
.doc-panel{display:none}
.doc-panel.active{display:block}

.letter{background:#fffef7;border:1px solid #e5e7eb;border-radius:6px;
        padding:24px 28px;font-size:13px;line-height:1.9;color:#1e293b;
        white-space:pre-wrap;word-break:break-word;
        font-family:'Georgia','Times New Roman',serif}
.letter .flag-notice{background:#fef3c7;color:#92400e;
                     font-family:monospace;font-size:10px;
                     padding:2px 6px;border-radius:3px;
                     font-style:normal;font-weight:600}

/* Decision form */
.decision-form{padding:16px 20px;border-top:2px solid #e5e7eb;
               background:#fafafa}
.form-row{display:flex;gap:12px;align-items:flex-start;flex-wrap:wrap}
.agent-field{flex:1;min-width:160px}
.agent-field label{display:block;font-size:11px;font-weight:600;
                   color:#374151;margin-bottom:4px}
.agent-field input,.reason-field textarea{
    width:100%;border:1px solid #d1d5db;border-radius:5px;
    padding:8px 10px;font-size:13px;font-family:inherit;
    background:white;transition:border-color .15s}
.agent-field input:focus,.reason-field textarea:focus{
    outline:none;border-color:#16a34a;
    box-shadow:0 0 0 2px rgba(22,163,74,.1)}
.reason-field{flex:2;min-width:200px}
.reason-field label{display:block;font-size:11px;font-weight:600;
                    color:#374151;margin-bottom:4px}
.reason-field textarea{resize:vertical;min-height:52px}
.btns{display:flex;gap:8px;align-items:flex-end;flex-shrink:0}

.btn-approve{background:#16a34a;color:white;border:none;
             border-radius:5px;padding:10px 20px;font-size:13px;
             font-weight:600;cursor:pointer;font-family:inherit;
             transition:background .15s;white-space:nowrap}
.btn-approve:hover{background:#15803d}
.btn-reject{background:white;color:#dc2626;border:1px solid #dc2626;
            border-radius:5px;padding:10px 18px;font-size:13px;
            font-weight:600;cursor:pointer;font-family:inherit;
            transition:all .15s;white-space:nowrap}
.btn-reject:hover{background:#fef2f2}

.iter-warning{font-size:11px;color:#b45309;background:#fef3c7;
              padding:4px 10px;border-radius:4px;margin-bottom:8px}
.esc-warning{font-size:11px;color:#dc2626;background:#fee2e2;
             padding:4px 10px;border-radius:4px;margin-bottom:8px;font-weight:600}

/* Decisions log */
.decisions-table{background:white;border-radius:7px;
                 box-shadow:0 1px 3px rgba(0,0,0,.08);overflow:hidden}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;padding:10px 14px;background:#f8f9fb;color:#6b7280;
   font-size:11px;font-weight:600;text-transform:uppercase;
   letter-spacing:.05em;border-bottom:1px solid #e5e7eb}
td{padding:10px 14px;border-bottom:1px solid #f3f4f6;
   color:#374151;vertical-align:middle}
tr:last-child td{border-bottom:none}

.dec-approved {color:#16a34a;font-weight:700}
.dec-rejected {color:#d97706;font-weight:700}
.dec-escalated{color:#dc2626;font-weight:700}

.sig-box{font-family:monospace;font-size:10px;color:#9ca3af}

/* Log panel */
.log-panel{background:#1a2332;border-radius:7px;padding:16px 20px;
           max-height:180px;overflow-y:auto;margin-top:16px}
.log-line{font-family:'Menlo',monospace;font-size:11px;
          color:#8b949e;padding:2px 0;line-height:1.6}
.ts{color:#484f58}
.ev-approve{color:#2ea043}
.ev-reject {color:#d29922}
.ev-escalate{color:#f85149;font-weight:600}

.empty{text-align:center;color:#9ca3af;padding:32px;font-size:13px}
.all-done{text-align:center;padding:32px;font-size:14px;
          color:#16a34a;font-weight:600}

.refresh-note{font-size:11px;color:#9ca3af;text-align:center;margin-top:10px}
"""

_MOOD_ICON  = {"NEUTRAL":"😐","FRUSTRATED":"😤","ANGRY":"😠","DISTRESSED":"😰"}
_MOOD_TAG   = {"NEUTRAL":"tag-mood-n","FRUSTRATED":"tag-mood-f",
               "ANGRY":"tag-mood-a","DISTRESSED":"tag-mood-d"}
_PRIO_COLOR = {"LOW":"#6b7280","NORMAL":"#059669",
               "HIGH":"#d97706","URGENT":"#dc2626"}
_SRC        = {"DIRECT_EMAIL":"Email","STAFF_FORWARD":"Staff",
               "POSTAL_SCAN":"Scan","WEB_FORM":"Web Form"}


def _format_letter(text: str) -> str:
    text = _html.escape(text or "")
    text = re.sub(
        r"(\[HINWEIS FÜR PRÜFER:[^\]]+\]|\[NOTE FOR VALIDATOR:[^\]]+\]"
        r"|\[NOTE POUR LE VALIDATEUR:[^\]]+\]|\[NOTA PER IL VALIDATORE:[^\]]+\])",
        r'<span class="flag-notice">\1</span>',
        text,
    )
    return text


def render_page(val_conn: sqlite3.Connection) -> str:
    pending   = get_pending_cases(val_conn)
    decisions = get_all_decisions(val_conn)
    logs      = [dict(r) for r in val_conn.execute(
        "SELECT * FROM validation_log ORDER BY ts DESC LIMIT 60"
    ).fetchall()]

    # Stats
    approved  = sum(1 for d in decisions if d["decision"] == "APPROVED")
    rejected  = sum(1 for d in decisions if d["decision"] == "REJECTED")
    escalated = sum(1 for d in decisions if d["decision"] == "ESCALATED")

    stats_html = f"""<div class="stats">
      <div class="stat"><div class="stat-num">{len(pending)}</div>
        <div class="stat-label">Awaiting Review</div></div>
      <div class="stat"><div class="stat-num" style="color:#16a34a">{approved}</div>
        <div class="stat-label">✅ Approved</div></div>
      <div class="stat"><div class="stat-num" style="color:#d97706">{rejected}</div>
        <div class="stat-label">🔄 Rejected</div></div>
      <div class="stat"><div class="stat-num" style="color:#dc2626">{escalated}</div>
        <div class="stat-label">🚨 Escalated</div></div>
    </div>"""

    # Pending case cards
    case_cards = ""
    if not pending:
        if decisions:
            case_cards = '<div class="all-done">✅ All cases reviewed for this session.<br><span style="font-size:12px;color:#6b7280;font-weight:400">New cases appear automatically as the pipeline processes them.</span></div>'
        else:
            case_cards = '<div class="empty">⏳ No cases ready for validation yet.<br><span style="font-size:12px">Cases appear here when Recomposition completes.</span></div>'

    for item in pending:
        cid      = item["case_id"]
        recomp   = item["recomp"]
        qs       = item["questions"]
        mood     = item.get("mood") or "NEUTRAL"
        lang     = item.get("language", "DE")
        priority = item.get("priority") or "NORMAL"
        src      = _SRC.get(item.get("source_type",""), "")
        iter_n   = item.get("iteration", 0) or 0
        subj     = item.get("subject") or "(no subject)"

        mood_tag = _MOOD_TAG.get(mood, "tag-mood-n")
        mood_icon= _MOOD_ICON.get(mood, "")
        prio_col = _PRIO_COLOR.get(priority, "#6b7280")
        toned    = recomp.get("tone_applied", 0)
        flagged  = recomp.get("flagged_count", 0)

        # Quality score bars
        qs_html = ""
        for q in qs:
            if q.get("scope") != "IN_SCOPE":
                continue
            a = q.get("alignment_score") or 0
            f = q.get("faithful_score") or 0
            a_col = "#22c55e" if a >= 0.4  else "#ef4444"
            f_col = "#22c55e" if f >= 0.35 else "#ef4444"
            flag_badge = (
                ' <span class="tag tag-flag">⚑ flagged</span>'
                if q.get("quality_flagged") else ""
            )
            qs_html += f"""
            <div class="qs-item">
              <span>{_html.escape(q['theme'])}</span>
              <span>align</span>
              <div class="qs-bar">
                <div class="qs-fill" style="width:{int(a*100)}%;background:{a_col}"></div>
              </div>
              <span>{a:.2f}</span>
              <span>faith</span>
              <div class="qs-bar">
                <div class="qs-fill" style="width:{int(f*100)}%;background:{f_col}"></div>
              </div>
              <span>{f:.2f}</span>
              {flag_badge}
            </div>"""

        # Document panels
        neutral = _format_letter(recomp.get("neutral_draft",""))
        toned_d = _format_letter(recomp.get("toned_draft",""))

        if toned and toned_d:
            tabs = f"""
            <div class="doc-tabs">
              <div class="doc-tab active"
                   onclick="switchTab(this,'{cid}','neutral')">
                📄 Neutral draft
              </div>
              <div class="doc-tab"
                   onclick="switchTab(this,'{cid}','toned')">
                🎨 Tone-adapted ({mood})
              </div>
            </div>
            <div class="doc-panels">
              <div class="doc-panel active" id="panel-{cid}-neutral">
                <div class="letter">{neutral}</div>
              </div>
              <div class="doc-panel" id="panel-{cid}-toned">
                <div class="letter">{toned_d}</div>
              </div>
            </div>"""
        else:
            tabs = f"""
            <div class="doc-panels">
              <div class="doc-panel active">
                <div class="letter">{neutral}</div>
              </div>
            </div>"""

        # Iteration warnings
        iter_warning = ""
        if iter_n >= MAX_VALIDATION_REJECTIONS - 1 and iter_n > 0:
            iter_warning = (
                f'<div class="esc-warning">⚠ Next rejection will ESCALATE '
                f'this case to supervisor (iteration {iter_n}/{MAX_VALIDATION_REJECTIONS})</div>'
            )
        elif iter_n > 0:
            iter_warning = (
                f'<div class="iter-warning">ℹ Iteration {iter_n}/{MAX_VALIDATION_REJECTIONS} '
                f'— this response was regenerated after a previous rejection</div>'
            )

        case_cards += f"""
        <div class="case-card" id="card-{cid}">
          <div class="case-head">
            <div class="case-id">{cid[:8]}…</div>
            <div class="case-meta">
              <div class="case-title">{_html.escape(subj[:70])}</div>
              <div class="case-sub">
                {_html.escape(item.get('sender_email','') or '')} ·
                {_html.escape(item.get('sender_name','') or '')}
              </div>
              <div class="case-tags">
                <span class="tag tag-src">{src}</span>
                <span class="tag tag-lang">{lang}</span>
                <span class="tag {mood_tag}">{mood_icon} {mood}</span>
                <span class="tag tag-prio" style="color:{prio_col}">{priority}</span>
                {'<span class="tag tag-tone">🎨 toned</span>' if toned else ''}
                {'<span class="tag tag-flag">⚑ quality flag</span>' if flagged else ''}
              </div>
            </div>
          </div>

          <div class="q-scores">{qs_html or '<span style="font-size:11px;color:#9ca3af">No scored questions</span>'}</div>

          {tabs}

          <div class="decision-form">
            {iter_warning}
            <form method="POST" action="/decide">
              <input type="hidden" name="case_id" value="{cid}">
              <div class="form-row">
                <div class="agent-field">
                  <label>Agent name (simulates eIAM)</label>
                  <input type="text" name="agent_name"
                         placeholder="e.g. M. Weber"
                         value="Agent Demo" required>
                </div>
                <div class="reason-field">
                  <label>Rejection reason (required if rejecting)</label>
                  <textarea name="rejection_reason"
                    placeholder="e.g. Answer does not address the specific timeline question. Please include processing time information."></textarea>
                </div>
                <div class="btns">
                  <button type="submit" name="action" value="approve"
                          class="btn-approve">✅ Approve</button>
                  <button type="submit" name="action" value="reject"
                          class="btn-reject">↩ Reject</button>
                </div>
              </div>
            </form>
          </div>
        </div>"""

    # Decisions history table
    hist_rows = ""
    if not decisions:
        hist_rows = '<tr><td colspan="5" class="empty">No decisions yet</td></tr>'
    for d in decisions:
        dec_css = {
            "APPROVED":  "dec-approved",
            "REJECTED":  "dec-rejected",
            "ESCALATED": "dec-escalated",
        }.get(d["decision"], "")
        dec_icon = {"APPROVED":"✅","REJECTED":"↩","ESCALATED":"🚨"}.get(d["decision"],"")
        sig = (f'<span class="sig-box">{d["signature"]}</span>'
               if d.get("signature") else "—")
        reason = _html.escape((d.get("rejection_reason") or "")[:50])
        hist_rows += f"""
        <tr>
          <td><code style="font-size:11px">{d['case_id'][:8]}…</code></td>
          <td class="{dec_css}">{dec_icon} {d['decision']}</td>
          <td>{_html.escape(d['agent_name'])}</td>
          <td style="font-size:11px;color:#6b7280">{reason}</td>
          <td>{sig}</td>
        </tr>"""

    # Log
    ev_css = {
        "APPROVED":  "ev-approve",
        "REJECTED":  "ev-reject",
        "ESCALATED": "ev-escalate",
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
        log_lines = '<div class="log-line" style="color:#484f58">— no decisions yet —</div>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Phase 11 — Validation</title>
<style>{CSS}</style>
</head>
<body>
<div class="page">
  <div class="header">
    <div>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
        <span class="badge">Phase 11</span>
        <h1>Validation</h1>
      </div>
      <p>Human review · Approve or Reject · ZertES signature simulation</p>
    </div>
    <div class="hdr-right">
      Interactive — no auto-refresh<br>
      <span style="color:#16a34a;font-weight:600">● Awaiting agent</span>
    </div>
  </div>

  <div class="auth-bar">
    <span class="auth-icon">🔐</span>
    <strong>Demo session</strong> — authenticated as Agent Demo (eIAM simulation).
    In production: FED LOGIN / eIAM required. All decisions are audit-logged.
  </div>

  {stats_html}

  <div class="sec-title">Cases awaiting validation
    <span style="font-weight:400;color:#9ca3af;margin-left:8px">
      Review the document below, then Approve or Reject
    </span>
  </div>
  {case_cards}

  <div class="sec-title">Decision history</div>
  <div class="decisions-table">
    <table>
      <thead>
        <tr>
          <th>Case ID</th><th>Decision</th><th>Agent</th>
          <th>Reason</th><th>ZertES Signature (simulated)</th>
        </tr>
      </thead>
      <tbody>{hist_rows}</tbody>
    </table>
  </div>

  <div class="log-panel">{log_lines}</div>
  <p class="refresh-note">
    Page refreshes after each decision.
    <a href="/" style="color:#16a34a;margin-left:6px">↻ Refresh manually</a>
  </p>
</div>

<script>
function switchTab(btn, caseId, which) {{
  var card = document.getElementById('card-' + caseId);
  card.querySelectorAll('.doc-tab').forEach(function(t) {{
    t.classList.remove('active');
  }});
  btn.classList.add('active');
  card.querySelectorAll('.doc-panel').forEach(function(p) {{
    p.classList.remove('active');
  }});
  var panel = document.getElementById('panel-' + caseId + '-' + which);
  if (panel) panel.classList.add('active');
}}

// Require rejection reason before submitting reject
document.addEventListener('submit', function(e) {{
  var form = e.target;
  var action = form.querySelector('button[type=submit]:focus');
  if (action && action.value === 'reject') {{
    var reason = form.querySelector('[name=rejection_reason]').value.trim();
    if (!reason) {{
      e.preventDefault();
      alert('Please enter a rejection reason before rejecting.');
    }}
  }}
}});
</script>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────
# HTTP SERVER
# ─────────────────────────────────────────────────────────────

class ValidationHandler(http.server.BaseHTTPRequestHandler):
    val_conn:   sqlite3.Connection = None  # type: ignore
    write_lock: threading.Lock     = None  # type: ignore

    def log_message(self, *args: object) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/favicon.ico":
            self.send_response(204); self.end_headers(); return
        page = render_page(self.val_conn)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode())

    def do_POST(self) -> None:
        if self.path != "/decide":
            self.send_response(404); self.end_headers(); return

        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length).decode("utf-8")
        fields = urllib.parse.parse_qs(body, keep_blank_values=True)

        def f(key: str) -> str:
            return (fields.get(key, [""])[0]).strip()

        case_id          = f("case_id")
        action           = f("action")
        agent_name       = f("agent_name") or "Agent Demo"
        rejection_reason = f("rejection_reason")

        with self.write_lock:
            if action == "approve":
                decision = process_approve(case_id, agent_name, self.val_conn)
                _print_decision(decision)
            elif action == "reject":
                if not rejection_reason:
                    self.send_response(302)
                    self.send_header("Location", "/")
                    self.end_headers()
                    return
                decision = process_reject(
                    case_id, agent_name, rejection_reason, self.val_conn
                )
                _print_decision(decision)

        self.send_response(302)
        self.send_header("Location", "/")
        self.end_headers()


def make_handler(conn: sqlite3.Connection, lk: threading.Lock):
    class H(ValidationHandler):
        val_conn   = conn
        write_lock = lk
    return H


# ─────────────────────────────────────────────────────────────
# TERMINAL PRINTER
# ─────────────────────────────────────────────────────────────

def _print_decision(d: dict) -> None:
    icons = {"APPROVED":"✅","REJECTED":"↩","ESCALATED":"🚨"}
    icon  = icons.get(d["decision"], "?")
    print(f"\n{'─'*60}")
    print(f"  {icon}  Validation decision: {d['decision']}")
    print(f"  Case ID : {d['case_id']}")
    print(f"  Agent   : {d['agent_name']}")
    if d.get("rejection_reason"):
        print(f"  Reason  : {d['rejection_reason'][:70]}")
    if d.get("signature"):
        print(f"  Sig     : {d['signature']}")
    if d["decision"] == "APPROVED":
        print(f"  → Next step: Phase 12 Dispatch")
    elif d["decision"] == "REJECTED":
        print(f"  → Routing back to Response (iter {d['iteration']})")
    else:
        print(f"  → ESCALATED to supervisor")
    print(json.dumps({
        "case_id":   d["case_id"],
        "decision":  d["decision"],
        "iteration": d["iteration"],
        "step": "DISPATCH" if d["decision"] == "APPROVED" else
                "RESPONSE"  if d["decision"] == "REJECTED" else "ESCALATED",
    }, indent=4))


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "═"*60)
    print("  CITIZEN PLATFORM — Phase 11: Validation  (DEMO)")
    print("═"*60)

    val_conn = init_validation_db()
    lock     = threading.Lock()

    print(f"\n  ✓  Validation DB : {VALIDATION_DB_PATH}")
    print(f"  ✓  Max rejections: {MAX_VALIDATION_REJECTIONS} (then ESCALATED)")

    handler    = make_handler(val_conn, lock)
    server     = http.server.HTTPServer(("", PORT), handler)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    print(f"\n  ✓  Validation UI : http://localhost:{PORT}")
    print(f"\n  ⬆  Open the URL above in your browser.")
    print(f"     Review each case and click Approve or Reject.")
    print(f"\n  Run order:")
    for i, (name, port) in enumerate([
        ("reception.py",8000),("ingestion.py",8001),
        ("security.py",8002),("privacy.py",8003),
        ("analysis.py",8004),("decomposition.py",8005),
        ("prompt_enrichment.py",8006),("response.py",8007),
        ("quality.py",8008),("recomposition.py",8009),
        ("validation.py",8010),
    ], 1):
        print(f"    Terminal {i:2} → python3 demo/{name:<26} (port {port})")
    print(f"\n  Press Ctrl-C to stop.\n")
    print("─"*60)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n\n  Stopping validation...")
        server.shutdown()
        print("  Done.\n")


if __name__ == "__main__":
    main()
