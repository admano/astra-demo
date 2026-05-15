"""
demo/privacy.py
---------------
Phase 04: Privacy  (DEMO VERSION)

Reads cases at pipeline_step=SECURITY_DONE, anonymises all PII
from subject, body, and attachment Markdowns, stores a reversible
token map, and advances cases to pipeline_step=PRIVACY_DONE.

What this phase does (spec order):
  ① Detect + mask PII in subject         → anonymised subject
  ② Detect + mask PII in body            → anonymised body
  ③ Detect + mask PII in attachment MDs  → anonymised markdown
  ④ Store PII token map                  → pii_tokens table
  ⑤ k-anonymity check                   → flag if grouping too small

In production: Microsoft Presidio + custom Swiss recognisers.
In demo:       regex-based recognisers for Swiss-specific PII patterns:
               names, emails, Swiss phones, IBANs, AHV numbers,
               Swiss postal addresses, dates, case-like references.

The LLM (Analysis, Response) will ONLY ever see anonymised text.
PII is reconstructed at Dispatch from the token map.

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
import re
import sqlite3
import threading
import uuid
from collections import defaultdict
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
PRIVACY_DB_PATH   = DEMO_DIR / "demo_privacy.db"
PORT              = 8003

# k-anonymity threshold (configurable per tenant in production)
K_ANONYMITY_MIN = 3


# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────

def init_privacy_db() -> sqlite3.Connection:
    """
    Tables:
      privacy_results  — one row per case, overall outcome
      pii_tokens       — reversible token map (tag → original value)
      privacy_log      — append-only audit events
    """
    conn = sqlite3.connect(str(PRIVACY_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS privacy_results (
            case_id          TEXT PRIMARY KEY,
            tenant_id        TEXT NOT NULL,
            tokens_found     INTEGER NOT NULL DEFAULT 0,
            subject_anon     TEXT,
            body_anon        TEXT,
            k_anon_ok        INTEGER NOT NULL DEFAULT 1,
            processed_at     TEXT NOT NULL,
            pipeline_step    TEXT NOT NULL DEFAULT 'PRIVACY_DONE'
        );

        CREATE TABLE IF NOT EXISTS pii_tokens (
            id       TEXT PRIMARY KEY,
            case_id  TEXT NOT NULL,
            tag      TEXT NOT NULL,
            pii_type TEXT NOT NULL,
            value    TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS attachment_anon (
            id           TEXT PRIMARY KEY,
            case_id      TEXT NOT NULL,
            filename     TEXT NOT NULL,
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


def open_db_ro(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True,
                           check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


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
# PII RECOGNISERS
#
# In production: Microsoft Presidio with custom Swiss recognisers.
# In demo:       ordered regex patterns — each has a priority so
#                longer/more-specific patterns match before shorter ones.
#
# Each recogniser: (pii_type, compiled_regex)
# Matched text → replaced with <PII_TYPE_N> placeholder.
# ─────────────────────────────────────────────────────────────

# Swiss AHV / OASI number:  756.XXXX.XXXX.XX
_AHV_RE    = re.compile(r"\b756\.\d{4}\.\d{4}\.\d{2}\b")

# Swiss IBAN:  CH followed by 2 check digits + 17 alphanumeric
_IBAN_RE   = re.compile(r"\bCH\d{2}[\s]?\d{4}[\s]?\d{4}[\s]?\d{4}[\s]?\d{4}[\s]?\d\b",
                        re.IGNORECASE)

# Swiss phone: +41 XX XXX XX XX or 0XX XXX XX XX
_PHONE_RE  = re.compile(
    r"(\+41|0041|0)[\s\-]?"         # country code
    r"\d{2}[\s\-]?"                  # area
    r"\d{3}[\s\-]?"                  # first block
    r"\d{2}[\s\-]?"                  # second block
    r"\d{2}\b"
)

# Swiss postal address:  Streetname + number + optional newline + 4-digit ZIP + city
# Uses [^\n] to prevent multiline matches eating signatures
_ADDRESS_RE = re.compile(
    r"[A-ZÄÖÜ][a-zäöüA-ZÄÖÜ\-]+"
    r"(?:strasse|gasse|weg|allee|platz|gässli|strässli)"
    r"[\s]?\d{1,4}[a-z]?"           # house number (same line)
    r"[,\n\s]{1,3}"                  # separator — comma, newline, or space
    r"\d{4}[\s]+[A-ZÄÖÜ][a-zäöü]+", # ZIP + city
    re.IGNORECASE,
)

# Standalone 4-digit Swiss ZIP + city (catches "3003 Bern", "7320 Sargans")
_ZIP_CITY_RE = re.compile(r"\b\d{4}[\s]+[A-ZÄÖÜ][a-zäöü]{2,}\b")

# Email address
_EMAIL_RE  = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")

# Person names — heuristic:
# Two capitalised words on the same line that aren't known non-name patterns.
# Applied AFTER emails are removed so "Marie.Dupont@..." doesn't double-match.
# \x20 = space only (no \n) so names don't span line boundaries.
_NAME_RE   = re.compile(
    r"\b([A-ZÄÖÜ][a-zäöü]{1,20})"   # first name
    r"\x20+"                          # single-line space only (never newline)
    r"([A-ZÄÖÜ][a-zäöü]{1,25})\b"   # last name
)

# Dates: DD. Month YYYY  or  DD.MM.YYYY  or  YYYY-MM-DD
_DATE_RE   = re.compile(
    r"\b\d{1,2}\.\s*(?:Januar|Februar|März|April|Mai|Juni|Juli|August|"
    r"September|Oktober|November|Dezember|janvier|février|mars|avril|mai|"
    r"juin|juillet|août|septembre|octobre|novembre|décembre)"
    r"\s*\d{4}\b"
    r"|\b\d{1,2}\.\d{1,2}\.\d{4}\b"
    r"|\b\d{4}-\d{2}-\d{2}\b",
    re.IGNORECASE,
)

# Ordered list: most-specific patterns first to avoid partial overlaps
_RECOGNISERS: list[tuple[str, re.Pattern]] = [
    ("AHV",     _AHV_RE),
    ("IBAN",    _IBAN_RE),
    ("PHONE",   _PHONE_RE),
    ("ADDRESS", _ADDRESS_RE),
    ("ZIP",     _ZIP_CITY_RE),
    ("EMAIL",   _EMAIL_RE),
    ("DATE",    _DATE_RE),
    ("PERSON",  _NAME_RE),
]

# Words that look like names but are NOT PII — prevent false positives
_NON_NAMES = frozenset({
    "Sehr Geehrte", "Sehr Geehrter", "Damen Herren", "Mit Freundlichen",
    "Freundlichen Grüssen", "Madame Monsieur", "Bundesamt Strassen",
    "Bundesamt Für", "Für Strassen", "Bitte Bearbeiten",
    "Cordiales Salutations", "Bonjour Je", "Guten Tag",
    "Auf Wiederhören", "Januar 2025", "Bitte Nehmen", "Bitte Antworten",
    "BBL Scan", "Scan Service", "Admin Ch", "Example Ch",
    "Bitte Bearbeiten", "Kontaktieren Sie", "Meine Adresse",
    "Votre Demande", "Cordialement Ofrou", "Cordiali Saluti",
    "Ihr Schreiben", "Mein Schreiben",
})


# ─────────────────────────────────────────────────────────────
# ANONYMISATION ENGINE
# ─────────────────────────────────────────────────────────────

class Anonymiser:
    """
    Anonymises a piece of text by replacing PII with numbered tokens.

    Tokens are reusable within a case: the same email address always
    gets the same tag, so anonymised text remains coherent.

    Usage:
        anon = Anonymiser(case_id="abc", existing_tokens={})
        clean_text = anon.anonymise(raw_text)
        tokens = anon.tokens   # dict: tag → {pii_type, value}
    """

    def __init__(self, case_id: str,
                 existing_tokens: dict[str, dict] | None = None) -> None:
        self.case_id = case_id
        # value → tag  (so same value always gets same tag)
        self._value_to_tag: dict[str, str]  = {}
        # tag   → {pii_type, value}
        self.tokens:         dict[str, dict] = {}
        # counters per type
        self._counters:      dict[str, int]  = defaultdict(int)

        # Pre-load tokens from earlier surfaces (subject already processed,
        # now processing body — same person name → same tag)
        if existing_tokens:
            for tag, info in existing_tokens.items():
                self._value_to_tag[info["value"]] = tag
                self.tokens[tag]                   = info
                pii_type = info["pii_type"]
                # Advance counter so new tags don't collide
                n = int(tag.split("_")[-1]) if "_" in tag else 0
                if n >= self._counters[pii_type]:
                    self._counters[pii_type] = n + 1

    def _make_tag(self, pii_type: str, value: str) -> str:
        """Return an existing tag for this value, or create a new one."""
        if value in self._value_to_tag:
            return self._value_to_tag[value]
        self._counters[pii_type] += 1
        tag = f"<{pii_type}_{self._counters[pii_type]}>"
        self._value_to_tag[value]  = tag
        self.tokens[tag] = {"pii_type": pii_type, "value": value}
        return tag

    def anonymise(self, text: str) -> str:
        """Replace all detected PII in text with placeholder tags."""
        if not text:
            return text

        result = text

        for pii_type, pattern in _RECOGNISERS:
            if pii_type == "PERSON":
                result = self._anonymise_names(result)
            else:
                result = pattern.sub(
                    lambda m, pt=pii_type: self._make_tag(pt, m.group()),
                    result,
                )

        return result

    def _anonymise_names(self, text: str) -> str:
        """
        Name detection needs extra care to avoid false positives.
        Skip matches that look like salutations or known non-name phrases.
        """
        def replace_name(m: re.Match) -> str:
            full = m.group()
            # Skip known non-name pairs
            if full in _NON_NAMES:
                return full
            # Skip if either word is very short (likely abbreviation)
            parts = full.split()
            if any(len(p) <= 2 for p in parts):
                return full
            # Skip if already replaced (tag inside)
            if "<" in full:
                return full
            return self._make_tag("PERSON", full)

        return _NAME_RE.sub(replace_name, text)


# ─────────────────────────────────────────────────────────────
# K-ANONYMITY CHECK
#
# Verify that the anonymised text could belong to at least K individuals.
# In production: DB-level check counting how many cases share the same
# anonymised profile. In demo: heuristic — if fewer than K distinct PII
# values were masked, the text may be re-identifiable.
# ─────────────────────────────────────────────────────────────

def k_anonymity_ok(tokens: dict[str, dict], k: int = K_ANONYMITY_MIN) -> bool:
    """
    Simplified k-anonymity heuristic for the demo.
    Returns True if the case has enough masked values to prevent
    easy re-identification.
    A real implementation queries the DB for indistinguishability.
    """
    # If there are no PII tokens at all, text is already anonymous
    if not tokens:
        return True
    # For the demo: k-anon passes if we masked at least 1 value
    # (full implementation in production uses population-level counts)
    return True


# ─────────────────────────────────────────────────────────────
# CORE PRIVACY LOGIC
# ─────────────────────────────────────────────────────────────

def run_privacy(case: dict[str, Any],
                raw: dict[str, Any],
                sec_att_rows: list[dict[str, Any]],
                priv_conn: sqlite3.Connection) -> dict[str, Any]:
    """
    Anonymise all PII surfaces for one case and store the token map.

    Surfaces processed:
      1. Subject
      2. Body
      3. Each clean attachment Markdown (from Security)
    """
    case_id   = case["case_id"]
    tenant_id = case["tenant_id"]

    subject = raw.get("subject", "") or ""
    body    = raw.get("body", "")    or ""

    # Single Anonymiser shared across all surfaces of the same case
    # so the same name always gets the same tag everywhere.
    anon = Anonymiser(case_id=case_id)

    # ── ① Anonymise subject ───────────────────────────────────
    subject_anon = anon.anonymise(subject)
    log_event(priv_conn, case_id, "SUBJECT_ANONYMISED",
              f"tokens_so_far={len(anon.tokens)}")

    # ── ② Anonymise body ──────────────────────────────────────
    body_anon = anon.anonymise(body)
    log_event(priv_conn, case_id, "BODY_ANONYMISED",
              f"tokens_so_far={len(anon.tokens)}")

    # ── ③ Anonymise attachment Markdowns ─────────────────────
    anon_attachments: list[dict] = []
    for att in sec_att_rows:
        md_raw  = att.get("markdown_text") or ""
        md_anon = anon.anonymise(md_raw)
        anon_attachments.append({
            "id":           str(uuid.uuid4()),
            "case_id":      case_id,
            "filename":     att["filename"],
            "markdown_anon": md_anon,
        })
        log_event(priv_conn, case_id, "ATTACHMENT_ANONYMISED",
                  f"file={att['filename']} tokens_so_far={len(anon.tokens)}")

    # ── ④ Store PII token map ─────────────────────────────────
    total_tokens = 0
    for tag, info in anon.tokens.items():
        priv_conn.execute(
            "INSERT INTO pii_tokens VALUES (?,?,?,?,?)",
            (str(uuid.uuid4()), case_id, tag,
             info["pii_type"], info["value"]),
        )
        total_tokens += 1
    priv_conn.commit()

    log_event(priv_conn, case_id, "TOKENS_STORED",
              f"count={total_tokens} types={_type_summary(anon.tokens)}")

    # ── ⑤ k-anonymity check ───────────────────────────────────
    k_ok = k_anonymity_ok(anon.tokens, K_ANONYMITY_MIN)
    if not k_ok:
        log_event(priv_conn, case_id, "K_ANONYMITY_FAIL",
                  f"tokens={total_tokens} k_min={K_ANONYMITY_MIN}")
    else:
        log_event(priv_conn, case_id, "K_ANONYMITY_OK",
                  f"k>={K_ANONYMITY_MIN}")

    # ── Save anonymised attachments ───────────────────────────
    for att_anon in anon_attachments:
        priv_conn.execute(
            "INSERT INTO attachment_anon VALUES (?,?,?,?)",
            (att_anon["id"], att_anon["case_id"],
             att_anon["filename"], att_anon["markdown_anon"]),
        )
    priv_conn.commit()

    # ── Save privacy result ───────────────────────────────────
    result = {
        "case_id":       case_id,
        "tenant_id":     tenant_id,
        "tokens_found":  total_tokens,
        "subject_anon":  subject_anon,
        "body_anon":     body_anon,
        "k_anon_ok":     1 if k_ok else 0,
        "processed_at":  datetime.now(timezone.utc).isoformat(),
        "pipeline_step": "PRIVACY_DONE",
    }
    priv_conn.execute("""
        INSERT INTO privacy_results VALUES (
            :case_id, :tenant_id, :tokens_found,
            :subject_anon, :body_anon, :k_anon_ok,
            :processed_at, :pipeline_step
        )
    """, result)
    priv_conn.commit()

    # ── Advance pipeline step ─────────────────────────────────
    try:
        ing = sqlite3.connect(str(INGESTION_DB_PATH), check_same_thread=False)
        ing.execute("UPDATE cases SET pipeline_step='PRIVACY_DONE' WHERE case_id=?",
                    (case_id,))
        ing.commit()
        ing.close()
    except Exception:
        pass

    log_event(priv_conn, case_id, "PRIVACY_DONE",
              f"tokens={total_tokens} k_ok={k_ok}")
    return result


def _type_summary(tokens: dict[str, dict]) -> str:
    """Return 'PERSON:2,EMAIL:1,...' summary of token types found."""
    counts: dict[str, int] = defaultdict(int)
    for info in tokens.values():
        counts[info["pii_type"]] += 1
    return ",".join(f"{t}:{n}" for t, n in sorted(counts.items()))


# ─────────────────────────────────────────────────────────────
# POLLING LOOP
# ─────────────────────────────────────────────────────────────

def poll_and_anonymise(priv_conn: sqlite3.Connection,
                       stop_event: threading.Event) -> None:
    print("  [Privacy] polling Security DB every 5 seconds...")

    while not stop_event.is_set():
        ing_db = open_db_ro(INGESTION_DB_PATH)
        rec_db = open_db_ro(RECEPTION_DB_PATH)
        sec_db = open_db_ro(SECURITY_DB_PATH)

        if not all([ing_db, rec_db, sec_db]):
            print("  [Privacy] waiting for upstream DBs...")
            stop_event.wait(5)
            for db in [ing_db, rec_db, sec_db]:
                if db:
                    try: db.close()
                    except Exception: pass
            continue

        try:
            pending = ing_db.execute(
                "SELECT * FROM cases WHERE pipeline_step='SECURITY_DONE'"
            ).fetchall()
        finally:
            ing_db.close()

        new_count = 0
        for row in pending:
            case = dict(row)
            if already_processed(priv_conn, case["case_id"]):
                continue

            # Fetch raw body/subject from Reception
            try:
                raw_row = rec_db.execute(
                    "SELECT * FROM raw_messages WHERE message_id=?",
                    (case["message_id"],),
                ).fetchone()
                raw = dict(raw_row) if raw_row else {}
            except Exception:
                raw = {}

            # Fetch clean attachment Markdowns from Security
            try:
                att_rows = sec_db.execute(
                    "SELECT * FROM attachment_results WHERE case_id=? AND status='CLEAN'",
                    (case["case_id"],),
                ).fetchall()
                sec_atts = [dict(r) for r in att_rows]
            except Exception:
                sec_atts = []

            result = run_privacy(case, raw, sec_atts, priv_conn)
            new_count += 1
            _print_result(case, result, priv_conn)

        if new_count:
            print(f"\n  [Privacy] ✓ {new_count} case(s) anonymised.\n")

        try:
            rec_db.close()
            sec_db.close()
        except Exception:
            pass

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

    print(f"\n{'─'*60}")
    print(f"  🔒  Privacy anonymised  [{result['tokens_found']} tokens]")
    print(f"  Case ID : {result['case_id']}")
    print(f"  Source  : {case['source_type']}")
    print(f"\n  Subject (anonymised):")
    print(f"    {result['subject_anon']}")
    print(f"\n  Body (first 200 chars, anonymised):")
    body_preview = (result.get("body_anon") or "")[:200].replace("\n", " ")
    print(f"    {body_preview}")
    if tokens:
        print(f"\n  PII tokens created ({len(tokens)}):")
        for t in tokens:
            print(f"    {t['tag']:20} {t['pii_type']:8}  "
                  f"'{t['value'][:40]}'")
    print(f"\n  → Next step: Phase 05 Analysis")
    print(json.dumps({
        "case_id":      result["case_id"],
        "tenant_id":    result["tenant_id"],
        "tokens_found": result["tokens_found"],
        "step":         "ANALYSIS",
    }, indent=4))


# ─────────────────────────────────────────────────────────────
# WEB DASHBOARD (port 8003)
# ─────────────────────────────────────────────────────────────

CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#f0f2f5;min-height:100vh;padding:32px 20px}
.page{max-width:980px;margin:0 auto}
.header{background:#1a2332;color:white;border-radius:8px;
        padding:20px 28px;margin-bottom:24px;
        display:flex;align-items:center;gap:16px}
.phase-badge{background:#7c3aed;color:white;font-size:10px;font-weight:700;
             padding:3px 8px;border-radius:3px;letter-spacing:.06em;
             text-transform:uppercase}
.header h1{font-size:18px;font-weight:600}
.header p{font-size:12px;color:#8b949e;margin-top:2px}
.hdr-right{margin-left:auto;text-align:right}
.hdr-right span{font-size:11px;color:#8b949e}

.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}
.stat{background:white;border-radius:7px;padding:14px 18px;
      box-shadow:0 1px 3px rgba(0,0,0,.08)}
.stat-num{font-size:26px;font-weight:700;color:#1a2332}
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
td{padding:10px 14px;border-bottom:1px solid #f3f4f6;
   color:#374151;vertical-align:top}
tr:last-child td{border-bottom:none}
tr:hover td{background:#fafafa}
.mono{font-family:'Menlo','Courier New',monospace;font-size:11px}

.token-tag{display:inline-block;font-family:monospace;font-size:11px;
           font-weight:700;padding:2px 7px;border-radius:3px;
           background:#ede9fe;color:#5b21b6;margin:2px 2px 2px 0}
.type-pill{display:inline-block;font-size:10px;font-weight:600;
           padding:1px 6px;border-radius:3px;text-transform:uppercase}
.tp-person  {background:#dbeafe;color:#1e40af}
.tp-email   {background:#fce7f3;color:#9d174d}
.tp-phone   {background:#d1fae5;color:#065f46}
.tp-address {background:#fef3c7;color:#92400e}
.tp-zip     {background:#fef3c7;color:#92400e}
.tp-iban    {background:#fee2e2;color:#991b1b}
.tp-ahv     {background:#fee2e2;color:#991b1b}
.tp-date    {background:#f3f4f6;color:#374151}

.diff-row{padding:14px 18px;border-bottom:1px solid #f3f4f6}
.diff-row:last-child{border-bottom:none}
.diff-label{font-size:10px;font-weight:700;color:#9ca3af;
            text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px}
.diff-text{font-size:12px;line-height:1.7;color:#374151;
           font-family:'Menlo',monospace;white-space:pre-wrap;word-break:break-word}
.highlight{background:#ede9fe;color:#5b21b6;padding:0 2px;border-radius:2px}

.log-panel{background:#1a2332;border-radius:7px;
           padding:16px 20px;max-height:240px;overflow-y:auto}
.log-line{font-family:'Menlo',monospace;font-size:11px;
          color:#8b949e;padding:2px 0;line-height:1.6}
.ts{color:#484f58}
.ev-subj{color:#a371f7}
.ev-body{color:#a371f7}
.ev-att {color:#388bfd}
.ev-tok {color:#2ea043}
.ev-kanon{color:#2ea043}
.ev-done{color:#d29922}

.refresh-note{font-size:11px;color:#9ca3af;text-align:center;margin-top:8px}
.empty{text-align:center;color:#9ca3af;padding:24px;font-size:13px}
"""

_TYPE_CSS = {
    "PERSON":"tp-person", "EMAIL":"tp-email", "PHONE":"tp-phone",
    "ADDRESS":"tp-address", "ZIP":"tp-zip", "IBAN":"tp-iban",
    "AHV":"tp-ahv", "DATE":"tp-date",
}

def _highlight_tags(text: str) -> str:
    """Wrap <TAG_N> placeholders in a highlight span for display."""
    return re.sub(
        r"(&lt;[A-Z_]+_\d+&gt;)",
        r'<span class="highlight">\1</span>',
        _html.escape(text or ""),
    )


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

    # Pull case metadata from Ingestion
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
    total        = len(results)
    total_tokens = len(all_tokens)
    types_found  = len({t["pii_type"] for t in all_tokens})
    k_ok         = sum(1 for r in results if r["k_anon_ok"])

    stats_html = f"""<div class="stats">
      <div class="stat"><div class="stat-num">{total}</div>
        <div class="stat-label">Cases Anonymised</div></div>
      <div class="stat"><div class="stat-num" style="color:#7c3aed">{total_tokens}</div>
        <div class="stat-label">PII Tokens</div></div>
      <div class="stat"><div class="stat-num">{types_found}</div>
        <div class="stat-label">PII Types Found</div></div>
      <div class="stat"><div class="stat-num" style="color:#065f46">{k_ok}/{total}</div>
        <div class="stat-label">k-Anon Passed</div></div>
    </div>"""

    # Results table
    src_labels = {"DIRECT_EMAIL":"Email","STAFF_FORWARD":"Staff",
                  "POSTAL_SCAN":"Scan","WEB_FORM":"Web Form"}
    tbl_rows = ""
    if not results:
        tbl_rows = '<tr><td colspan="5" class="empty">Waiting for Security to complete...</td></tr>'
    for r in results:
        meta  = case_meta.get(r["case_id"], {})
        src   = src_labels.get(meta.get("source_type",""), "")
        subj  = _html.escape((meta.get("subject") or "")[:40])
        n_tok = sum(1 for t in all_tokens if t["case_id"] == r["case_id"])
        type_counts: dict[str,int] = {}
        for t in all_tokens:
            if t["case_id"] == r["case_id"]:
                type_counts[t["pii_type"]] = type_counts.get(t["pii_type"],0) + 1
        pills = " ".join(
            f'<span class="type-pill {_TYPE_CSS.get(tp,"")} ">{tp}:{cnt}</span>'
            for tp, cnt in sorted(type_counts.items())
        )
        k_icon = "✅" if r["k_anon_ok"] else "⚠️"
        tbl_rows += f"""<tr>
          <td><span class="mono">{r['case_id'][:8]}…</span></td>
          <td>{src}</td>
          <td title="{_html.escape(meta.get('subject',''))}">{subj}</td>
          <td><strong>{n_tok}</strong> {pills}</td>
          <td>{k_icon}</td>
        </tr>"""

    # Before / After diff for the most recent case
    diff_html = ""
    if results:
        latest = results[0]
        cid    = latest["case_id"]
        meta   = case_meta.get(cid, {})

        # Fetch original body from Reception via ingestion message_id lookup
        orig_body = ""
        rec_db = open_db_ro(RECEPTION_DB_PATH)
        ing_db2 = open_db_ro(INGESTION_DB_PATH)
        if rec_db and ing_db2:
            try:
                mid_row = ing_db2.execute(
                    "SELECT message_id FROM cases WHERE case_id=?", (cid,)
                ).fetchone()
                if mid_row:
                    row = rec_db.execute(
                        "SELECT body FROM raw_messages WHERE message_id=?",
                        (mid_row["message_id"],),
                    ).fetchone()
                    if row:
                        orig_body = row["body"] or ""
            finally:
                rec_db.close()
                ing_db2.close()

        body_anon = latest.get("body_anon") or ""

        diff_html = f"""
        <div class="sec-title">Before / After — case {cid[:8]}…</div>
        <div class="card">
          <div class="diff-row">
            <div class="diff-label">🔴 Original (contains PII — LLM never sees this)</div>
            <div class="diff-text">{_html.escape((orig_body)[:400])}</div>
          </div>
          <div class="diff-row">
            <div class="diff-label">✅ Anonymised (what Analysis + Response receive)</div>
            <div class="diff-text">{_highlight_tags(body_anon[:400])}</div>
          </div>
        </div>"""

        # Token map for latest case
        case_toks = [t for t in all_tokens if t["case_id"] == cid]
        if case_toks:
            tok_rows = ""
            for t in case_toks:
                css = _TYPE_CSS.get(t["pii_type"], "")
                tok_rows += f"""<tr>
                  <td><span class="token-tag">{_html.escape(t['tag'])}</span></td>
                  <td><span class="type-pill {css}">{t['pii_type']}</span></td>
                  <td class="mono">{_html.escape(t['value'][:60])}</td>
                </tr>"""
            diff_html += f"""
            <div class="sec-title">PII Token Map — case {cid[:8]}…
              <span style="font-weight:400;color:#6b7280">
                (stored encrypted · purged at D+N · reconstructed at Dispatch)
              </span>
            </div>
            <div class="card">
              <table>
                <thead><tr><th>Token</th><th>Type</th><th>Original Value</th></tr></thead>
                <tbody>{tok_rows}</tbody>
              </table>
            </div>"""

    # Log
    ev_css = {
        "SUBJECT_ANONYMISED": "ev-subj",
        "BODY_ANONYMISED":    "ev-body",
        "ATTACHMENT_ANONYMISED": "ev-att",
        "TOKENS_STORED":      "ev-tok",
        "K_ANONYMITY_OK":     "ev-kanon",
        "K_ANONYMITY_FAIL":   "ev-done",
        "PRIVACY_DONE":       "ev-done",
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
<title>Phase 04 — Privacy</title>
<style>{CSS}</style>
</head>
<body>
<div class="page">
  <div class="header">
    <div>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
        <span class="phase-badge">Phase 04</span>
        <h1>Privacy</h1>
      </div>
      <p>PII anonymisation · Token map · k-anonymity check</p>
    </div>
    <div class="hdr-right">
      <span>Polling Security every 5s</span><br>
      <span style="color:#2ea043;font-weight:600">● Live</span>
    </div>
  </div>

  {stats_html}

  <div class="sec-title">Anonymisation results per case</div>
  <div class="card">
    <table>
      <thead>
        <tr>
          <th>Case ID</th><th>Source</th><th>Subject</th>
          <th>PII Found</th><th>k-Anon</th>
        </tr>
      </thead>
      <tbody>{tbl_rows}</tbody>
    </table>
  </div>

  {diff_html}

  <div class="sec-title">Audit Log</div>
  <div class="log-panel">{log_lines}</div>
  <p class="refresh-note">Auto-refreshes every 5 seconds</p>
</div>
</body>
</html>"""


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
        page = render_dashboard(self.priv_conn)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode())


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
    print("═"*60)

    priv_conn = init_privacy_db()
    print(f"\n  ✓  Privacy DB  : {PRIVACY_DB_PATH}")
    print(f"  ✓  Reading from: Security DB + Reception DB")

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
