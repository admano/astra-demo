# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

ASTRA is a demo of a 12-phase citizen-communication pipeline for a Swiss public-sector
context (citizen emails/letters/forms → security/privacy processing → AI-assisted
response → dispatch). Each phase is a standalone Python script that simulates what a
production microservice would do; LLM calls are simulated with rule-based heuristics
except in the Privacy phase, which uses a real Presidio-based detection engine.

Only **phases 1–4 are implemented**; phases 5–12 are placeholders referenced by
`portal.py` but their scripts (`analysis.py`, `decomposition.py`, etc.) do not exist yet.

## Running it

There is no build step and no test suite. Everything is plain stdlib `http.server` +
`sqlite3`, except the Privacy phase which needs the packages in `requirements.txt`.

```bash
pip install -r requirements.txt          # only required for Phase 4 (Privacy)
python3 -m spacy download en_core_web_sm # one-time, tokenizer only — needed by Presidio

python3 portal.py                        # runs everything: http://localhost:8080
```

`portal.py` launches each active phase as a subprocess, kills any stale process
already bound to a phase's port before starting it, and serves a sidebar+iframe
portal UI. `GET /status` returns JSON of each phase's up/down state.

Phases can also be run standalone, in order (each depends on the previous phase's
SQLite DB existing), useful when iterating on a single phase without going through
the portal:

```bash
python3 reception.py     # port 8000
python3 ingestion.py     # port 8001
python3 security.py      # port 8002
python3 privacy/privacy.py  # port 8003
```

There are no automated tests in this repo currently.

## Architecture

### Phase script pattern

Every phase file (`reception.py`, `ingestion.py`, `security.py`, `privacy/privacy.py`)
follows the same shape:

- A module-level `init_<phase>_db()` (via the shared `_connect_shared_db()` helper
  duplicated in each file) opens the **one shared** SQLite file at
  `demo_db/demo_pipeline.db` and `CREATE TABLE IF NOT EXISTS` its own tables in it.
- An `http.server.BaseHTTPRequestHandler` subclass serves a dashboard at the
  phase's port, rendering HTML via `string.Template` substitution from files in
  `templates/` (no Jinja/Flask — just `Template(...).substitute(...)`), and a
  `do_POST` handler for `/notify` (see below).
- A `process_pending_<phase>()` function does the actual work (reads rows left
  by the *previous* phase, advances each case's `pipeline_step` column once
  done). It's reused by both the `/notify` handler and a 5s polling loop, and
  guarded by a module-level `threading.Lock()` since both can call it
  concurrently.

### Cross-phase wiring: shared SQLite + HTTP push, polling as fallback

All four phases open the **same** `demo_db/demo_pipeline.db` file — there's no
per-phase DB anymore. Each phase still owns its own tables (`raw_messages`,
`cases`/`ingested_ids`, `security_results`/`attachment_results`,
`privacy_results`/`pii_tokens`) and reads upstream tables directly via the same
connection (no more opening a second read-only connection to another file).
`pipeline_step` on the shared `cases` table is still the state marker:

```
raw_messages  →  cases.pipeline_step = INGESTION
              →  pipeline_step = SECURITY_DONE | SECURITY_ESCALATE
              →  pipeline_step = PRIVACY_DONE | PRIVACY_BLOCKED
```

Because four processes hit one file, `_connect_shared_db()` sets
`PRAGMA journal_mode=WAL` + `busy_timeout=5000` and retries the initial connect
a few times — switching a brand-new file to WAL needs a brief exclusive lock,
so when all four phases start at once the very first connect can race and hit
`database is locked` otherwise.

Instead of waiting on the old 5s poll tick, each phase fires a fire-and-forget
HTTP `POST /notify` at the next phase right after writing new data
(`_notify_next_phase()`, stdlib `urllib.request`, 2s timeout, failures
swallowed). The 5s polling loop is kept as a fallback in case a notify call is
missed (downstream phase briefly down, etc.) — so the pipeline is push-driven
in the common case but self-heals like before if a push fails. Privacy has no
downstream notify yet since Phase 05 (Analysis) doesn't exist.

`demo_pipeline.db` is gitignored — it's regenerated on each run and holds
whatever demo data you fed through Reception's web form.

### portal.py

- `PHASES`: ordered `(num, name, port, script_path)` tuples — the source of truth
  for the full 12-phase pipeline, including unimplemented phases.
- `DEMO_ACTIVE_PHASES = {1, 2, 3, 4}`: gates which phases actually get spawned and
  clickable in the sidebar; the rest render disabled/greyed out. Bump this set as
  more phases get implemented.
- Sidebar/landing HTML is assembled from `templates/portal.html` and
  `templates/landing.html` (the latter has an inline `<!-- pipeline_step_tmpl -->`
  block that gets extracted and `.format()`-filled per phase).

### Privacy phase (`privacy/`)

`privacy/privacy.py` is the phase entry point (port 8003); the actual detection/
anonymization logic lives in the `privacy` package alongside it:

- `detector.py` — Presidio `AnalyzerEngine` + custom Swiss recognizers
  (`swiss_recognizers.py`: AHV number, Swiss IBAN/phone/UID/ZIP) + optional
  HuggingFace NER, all behind `try/except ImportError` so the package degrades to
  a regex-only fallback when optional deps aren't installed.
- `anonymizer.py` — turns detected entities into context-preserving pseudonyms
  (e.g. `"Anna Müller"` → `"Person_A1"`) rather than opaque tokens, so downstream
  LLM phases keep semantic context without seeing real PII.
- `vault.py` — `PseudonymVault`, a session-scoped, reversible pseudonym↔original
  map. Deanonymization (at Dispatch, phase 12) needs only `session_id` + AI text.
- `scorer.py` — residual-risk scoring: it scores what *leaked through* into the
  anonymized output (verbatim originals, failed pseudonyms), not the raw
  sensitivity of detected input. Thresholds in `pipeline.py`'s `PRIVACY_RULES`
  decide `safe` / `escalated` / `blocked`.
- `pipeline.py` — `run_privacy_pipeline()`, the orchestrator: identify input →
  normalize → apply rules → detect → pseudonymize → risk check → output.

**Import gotcha (fixed, keep it that way):** every module inside `privacy/`
imports its siblings with the qualified `privacy.` prefix (`from privacy.vault
import vault`), including `privacy/privacy.py` itself. This used to be mixed —
`privacy.py`, `anonymizer.py` and `scorer.py` used unqualified imports (`from
vault import vault`) — which silently loaded `vault`/`models`/`detector` as
*second, separate module objects* distinct from `privacy.vault` etc. Two module
objects meant two `PseudonymVault()` singletons: `pipeline.py` pseudonymized
into one instance while `privacy.py` read `pii_tokens` back from the other,
so `tokens_found`/`pii_tokens` were silently always empty even though
anonymization itself worked. If you add a new submodule to `privacy/`, import
its siblings with the qualified `privacy.` prefix — an unqualified import will
quietly reintroduce this bug rather than raising an error.
