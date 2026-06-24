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

- A module-level `init_<phase>_db()` creates/opens that phase's own SQLite file
  under `demo_db/` (e.g. `demo_db/demo_ingestion.db`) and `CREATE TABLE IF NOT EXISTS`.
- An `http.server.BaseHTTPRequestHandler` subclass serves a dashboard at the
  phase's port, rendering HTML via `string.Template` substitution from files in
  `templates/` (no Jinja/Flask — just `Template(...).substitute(...)`).
- A polling loop processes rows left by the *previous* phase and advances each
  case's `pipeline_step` column once done.

### Cross-phase state machine

Phases are chained through SQLite, not HTTP calls. Each phase reads rows from the
upstream phase's DB filtered by a `pipeline_step` marker, and writes its own
results plus an updated `pipeline_step` for the next phase to pick up:

```
demo_reception.db          (raw_messages)
   → demo_ingestion.db      cases.pipeline_step = INGESTION
       → demo_security.db   pipeline_step = SECURITY_DONE
           → demo_privacy.db pipeline_step = PRIVACY_DONE
```

`*.db` files are gitignored — they're regenerated on each run and hold whatever
demo data you fed through Reception's web form.

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

**Import gotcha:** modules inside `privacy/` are imported two different ways
depending on caller. `privacy/privacy.py` manually inserts the repo root onto
`sys.path` and does unqualified imports (`from pipeline import ...`) that resolve
against `privacy/` itself (Python puts a script's own directory at `sys.path[0]`).
But `pipeline.py` and `detector.py` import their siblings with the qualified
`privacy.` prefix (`from privacy.models import ...`), which only resolves because
the repo root was put on `sys.path`. `anonymizer.py` and `scorer.py`, on the other
hand, use unqualified sibling imports (`from models import ...`,
`from detector import ...`). Both styles currently work together, but only because
of this specific dual-path setup — don't "clean up" the imports to be consistent
without verifying both entry points (`privacy/privacy.py` as a script, and
`privacy.pipeline` imported as a package from elsewhere) still resolve.
