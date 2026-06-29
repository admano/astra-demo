# Fixed Actions

Log of concrete fixes applied to the codebase, with what changed and why.

## Privacy phase: slow/blocking HF model loading on startup

**File:** `privacy/detector.py`

**Problem:** The Privacy phase (port 8003) was slow to come up compared to the
other phases — `spacy`, `presidio_analyzer`, and `transformers` were imported
eagerly at module load, and two HuggingFace models (`dslim/bert-base-NER` for
Presidio's NLP backend, `Davlan/bert-base-multilingual-cased-ner-hrl` for
standalone NER) were built synchronously before the HTTP server could start.
If either model wasn't already cached locally, this also risked a live,
multi-hundred-MB network download blocking the dashboard from ever appearing.

**Fix:**
1. **Cheap presence checks, lazy real imports** — `_SPACY_OK` / `_PRESIDIO_OK` /
   `_TRANSFORMERS_OK` now use `importlib.util.find_spec(...)` (no package code
   runs) instead of actually importing `spacy` / `presidio_analyzer` /
   `transformers` at module load. The real imports happen lazily inside the
   functions that need them — including moving the `swiss_recognizers` import
   (which itself eagerly imports `presidio_analyzer`) into `build_analyzer()`.
2. **Cache-gated model loading** — new `_is_cached(model_name)` checks the
   local HuggingFace cache via `snapshot_download(..., local_files_only=True)`
   (zero network calls). A model only loads if it's already cached; otherwise
   it's skipped rather than downloaded.
3. **Loading deferred to background threads** — `_ANALYZER` / `_HF_NER` start
   as `None` and are populated by two *separate* daemon threads
   (`_load_analyzer`, `_load_hf_ner`). Running both builds sequentially in a
   single shared thread was found to deadlock on this machine's PyTorch MPS
   backend; splitting them into independent threads fixed it. `detect_pii()`
   needed no changes — it already treats both engines as optional, so
   requests served before background loading finishes just get fewer
   detected entities and quietly get richer once it's ready.

**Result:** Module import time dropped from ~13s to ~0.02s. The real phase
process now serves its dashboard in ~0.1–0.2s — on par with the other three
phases — verified by launching `privacy/privacy.py` and curling it. Both
engines still finish loading in the background within a few seconds, and
detection correctly upgrades from empty/partial results to full
Presidio+HF-NER results once ready.

**Caveat:** Detection results during the first few seconds of a fresh process
(before background loading completes) will be weaker than once both engines
are warm — worth rehearsing that window once before a live demo.

## Pipeline: consolidated to one shared SQLite DB + HTTP push between phases

**Files:** `reception.py`, `ingestion.py`, `security.py`, `privacy/privacy.py`

**Change requested:** replace the four per-phase SQLite files
(`demo_reception.db`, `demo_ingestion.db`, `demo_security.db`,
`demo_privacy.db`) with one shared file, and replace poll-only chaining
between phases with HTTP calls — keeping the same overall logic/design and
without removing the safety net polling provided.

**What changed:**
1. All four phases now connect to one `demo_db/demo_pipeline.db`. Each phase
   still owns its own tables, but reads upstream tables directly through its
   own connection instead of opening a second read-only connection to a
   different file (`open_reception_db`, `open_db`, `open_db_ro` all removed).
2. Each phase's polling-loop body was extracted into a reusable
   `process_pending_<phase>()` function, guarded by a `threading.Lock()`.
   Every phase now also exposes `POST /notify`, which runs that same function
   immediately. After writing new data, each phase fires a fire-and-forget
   `POST /notify` at the next phase (`_notify_next_phase()`, stdlib
   `urllib.request`, 2s timeout, failures swallowed). The original 5s polling
   loop is kept — unchanged in spirit — as a fallback in case a push is
   missed (e.g. the downstream phase was briefly down).
3. Multiple processes now hit one file, so `_connect_shared_db()` sets
   `PRAGMA journal_mode=WAL` + `busy_timeout=5000` and retries the connect a
   few times.

### Per-file breakdown

**`reception.py`**
- `DB_PATH` repointed from `demo_db/demo_reception.db` to the shared
  `demo_db/demo_pipeline.db`.
- New `_connect_shared_db(db_path)` replaces the inline `sqlite3.connect(...)`
  in `init_db()`: opens the file, sets `row_factory`, then
  `PRAGMA journal_mode=WAL` + `PRAGMA busy_timeout=5000`, retrying up to 10
  times on `sqlite3.OperationalError` (the startup-race fix, see below).
- New `INGESTION_NOTIFY_URL = "http://localhost:8001/notify"` and
  `_notify_next_phase(url)` — spawns a daemon thread that does a best-effort
  `urllib.request.urlopen(Request(url, method="POST"), timeout=2)`, swallowing
  all exceptions.
- `save_message()` now calls `_notify_next_phase(INGESTION_NOTIFY_URL)` right
  after `conn.commit()`, for every message regardless of source (web form or
  the 3 startup samples).

**`ingestion.py`**
- `RECEPTION_DB_PATH` + `INGESTION_DB_PATH` collapsed into one `DB_PATH`.
- `open_reception_db()` removed; replaced by
  `_raw_messages_table_exists(conn)`, a one-line
  `SELECT 1 FROM sqlite_master WHERE type='table' AND name='raw_messages'`
  check (no second connection needed now that it's the same file).
- `poll_and_ingest()`'s body was extracted into
  `process_pending_ingestion(ing_conn) -> int`, wrapped in a module-level
  `_PROCESS_LOCK = threading.Lock()` so the poll timer and the new `/notify`
  handler can't run it concurrently. It calls
  `_notify_next_phase(SECURITY_NOTIFY_URL)` once per pass if `new_count > 0`.
  `poll_and_ingest()` is now a thin `while` loop calling that function every 5s.
- `IngestionDashboardHandler` gained a `do_POST` handling `POST /notify` →
  calls `process_pending_ingestion(self.ing_conn)`, returns `204`.
- `SECURITY_NOTIFY_URL = "http://localhost:8002/notify"` added; same
  `_notify_next_phase()` helper duplicated (each phase script is
  self-contained, matching the existing per-file `_load_template` pattern).

**`security.py`**
- `RECEPTION_DB_PATH` + `INGESTION_DB_PATH` + `SECURITY_DB_PATH` collapsed
  into one `DB_PATH`.
- `open_db(path)` removed; replaced by `_table_exists(conn, name)` (generic
  version, takes a table name instead of a path).
- `run_security()`'s "advance pipeline step" block no longer opens a second
  `sqlite3.connect(INGESTION_DB_PATH)` — it just runs
  `sec_conn.execute("UPDATE cases SET pipeline_step=...")` on the connection
  it already has.
- `poll_and_check()`'s body was extracted into
  `process_pending_security(sec_conn) -> int` (same lock pattern as
  ingestion). It tracks `any_cleared` across the batch and calls
  `_notify_next_phase(PRIVACY_NOTIFY_URL)` once per pass only if at least one
  case verdict was `CLEAN` (escalated cases must not reach Privacy — mirrors
  the existing `pipeline_step` filter Privacy's poller already used).
- `render_dashboard()`'s case-metadata lookup now queries `sec_conn` directly
  instead of opening `INGESTION_DB_PATH` read-only.
- `SecurityDashboardHandler` gained `do_POST` for `/notify`.

**`privacy/privacy.py`**
- `RECEPTION_DB_PATH` / `INGESTION_DB_PATH` / `SECURITY_DB_PATH` /
  `PRIVACY_DB_PATH` collapsed into one `DB_PATH`.
- `open_db_ro(path)` removed; replaced by `_table_exists(conn, name)`.
- `run_privacy()`'s "advance pipeline step" block simplified the same way as
  security.py — `priv_conn.execute(...)` directly, no second connection.
- `poll_and_anonymise()`'s body was extracted into
  `process_pending_privacy(priv_conn) -> int` (same lock pattern).
  No outbound notify call — Phase 05 (Analysis) doesn't exist yet; left a
  comment for where to add it.
- `_serve_case()` (the `/case/<id>` JSON endpoint used by the dashboard's
  detail panel) used to open two extra read-only connections
  (`RECEPTION_DB_PATH`, `INGESTION_DB_PATH`) just to join `cases` →
  `raw_messages` for the original body text; now both queries run on
  `self.priv_conn` directly.
- `render_dashboard()`'s case-metadata lookup simplified the same way.
- `PrivacyDashboardHandler` gained `do_POST` for `/notify`.

**`privacy/anonymizer.py` / `privacy/scorer.py`** (fixed during testing, see below)
- Sibling imports changed from unqualified (`from vault import PseudonymVault`,
  `from detector import RawEntity`) to qualified (`from privacy.vault import
  PseudonymVault`, `from privacy.detector import RawEntity`).
- `anonymizer.py`'s broken `from venv import logger` replaced with
  `import logging; logger = logging.getLogger(__name__)`. Also dropped an
  unused `from models import PrivacyRunRequest` import while in there.

**Bug hit during testing, and fixed:** with all four phases starting at once
against the new shared file, switching a brand-new file to WAL mode briefly
needs an exclusive lock — two phases raced for it and **ingestion.py and
security.py crashed at startup** with `sqlite3.OperationalError: database is
locked` (this is what showed up as ingestion's dashboard being blank with no
mock data — the process had never actually started). Fixed by the
retry-with-backoff in `_connect_shared_db()`.

**Separate pre-existing bug found and fixed while testing:** the Privacy
dashboard's "PII Tokens" stat read 0 even when anonymization clearly worked.
Root cause: `privacy/privacy.py`, `anonymizer.py`, and `scorer.py` imported
sibling modules unqualified (`from vault import vault`), which made Python
load `vault`/`models`/`detector` as second module objects distinct from the
qualified `privacy.vault` etc. used by `pipeline.py` — i.e. **two separate
`PseudonymVault()` singletons**. Pseudonyms were written into one instance and
read back from the other, which was always empty. Fixed by making every
sibling import inside `privacy/` qualified (`from privacy.X import ...`),
confirmed with `vault_unqualified.vault is vault_qualified.vault` flipping
from `False` to a single shared instance. Also fixed a broken
`from venv import logger` in `anonymizer.py` (should never have resolved to a
real logger) while touching that import block.

**Result:** verified end-to-end — fresh boot of all four phases, the 4 mock
messages flow automatically from Reception to Privacy with zero manual
steps; a newly submitted web-form case propagated Reception→Privacy in
1.13s (via push, not the 5s fallback); `pii_tokens` now populates correctly
(21 tokens across 4 mock cases, including real names like "Hans Muster" and
"Marie Dupont").
