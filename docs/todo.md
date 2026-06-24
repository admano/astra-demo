# ASTRA — Pre-Demo Hardening Notes

Findings from a pass over `reception.py`, `ingestion.py`, `security.py`, `privacy/`, `portal.py`, `templates/`, and `requirements.txt`. Grouped by what would actually hurt in front of stakeholders vs. what's longer-term cleanup.

## 1. Things that could break *live*

- [x] **HF NER model auto-downloads on first run (\~400 MB via** `transformers`**)**. Fixed in `privacy/detector.py`: model presence is now checked with cheap `importlib.util.find_spec` calls at import time (no package code runs), the real `spacy`/`presidio_analyzer`/`transformers` imports happen lazily, and each model is only loaded if it's already in the local HF cache (`_is_cached`, via `local_files_only=True` — zero network calls). Loading itself moved off the import path entirely, into two independent background daemon threads (`_load_analyzer` / `_load_hf_ner`, kept separate — running both sequentially in one thread deadlocked on this machine's torch/MPS backend). The Privacy dashboard now responds in ~0.1–0.2s, same as the other phases; `detect_pii()` already tolerated both engines being `None`, so requests served before the background load finishes just get fewer entities and quietly get richer once it's ready. Still worth rehearsing that early-request window once, since detection results change over the first few seconds of a fresh process.

- [ ] **Background pollers have no per-iteration error handling.** In `ingestion.py` (`poll_and_ingest`, \~line 344) and the equivalent loops in `security.py`/`privacy/privacy.py`, a single unexpected row or transient SQLite error raises out of the daemon thread and silently kills polling — the dashboard keeps responding (port stays "up") but new cases stop flowing through, with no visible error. Wrap each loop body in try/except + log, so a bad row can't quietly stall the pipeline mid-demo.

- [ ] `portal.py:_kill_port_occupant` **SIGKILLs whatever process owns ports 8000–8003 on the host**, with no check that it's actually a previous ASTRA run. On a shared or unfamiliar demo laptop this could kill an unrelated process. At minimum, confirm the process command line looks like one of ours before killing it.

- [ ] **All phases bind to** `0.0.0.0` (`("", PORT)` in each `main()`), not `127.0.0.1`. Harmless on a presenter's own laptop, but if the demo runs on a machine attached to a conference/venue network, every phase dashboard (including raw PII in the Privacy "Original" panel) is reachable by anyone else on that network. Bind to localhost unless remote access is actually needed, or run behind a firewall rule for the demo.

## 2. Privacy-specific (this phase's correctness *is* the pitch)

- [ ] **The Privacy dashboard intentionally shows original PII next to its pseudonym** (`templates/privacy_dashboard.html` "🔴 Original" panel + token table) — good for proving detection works, but be ready for the obvious stakeholder question: in production, who can see that panel? Worth a one-line caveat on the slide: production would gate the reveal view behind an authorization/audit-logged action, not show it by default.

- [ ] **Vault is in-memory only** (`privacy/vault.py`: `PseudonymVault`, docstring already says *"Session TTL not implemented (demo) — production would use Redis + TTL"*). A process restart loses all live sessions; only whatever already got persisted into `pii_tokens` survives. If Dispatch (phase 12) is built to deanonymize via the live vault rather than the DB table, decide now which one is the source of truth — don't let it be decided implicitly by which one happens to still have the data.

- [ ] **No encryption-at-rest for** `demo_privacy.db`**.** `pii_tokens.original_value`stores real PII in plaintext SQLite right next to the anonymized data it's supposed to protect. Fine for a demo DB that gets deleted after, but call this out explicitly as a known gap rather than letting it look like an oversight if someone opens the `.db` file.

## 3. Repo hygiene

- [ ] `__pycache__/decomposition.cpython-314.pyc` **is committed to git**(`git ls-files | grep __pycache__`) even though `decomposition.py` itself doesn't exist in the repo. It predates `.gitignore` and was never untracked. Run `git rm --cached __pycache__/decomposition.cpython-314.pyc`.

- [ ] `templates/privacy_dashboard_v1.html` **is a stale, superseded copy**of `privacy_dashboard.html` (123 lines vs. 252; missing risk-color classes and the clickable detail-panel added later). Delete it or move it under a `templates/archive/` if it's kept for reference — having two similarly-named templates in the active `templates/` dir is an easy mis-edit waiting to happen.

- [ ] `requirements.txt` **lists** `fastapi`**,** `uvicorn`**,** `pydantic` but no phase actually imports them — everything runs on stdlib `http.server`. Either trim them (smaller, faster install before a demo) or, if there's a real plan to move phases onto FastAPI, say so in the file instead of leaving it looking like a leftover from an earlier prototype.

- [ ] **Minor formatting inconsistency**: path joins like `DATABASE_DIR /"demo_db" / "demo_reception.db"` (missing space after the first `/`, e.g. `reception.py:46`) repeat across `reception.py`, `ingestion.py`, `security.py`, `privacy/privacy.py`. Not a bug, but a one-pass `black`/`ruff format` would clean this up before any stakeholder sees the source.

- [ ] **Mixed import styles inside** `privacy/` — `pipeline.py`/`detector.py`use qualified `from privacy.X import ...`, while `anonymizer.py`/ `scorer.py` use unqualified `from X import ...`. Both work today only because of the manual `sys.path` insert in `privacy/privacy.py` (see `CLAUDE.md`), but it's fragile — pick one style and make it consistent so the package also imports cleanly from a future test suite or another phase without the script-launch trick.

- [ ] `reception.py:render_inbox` does `import html as _html` *inside* the per-message loop instead of once at module level — works, just an odd pattern worth tidying while touching that function.

## 4. Observability

- [ ] **No** `logging` **module usage** — every phase uses bare `print()` (113 occurrences across the four phase files). Fine for a single-terminal demo, but there's no log level, no timestamps on most lines, and nothing written to a file. If the demo involves any "let's check what happened" moment, a presenter is stuck scrolling unstructured stdout. Even a minimal `logging.basicConfig(...)` swap would make Q&A debugging faster.

- [ ] `portal.py` **redirects every phase's stdout/stderr to** `DEVNULL`(`subprocess.Popen(..., stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)`). If a phase fails to start or crashes after launch, the portal shows "down" with zero diagnostic info — whoever's running the demo has to kill the portal and run the phase standalone to see why. Consider redirecting to per-phase log files under a `logs/` dir instead of `DEVNULL`, so a failure mid-demo is debuggable without restarting everything.

## 5. Testing

- [ ] **There are no automated tests in the repo** (no `tests/`, no `test_*.py`, no `pytest.ini`/`pyproject.toml`), despite `pytest` and `httpx` being listed under "Testing" in `requirements.txt`. Before presenting to stakeholders, even a handful of smoke tests would catch regressions that currently only surface by clicking through the UI: - one test per phase asserting `init_*_db()` creates the expected tables, - one end-to-end test pushing a message through Reception → Ingestion → Security → Privacy and asserting `pipeline_step` advances and no real PII survives in `privacy_results.body_anon`.

## 6. Scope honesty for the stakeholder deck

- [ ] **Phases 5–12 don't exist yet** — `portal.py`'s `PHASES` list references `analysis.py`, `decomposition.py`, `prompt_enrichment.py`, `response.py`, `quality.py`, `recomposition.py`, `validation.py`, `dispatch.py`, none of which are present. The portal already greys these out via `DEMO_ACTIVE_PHASES = {1, 2, 3, 4}`, which is good — just make sure the narrative explicitly frames this as "phases 1–4 are live, 5–12 are the roadmap" rather than letting the greyed-out sidebar speak for itself.