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
