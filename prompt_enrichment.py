"""
demo/prompt_enrichment.py
--------------------------
Phase 07: Prompt Enrichment  (DEMO VERSION)

For each IN_SCOPE question, enriches the prompt with two sources:
  ① KB similarity search   — validated Q/A pairs from the Knowledge Base
  ② Citizen documents      — relevant passages from the anonymised
                              attachment Markdowns for this case

The enriched prompt is stored in questions.enriched_prompt.
Phase 08 (Response) uses enriched_prompt instead of the raw question.

Key rule from the spec:
  A previous KB answer is NEVER reused directly. It is provided as
  CONTEXT ONLY, with an explicit warning that the law may have changed.
  Response always generates a fresh answer.

In production:
  - pgVector cosine similarity search against two indexes:
      • kb_entries (long-lived, per tenant+theme)
      • attachment_anon (short-lived, scoped to case)
  - similarity_threshold configurable per tenant

In demo:
  - TF-IDF style cosine similarity on word vectors (stdlib only)
  - Pre-seeded KB with realistic ASTRA Q/A pairs per theme
  - Same prompt structure as production

Run:
    python3 demo/reception.py           ← port 8000
    python3 demo/ingestion.py           ← port 8001
    python3 demo/security.py            ← port 8002
    python3 demo/privacy.py             ← port 8003
    python3 demo/analysis.py            ← port 8004
    python3 demo/decomposition.py       ← port 8005
    python3 demo/prompt_enrichment.py   ← port 8006

Dashboard: http://localhost:8006
"""

from __future__ import annotations

import html as _html
import http.server
import json
import math
import re
import sqlite3
import threading
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ─────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────

DEMO_DIR          = Path(__file__).parent
INGESTION_DB_PATH = DEMO_DIR / "demo_ingestion.db"
PRIVACY_DB_PATH   = DEMO_DIR / "demo_privacy.db"
DECOMP_DB_PATH    = DEMO_DIR / "demo_decomposition.db"
ENRICHMENT_DB_PATH = DEMO_DIR / "demo_enrichment.db"
PORT              = 8006

# Similarity threshold — questions scoring below this get no KB context
# (configurable per tenant in production)
SIMILARITY_THRESHOLD = 0.15


# ─────────────────────────────────────────────────────────────
# KNOWLEDGE BASE  (pre-seeded ASTRA Q/A pairs)
#
# In production: kb_entries table in PostgreSQL with pgVector index.
# Entries are inserted by Phase 12 (Dispatch) after each validated case.
#
# In demo: hard-coded realistic Q/A pairs per ASTRA theme.
# Each entry mirrors the kb_entries schema exactly.
# ─────────────────────────────────────────────────────────────

SEED_KB: list[dict[str, str]] = [
    # ── DRIVERS_LICENSE ──────────────────────────────────────
    {
        "theme":    "DRIVERS_LICENSE",
        "question": "Wie kann ich einen Ersatz für meinen verlorenen Führerausweis beantragen?",
        "answer": (
            "Um einen Ersatzausweis zu beantragen, wenden Sie sich an das "
            "Strassenverkehrsamt (StVA) Ihres Wohnkantons. Sie benötigen: "
            "ein aktuelles Passfoto, einen gültigen Identitätsausweis und die "
            "Antragsgebühr (ca. CHF 20–30). Der Ersatzausweis wird in der Regel "
            "innerhalb von 5–10 Arbeitstagen ausgestellt."
        ),
    },
    {
        "theme":    "DRIVERS_LICENSE",
        "question": "Comment renouveler mon permis de conduire en Suisse?",
        "answer": (
            "Le renouvellement du permis de conduire se fait auprès de l'Office "
            "cantonal de la circulation (OCC) de votre canton de domicile. "
            "Documents requis: une photo d'identité récente, une pièce d'identité "
            "valide et, pour certaines catégories, un certificat médical. "
            "Le délai de traitement est généralement de 5 à 10 jours ouvrables."
        ),
    },
    {
        "theme":    "DRIVERS_LICENSE",
        "question": "Was ist der aktuelle Bearbeitungsstand meines Führerausweis-Antrags?",
        "answer": (
            "Den Bearbeitungsstand Ihres Antrags können Sie direkt beim zuständigen "
            "Strassenverkehrsamt Ihres Kantons erfragen. ASTRA verwaltet keine "
            "kantonalen Führerausweise — diese liegen in der Zuständigkeit der "
            "kantonalen StVA. Bitte nehmen Sie direkt mit Ihrem kantonalen Amt Kontakt auf."
        ),
    },
    # ── ROAD_INFRASTRUCTURE ───────────────────────────────────
    {
        "theme":    "ROAD_INFRASTRUCTURE",
        "question": "Wie melde ich Schäden oder Mängel an Nationalstrassen?",
        "answer": (
            "Schäden an Nationalstrassen (A-Strassen) können Sie über das ASTRA-Kontaktformular "
            "auf www.astra.admin.ch melden oder den Notruf 140 anrufen. "
            "Für dringende Sicherheitsmängel steht rund um die Uhr der Pikettdienst "
            "der zuständigen Gebietseinheit zur Verfügung."
        ),
    },
    {
        "theme":    "ROAD_INFRASTRUCTURE",
        "question": "Wer ist zuständig für die Instandhaltung der Fahrbahnmarkierungen auf Autobahnen?",
        "answer": (
            "Die Fahrbahnmarkierungen auf Nationalstrassen liegen in der Zuständigkeit "
            "der ASTRA-Gebietseinheiten. Bei Schäden an Markierungen auf der A1, A2 oder "
            "anderen Nationalstrassen ist das ASTRA direkt verantwortlich. Bitte melden "
            "Sie den genauen Ort (Strasse, Kilometer, Fahrtrichtung)."
        ),
    },
    # ── NOISE_PROTECTION ──────────────────────────────────────
    {
        "theme":    "NOISE_PROTECTION",
        "question": "Welche Massnahmen ergreift ASTRA zum Lärmschutz entlang von Nationalstrassen?",
        "answer": (
            "ASTRA realisiert Lärmschutzmassnahmen im Rahmen des Programms "
            "'Lärmsanierung Nationalstrassen'. Dies umfasst: Lärmschutzwände, "
            "Lärmschutzwälle, lärmarme Beläge (Flüsterasphalt) und bauliche "
            "Massnahmen an betroffenen Gebäuden. Ansprüche können bei der zuständigen "
            "ASTRA-Gebietseinheit geltend gemacht werden."
        ),
    },
    {
        "theme":    "NOISE_PROTECTION",
        "question": "Wie lange dauert die Bearbeitung einer Anfrage zu Lärmschutzmassnahmen?",
        "answer": (
            "Anfragen zu Lärmschutzmassnahmen werden in der Regel innerhalb von "
            "20 Arbeitstagen beantwortet. Bei komplexen Sachverhalten mit technischen "
            "Abklärungen kann die Bearbeitungszeit auf bis zu 60 Tage verlängert werden. "
            "Sie erhalten eine Eingangsbestätigung mit der Referenznummer Ihres Falls."
        ),
    },
    # ── TUNNEL_SAFETY ─────────────────────────────────────────
    {
        "theme":    "TUNNEL_SAFETY",
        "question": "Welche Sicherheitsvorschriften gelten in Schweizer Strassentunneln?",
        "answer": (
            "In Schweizer Strassentunneln gelten folgende Hauptregeln: "
            "Mindestabstand 150m einhalten, Licht einschalten, Tempolimit beachten, "
            "bei Panne Warnblinklicht einschalten und Pannenbuchten nutzen, "
            "im Brandfall sofort aus dem Fahrzeug aussteigen und Notausgang benutzen. "
            "Tunnelgesperrte Spuren sind strikt einzuhalten."
        ),
    },
    # ── GENERAL_INQUIRY ───────────────────────────────────────
    {
        "theme":    "GENERAL_INQUIRY",
        "question": "Wie kann ich ASTRA kontaktieren?",
        "answer": (
            "ASTRA ist erreichbar unter: "
            "Web: www.astra.admin.ch | "
            "E-Mail: info@astra.admin.ch | "
            "Telefon: +41 58 464 14 14 | "
            "Post: ASTRA, 3003 Bern. "
            "Für Notfälle auf Nationalstrassen: Notruf 140."
        ),
    },
]


# ─────────────────────────────────────────────────────────────
# TF-IDF COSINE SIMILARITY
#
# In production: pgVector cosine similarity on dense embeddings.
# In demo: sparse TF-IDF bag-of-words cosine similarity.
#          Good enough to find relevant KB entries from keyword overlap.
# ─────────────────────────────────────────────────────────────

def _tokenise(text: str) -> list[str]:
    """Lowercase, strip punctuation, split on whitespace."""
    text = text.lower()
    text = re.sub(r"[^\w\säöüéàèùâêîôûäöüß]", " ", text)
    return [t for t in text.split() if len(t) > 2]


def _tf(tokens: list[str]) -> dict[str, float]:
    counts = Counter(tokens)
    total  = len(tokens) or 1
    return {t: c / total for t, c in counts.items()}


def _tfidf_vector(tokens: list[str],
                  idf: dict[str, float]) -> dict[str, float]:
    tf = _tf(tokens)
    return {t: tf[t] * idf.get(t, 1.0) for t in tf}


def cosine_similarity(a: dict[str, float], b: dict[str, float]) -> float:
    """Cosine similarity between two sparse TF-IDF vectors."""
    keys = set(a) & set(b)
    if not keys:
        return 0.0
    dot     = sum(a[k] * b[k] for k in keys)
    norm_a  = math.sqrt(sum(v * v for v in a.values()))
    norm_b  = math.sqrt(sum(v * v for v in b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


class SimilarityIndex:
    """
    In-memory TF-IDF index over a list of documents.
    Replaces pgVector for the demo.
    """

    def __init__(self, docs: list[dict[str, str]]) -> None:
        """
        docs: list of {"id": ..., "text": ..., "metadata": {...}}
        """
        self._docs    = docs
        self._tokens  = [_tokenise(d["text"]) for d in docs]
        self._idf     = self._compute_idf()
        self._vectors = [_tfidf_vector(t, self._idf) for t in self._tokens]

    def _compute_idf(self) -> dict[str, float]:
        n    = len(self._tokens)
        df: dict[str, int] = {}
        for tokens in self._tokens:
            for t in set(tokens):
                df[t] = df.get(t, 0) + 1
        return {
            t: math.log((n + 1) / (c + 1)) + 1
            for t, c in df.items()
        }

    def search(self, query: str, top_k: int = 1,
               threshold: float = SIMILARITY_THRESHOLD,
               filter_theme: str | None = None
               ) -> list[dict[str, Any]]:
        """
        Return top_k most similar documents above threshold.
        Optionally filter by theme metadata.
        """
        q_tokens = _tokenise(query)
        q_vec    = _tfidf_vector(q_tokens, self._idf)

        results = []
        for i, doc in enumerate(self._docs):
            if filter_theme and doc.get("theme") != filter_theme:
                continue
            score = cosine_similarity(q_vec, self._vectors[i])
            if score >= threshold:
                results.append({
                    "score":    round(score, 4),
                    "document": doc,
                })

        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:top_k]


# ─────────────────────────────────────────────────────────────
# BUILD INDEXES
# ─────────────────────────────────────────────────────────────

def build_kb_index() -> SimilarityIndex:
    """Build the long-lived KB index from the pre-seeded entries."""
    docs = []
    for entry in SEED_KB:
        docs.append({
            "id":       str(uuid.uuid4()),
            "text":     entry["question"] + " " + entry["answer"],
            "theme":    entry["theme"],
            "question": entry["question"],
            "answer":   entry["answer"],
        })
    return SimilarityIndex(docs)


def build_attachment_index(
        priv_db: sqlite3.Connection, case_id: str
) -> SimilarityIndex | None:
    """
    Build a short-lived index from this case's anonymised attachment Markdowns.
    Returns None if the case has no clean attachments.
    """
    rows = priv_db.execute(
        "SELECT filename, markdown_anon FROM attachment_anon WHERE case_id=?",
        (case_id,),
    ).fetchall()

    if not rows:
        return None

    docs = []
    for row in rows:
        # Split Markdown into ~200-char passages for finer retrieval
        passages = _split_passages(row["markdown_anon"], max_len=200)
        for i, passage in enumerate(passages):
            docs.append({
                "id":       f"{case_id}_{i}",
                "text":     passage,
                "filename": row["filename"],
                "passage":  passage,
            })
    return SimilarityIndex(docs) if docs else None


def _split_passages(text: str, max_len: int = 200) -> list[str]:
    """Split text into overlapping passages of max_len chars."""
    sentences = re.split(r"(?<=[.!?])\s+", text)
    passages:  list[str] = []
    current  = ""
    for sent in sentences:
        if len(current) + len(sent) <= max_len:
            current = (current + " " + sent).strip()
        else:
            if current:
                passages.append(current)
            current = sent[:max_len]
    if current:
        passages.append(current)
    return passages or [text[:max_len]]


# ─────────────────────────────────────────────────────────────
# PROMPT BUILDER
#
# Constructs the enriched_prompt from question + KB context
# + attachment context. Format mirrors the production LLM prompt.
# ─────────────────────────────────────────────────────────────

_CONTEXT_WARNING = (
    "IMPORTANT: The following context is provided for reference only. "
    "Laws and regulations may have changed since this was validated. "
    "Always generate a fresh, accurate answer — do not copy the context verbatim."
)


def build_enriched_prompt(
    question:    str,
    theme:       str,
    kb_hit:      dict[str, Any] | None,
    att_hit:     dict[str, Any] | None,
    language:    str = "DE",
) -> str:
    """
    Assemble the enriched prompt that Response will use instead of
    the bare question.

    Structure:
      [CONTEXT — KB]         (if KB match found)
      [CONTEXT — DOCUMENT]   (if attachment match found)
      [WARNING]
      [QUESTION]
      [INSTRUCTION]
    """
    parts: list[str] = []

    if kb_hit:
        doc = kb_hit["document"]
        parts.append(
            f"[CONTEXT — KNOWLEDGE BASE | theme={theme} | "
            f"similarity={kb_hit['score']:.2f}]\n"
            f"Q: {doc['question']}\n"
            f"A: {doc['answer']}"
        )

    if att_hit:
        doc = att_hit["document"]
        parts.append(
            f"[CONTEXT — CITIZEN DOCUMENT | file={doc.get('filename','')} | "
            f"similarity={att_hit['score']:.2f}]\n"
            f"{doc['passage']}"
        )

    if parts:
        parts.append(f"[WARNING] {_CONTEXT_WARNING}")

    parts.append(f"[QUESTION]\n{question}")

    lang_map = {
        "DE": "Answer in German (Hochdeutsch).",
        "FR": "Répondre en français.",
        "IT": "Rispondere in italiano.",
        "RM": "Rispunder en rumantsch.",
        "EN": "Answer in English.",
    }
    parts.append(
        f"[INSTRUCTION] You are an ASTRA (Swiss Federal Roads Office) "
        f"assistant. Answer the citizen's question accurately, concisely, "
        f"and professionally. {lang_map.get(language, lang_map['DE'])} "
        f"Theme: {theme}."
    )

    return "\n\n".join(parts)


# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────

def init_enrichment_db() -> sqlite3.Connection:
    """
    Tables:
      enrichment_results  — one row per question, what was found
      enrichment_log      — append-only audit events
    """
    conn = sqlite3.connect(str(ENRICHMENT_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS enrichment_results (
            question_id      TEXT PRIMARY KEY,
            case_id          TEXT NOT NULL,
            question         TEXT NOT NULL,
            theme            TEXT NOT NULL,
            kb_hit           INTEGER NOT NULL DEFAULT 0,
            kb_score         REAL,
            att_hit          INTEGER NOT NULL DEFAULT 0,
            att_score        REAL,
            enriched_at      TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS enrichment_log (
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


def case_already_enriched(enr_conn: sqlite3.Connection,
                          case_id: str) -> bool:
    return enr_conn.execute(
        "SELECT 1 FROM enrichment_results WHERE case_id=?", (case_id,)
    ).fetchone() is not None


def log_event(enr_conn: sqlite3.Connection,
              case_id: str, event: str, detail: str = "") -> None:
    enr_conn.execute(
        "INSERT INTO enrichment_log VALUES (?,?,?,?,?)",
        (str(uuid.uuid4()), case_id, event, detail,
         datetime.now(timezone.utc).isoformat()),
    )
    enr_conn.commit()


# ─────────────────────────────────────────────────────────────
# CORE ENRICHMENT LOGIC
# ─────────────────────────────────────────────────────────────

def run_enrichment(
    case:      dict[str, Any],
    questions: list[dict[str, Any]],
    priv_db:   sqlite3.Connection,
    dec_db_rw: sqlite3.Connection,
    enr_conn:  sqlite3.Connection,
    kb_index:  SimilarityIndex,
) -> list[dict[str, Any]]:
    """
    Enrich each IN_SCOPE question for one case.
    Returns list of enrichment result dicts.
    """
    case_id  = case["case_id"]
    language = case.get("language", "DE")

    # Build short-lived attachment index for this case
    att_index = build_attachment_index(priv_db, case_id)
    if att_index:
        log_event(enr_conn, case_id, "ATTACHMENT_INDEX_BUILT",
                  f"passages={len(att_index._docs)}")

    results = []

    for q in questions:
        if q["scope"] != "IN_SCOPE":
            continue

        q_id   = q["id"]
        q_text = q["question"]
        theme  = q["theme"]

        # ── KB similarity search ──────────────────────────────
        kb_hits = kb_index.search(
            q_text,
            top_k=1,
            threshold=SIMILARITY_THRESHOLD,
            filter_theme=theme,
        )
        kb_hit = kb_hits[0] if kb_hits else None

        # Also try without theme filter if no themed match
        if not kb_hit:
            kb_hits = kb_index.search(
                q_text, top_k=1, threshold=SIMILARITY_THRESHOLD
            )
            kb_hit = kb_hits[0] if kb_hits else None

        if kb_hit:
            log_event(enr_conn, case_id, "KB_HIT",
                      f"q={q_text[:50]} score={kb_hit['score']:.3f} "
                      f"theme={theme}")
        else:
            log_event(enr_conn, case_id, "KB_MISS",
                      f"q={q_text[:50]} theme={theme}")

        # ── Attachment similarity search ──────────────────────
        att_hit = None
        if att_index:
            att_hits = att_index.search(
                q_text, top_k=1, threshold=SIMILARITY_THRESHOLD
            )
            att_hit = att_hits[0] if att_hits else None
            if att_hit:
                log_event(enr_conn, case_id, "ATTACHMENT_HIT",
                          f"q={q_text[:50]} score={att_hit['score']:.3f}")

        # ── Build enriched prompt ─────────────────────────────
        enriched = build_enriched_prompt(
            question=q_text,
            theme=theme,
            kb_hit=kb_hit,
            att_hit=att_hit,
            language=language,
        )

        # ── Write enriched_prompt back to questions table ─────
        dec_db_rw.execute(
            "UPDATE questions SET enriched_prompt=?, status='ENRICHED' WHERE id=?",
            (enriched, q_id),
        )
        dec_db_rw.commit()

        # ── Record result ─────────────────────────────────────
        result = {
            "question_id": q_id,
            "case_id":     case_id,
            "question":    q_text,
            "theme":       theme,
            "kb_hit":      1 if kb_hit  else 0,
            "kb_score":    kb_hit["score"]  if kb_hit  else None,
            "att_hit":     1 if att_hit else 0,
            "att_score":   att_hit["score"] if att_hit else None,
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        }
        enr_conn.execute("""
            INSERT INTO enrichment_results VALUES (
                :question_id, :case_id, :question, :theme,
                :kb_hit, :kb_score, :att_hit, :att_score, :enriched_at
            )
        """, result)
        enr_conn.commit()
        results.append(result)

    # ── Advance pipeline step ──────────────────────────────────
    try:
        ing = sqlite3.connect(str(INGESTION_DB_PATH),
                              check_same_thread=False)
        ing.execute(
            "UPDATE cases SET pipeline_step='ENRICHMENT_DONE' WHERE case_id=?",
            (case_id,),
        )
        ing.commit()
        ing.close()
    except Exception:
        pass

    log_event(enr_conn, case_id, "ENRICHMENT_DONE",
              f"questions={len(results)} "
              f"kb_hits={sum(r['kb_hit'] for r in results)} "
              f"att_hits={sum(r['att_hit'] for r in results)}")

    return results


# ─────────────────────────────────────────────────────────────
# POLLING LOOP
# ─────────────────────────────────────────────────────────────

def poll_and_enrich(enr_conn:  sqlite3.Connection,
                   kb_index:  SimilarityIndex,
                   stop_event: threading.Event) -> None:
    print("  [Prompt Enrichment] polling Decomposition DB every 5 seconds...")

    while not stop_event.is_set():
        ing_db  = open_db_ro(INGESTION_DB_PATH)
        priv_db = open_db_ro(PRIVACY_DB_PATH)

        if not all([ing_db, priv_db]):
            print("  [Prompt Enrichment] waiting for upstream DBs...")
            stop_event.wait(5)
            for db in [ing_db, priv_db]:
                try:
                    if db: db.close()
                except Exception:
                    pass
            continue

        # Open decomp DB read-write so we can update enriched_prompt
        dec_db_rw = sqlite3.connect(str(DECOMP_DB_PATH),
                                    check_same_thread=False)
        dec_db_rw.row_factory = sqlite3.Row

        try:
            pending = ing_db.execute(
                "SELECT * FROM cases WHERE pipeline_step='DECOMPOSITION_DONE'"
            ).fetchall()
        finally:
            ing_db.close()

        new_count = 0
        for row in pending:
            case = dict(row)
            if case_already_enriched(enr_conn, case["case_id"]):
                continue

            # Fetch questions from Decomposition DB
            qs = [dict(r) for r in dec_db_rw.execute(
                "SELECT * FROM questions WHERE case_id=? AND scope='IN_SCOPE'",
                (case["case_id"],),
            ).fetchall()]

            if not qs:
                # No in-scope questions — advance directly
                try:
                    ing2 = sqlite3.connect(str(INGESTION_DB_PATH),
                                           check_same_thread=False)
                    ing2.execute(
                        "UPDATE cases SET pipeline_step='ENRICHMENT_DONE' "
                        "WHERE case_id=?", (case["case_id"],)
                    )
                    ing2.commit(); ing2.close()
                except Exception:
                    pass
                continue

            results = run_enrichment(
                case, qs, priv_db, dec_db_rw, enr_conn, kb_index
            )
            new_count += 1
            _print_result(case, results, dec_db_rw)

        if new_count:
            print(f"\n  [Prompt Enrichment] ✓ {new_count} case(s) enriched.\n")

        try:
            priv_db.close()
            dec_db_rw.close()
        except Exception:
            pass

        stop_event.wait(5)


# ─────────────────────────────────────────────────────────────
# TERMINAL PRINTER
# ─────────────────────────────────────────────────────────────

def _print_result(case: dict, results: list[dict],
                  dec_db: sqlite3.Connection) -> None:
    kb_hits  = sum(r["kb_hit"]  for r in results)
    att_hits = sum(r["att_hit"] for r in results)

    print(f"\n{'─'*60}")
    print(f"  🔍  Prompt Enrichment complete")
    print(f"  Case ID : {case['case_id']}")
    print(f"  Source  : {case['source_type']}")
    print(f"  Questions: {len(results)}  KB hits: {kb_hits}  "
          f"Attachment hits: {att_hits}")

    for r in results:
        kb_icon  = "📚 KB hit"  if r["kb_hit"]  else "   KB miss"
        att_icon = "📎 Doc hit" if r["att_hit"] else "   Doc miss"
        score_kb  = f" (score={r['kb_score']:.3f})"  if r["kb_hit"]  else ""
        score_att = f" (score={r['att_score']:.3f})" if r["att_hit"] else ""
        print(f"\n  Q: {r['question'][:65]}")
        print(f"     {kb_icon}{score_kb}")
        print(f"     {att_icon}{score_att}")

        # Show first 200 chars of enriched_prompt
        ep_row = dec_db.execute(
            "SELECT enriched_prompt FROM questions WHERE id=?",
            (r["question_id"],),
        ).fetchone()
        if ep_row and ep_row["enriched_prompt"]:
            preview = ep_row["enriched_prompt"][:200].replace("\n", " ")
            print(f"     enriched_prompt: {preview}…")

    print(f"\n  → Next step: Phase 08 Response")
    print(json.dumps({
        "case_id":   case["case_id"],
        "tenant_id": case["tenant_id"],
        "kb_hits":   kb_hits,
        "att_hits":  att_hits,
        "step":      "RESPONSE",
    }, indent=4))


# ─────────────────────────────────────────────────────────────
# WEB DASHBOARD  (port 8006)
# ─────────────────────────────────────────────────────────────

CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#f0f2f5;min-height:100vh;padding:32px 20px}
.page{max-width:980px;margin:0 auto}
.header{background:#1a2332;color:white;border-radius:8px;
        padding:20px 28px;margin-bottom:24px;
        display:flex;align-items:center;gap:16px}
.badge{background:#b45309;color:white;font-size:10px;font-weight:700;
       padding:3px 8px;border-radius:3px;letter-spacing:.06em;text-transform:uppercase}
.header h1{font-size:18px;font-weight:600}
.header p{font-size:12px;color:#8b949e;margin-top:2px}
.hdr-right{margin-left:auto;text-align:right;font-size:11px;color:#8b949e}

.stats{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:24px}
.stat{background:white;border-radius:7px;padding:14px 18px;
      box-shadow:0 1px 3px rgba(0,0,0,.08)}
.stat-num{font-size:24px;font-weight:700;color:#1a2332}
.stat-label{font-size:10px;color:#9ca3af;margin-top:2px;
            text-transform:uppercase;letter-spacing:.05em}

.sec-title{font-size:11px;font-weight:700;color:#9ca3af;
           text-transform:uppercase;letter-spacing:.07em;
           margin-bottom:10px;padding:0 2px;margin-top:20px}

.card{background:white;border-radius:7px;
      box-shadow:0 1px 3px rgba(0,0,0,.08);overflow:hidden;margin-bottom:16px}

/* Question enrichment cards */
.q-card{padding:16px 20px;border-bottom:1px solid #f3f4f6}
.q-card:last-child{border-bottom:none}
.q-header{display:flex;align-items:flex-start;gap:10px;margin-bottom:10px}
.q-text{font-size:13px;font-style:italic;color:#1f2937;flex:1}
.q-badges{display:flex;gap:6px;flex-shrink:0;flex-wrap:wrap;align-items:center}
.badge-hit {background:#d1fae5;color:#065f46;font-size:10px;font-weight:700;
            padding:2px 7px;border-radius:3px}
.badge-miss{background:#f3f4f6;color:#9ca3af;font-size:10px;
            padding:2px 7px;border-radius:3px}
.badge-theme{background:#e0f2fe;color:#0369a1;font-size:10px;font-weight:600;
             padding:2px 7px;border-radius:3px}
.score-tag{font-family:monospace;font-size:10px;color:#6b7280;margin-left:3px}

/* Prompt viewer */
.prompt-viewer{background:#f8f9fb;border:1px solid #e5e7eb;border-radius:5px;
               padding:12px 14px;font-family:'Menlo',monospace;font-size:11px;
               color:#374151;white-space:pre-wrap;word-break:break-word;
               max-height:220px;overflow-y:auto;line-height:1.6;margin-top:8px}
.prompt-section-kb  {color:#1d4ed8;font-weight:600}
.prompt-section-doc {color:#6d28d9;font-weight:600}
.prompt-section-warn{color:#b45309;font-weight:600}
.prompt-section-q   {color:#065f46;font-weight:600}
.prompt-section-ins {color:#6b7280}

/* KB viewer */
.kb-row{padding:10px 16px;border-bottom:1px solid #f3f4f6;font-size:12px}
.kb-row:last-child{border-bottom:none}
.kb-q{color:#1f2937;font-weight:500;margin-bottom:3px}
.kb-a{color:#6b7280;line-height:1.5}
.kb-theme{font-size:10px;font-weight:700;padding:1px 6px;border-radius:3px;
          background:#e0f2fe;color:#0369a1;margin-left:6px}

.log-panel{background:#1a2332;border-radius:7px;
           padding:16px 20px;max-height:200px;overflow-y:auto}
.log-line{font-family:'Menlo',monospace;font-size:11px;
          color:#8b949e;padding:2px 0;line-height:1.6}
.ts{color:#484f58}
.ev-kb-hit  {color:#2ea043}
.ev-kb-miss {color:#d29922}
.ev-att-hit {color:#388bfd}
.ev-done    {color:#a371f7}

.refresh-note{font-size:11px;color:#9ca3af;text-align:center;margin-top:8px}
.empty{text-align:center;color:#9ca3af;padding:24px;font-size:13px}
"""


def _colour_prompt(text: str) -> str:
    """Add span colours to prompt sections for display."""
    text = _html.escape(text)
    text = re.sub(r"(\[CONTEXT — KNOWLEDGE BASE[^\]]*\])",
                  r'<span class="prompt-section-kb">\1</span>', text)
    text = re.sub(r"(\[CONTEXT — CITIZEN DOCUMENT[^\]]*\])",
                  r'<span class="prompt-section-doc">\1</span>', text)
    text = re.sub(r"(\[WARNING\][^\n]*)",
                  r'<span class="prompt-section-warn">\1</span>', text)
    text = re.sub(r"(\[QUESTION\])",
                  r'<span class="prompt-section-q">\1</span>', text)
    text = re.sub(r"(\[INSTRUCTION\][^\n]*)",
                  r'<span class="prompt-section-ins">\1</span>', text)
    return text


def render_dashboard(enr_conn: sqlite3.Connection,
                     kb_index: SimilarityIndex) -> str:
    results = enr_conn.execute(
        "SELECT * FROM enrichment_results ORDER BY enriched_at DESC"
    ).fetchall()
    results = [dict(r) for r in results]

    logs = enr_conn.execute(
        "SELECT * FROM enrichment_log ORDER BY ts DESC LIMIT 80"
    ).fetchall()
    logs = [dict(r) for r in logs]

    # Case metadata
    case_meta: dict[str, dict] = {}
    ing_db = open_db_ro(INGESTION_DB_PATH)
    if ing_db:
        try:
            for r in ing_db.execute(
                "SELECT case_id, source_type, subject, language FROM cases"
            ):
                case_meta[r["case_id"]] = dict(r)
        finally:
            ing_db.close()

    # Stats
    total    = len(results)
    kb_hits  = sum(r["kb_hit"]  for r in results)
    att_hits = sum(r["att_hit"] for r in results)
    kb_miss  = total - kb_hits
    kb_rate  = int(kb_hits / total * 100) if total else 0

    stats_html = f"""<div class="stats">
      <div class="stat"><div class="stat-num">{total}</div>
        <div class="stat-label">Questions</div></div>
      <div class="stat"><div class="stat-num" style="color:#065f46">{kb_hits}</div>
        <div class="stat-label">📚 KB Hits</div></div>
      <div class="stat"><div class="stat-num" style="color:#9ca3af">{kb_miss}</div>
        <div class="stat-label">KB Misses</div></div>
      <div class="stat"><div class="stat-num" style="color:#388bfd">{att_hits}</div>
        <div class="stat-label">📎 Doc Hits</div></div>
      <div class="stat"><div class="stat-num" style="color:#b45309">{kb_rate}%</div>
        <div class="stat-label">Cache Hit Rate</div></div>
    </div>"""

    # Question enrichment cards — grouped by case
    dec_db = sqlite3.connect(f"file:{DECOMP_DB_PATH}?mode=ro", uri=True)
    dec_db.row_factory = sqlite3.Row

    by_case: dict[str, list[dict]] = {}
    for r in results:
        by_case.setdefault(r["case_id"], []).append(r)

    src_labels = {"DIRECT_EMAIL": "Email", "STAFF_FORWARD": "Staff",
                  "POSTAL_SCAN": "Scan",  "WEB_FORM": "Web Form"}

    q_cards_html = ""
    if not by_case:
        q_cards_html = '<div class="empty">Waiting for Decomposition to complete...</div>'

    for cid, case_results in by_case.items():
        meta = case_meta.get(cid, {})
        src  = src_labels.get(meta.get("source_type", ""), "")
        q_cards_html += f"""
        <div style="padding:10px 16px;background:#f8f9fb;
                    border-bottom:2px solid #e5e7eb;font-size:11px;
                    color:#6b7280;font-weight:700">
          {src} — {cid[:8]}…
          <span style="font-weight:400;margin-left:8px">
            {meta.get('subject','')[:50]}
          </span>
        </div>"""

        for r in case_results:
            # Fetch enriched_prompt from decomp DB
            ep_row = dec_db.execute(
                "SELECT enriched_prompt FROM questions WHERE id=?",
                (r["question_id"],),
            ).fetchone()
            enriched = (ep_row["enriched_prompt"] or "") if ep_row else ""

            kb_badge  = (
                f'<span class="badge-hit">📚 KB hit'
                f'<span class="score-tag">{r["kb_score"]:.3f}</span></span>'
                if r["kb_hit"] else
                '<span class="badge-miss">KB miss</span>'
            )
            att_badge = (
                f'<span class="badge-hit">📎 Doc hit'
                f'<span class="score-tag">{r["att_score"]:.3f}</span></span>'
                if r["att_hit"] else
                '<span class="badge-miss">Doc miss</span>'
            )
            theme_badge = f'<span class="badge-theme">{r["theme"]}</span>'

            prompt_html = ""
            if enriched:
                prompt_html = (
                    f'<div class="prompt-viewer">{_colour_prompt(enriched)}</div>'
                )

            q_cards_html += f"""
            <div class="q-card">
              <div class="q-header">
                <div class="q-text">
                  {_html.escape(r['question'][:100])}
                </div>
                <div class="q-badges">
                  {theme_badge}{kb_badge}{att_badge}
                </div>
              </div>
              {prompt_html}
            </div>"""

    try:
        dec_db.close()
    except Exception:
        pass

    # KB entries preview
    kb_rows_html = ""
    for entry in SEED_KB[:6]:
        kb_rows_html += f"""
        <div class="kb-row">
          <div class="kb-q">
            {_html.escape(entry['question'][:80])}
            <span class="kb-theme">{entry['theme']}</span>
          </div>
          <div class="kb-a">{_html.escape(entry['answer'][:120])}…</div>
        </div>"""

    # Log
    ev_css = {
        "KB_HIT":              "ev-kb-hit",
        "KB_MISS":             "ev-kb-miss",
        "ATTACHMENT_HIT":      "ev-att-hit",
        "ATTACHMENT_INDEX_BUILT": "ev-att-hit",
        "ENRICHMENT_DONE":     "ev-done",
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
<title>Phase 07 — Prompt Enrichment</title>
<style>{CSS}</style>
</head>
<body>
<div class="page">
  <div class="header">
    <div>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
        <span class="badge">Phase 07</span>
        <h1>Prompt Enrichment</h1>
      </div>
      <p>KB similarity search · Citizen document context · Enriched prompt assembly</p>
    </div>
    <div class="hdr-right">
      Polling Decomposition every 5s<br>
      <span style="color:#2ea043;font-weight:600">● Live</span>
    </div>
  </div>

  {stats_html}

  <div class="sec-title">Enriched prompts per question</div>
  <div class="card">{q_cards_html}</div>

  <div class="sec-title">Knowledge Base ({len(SEED_KB)} entries seeded)</div>
  <div class="card">{kb_rows_html}</div>

  <div class="sec-title">Audit Log</div>
  <div class="log-panel">{log_lines}</div>
  <p class="refresh-note">Auto-refreshes every 5 seconds</p>
</div>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────
# HTTP SERVER
# ─────────────────────────────────────────────────────────────

class EnrichmentDashboardHandler(http.server.BaseHTTPRequestHandler):
    enr_conn: sqlite3.Connection = None  # type: ignore
    kb_index: SimilarityIndex    = None  # type: ignore

    def log_message(self, *args: object) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/favicon.ico":
            self.send_response(204); self.end_headers(); return
        page = render_dashboard(self.enr_conn, self.kb_index)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode())


def make_handler(conn: sqlite3.Connection, index: SimilarityIndex):
    class H(EnrichmentDashboardHandler):
        enr_conn = conn
        kb_index = index
    return H


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "═"*60)
    print("  CITIZEN PLATFORM — Phase 07: Prompt Enrichment  (DEMO)")
    print("═"*60)

    enr_conn = init_enrichment_db()
    print(f"\n  ✓  Enrichment DB  : {ENRICHMENT_DB_PATH}")

    # Build KB index at startup
    kb_index = build_kb_index()
    print(f"  ✓  KB index built : {len(SEED_KB)} entries, "
          f"{len(kb_index._docs)} documents")
    print(f"  ✓  Reading from   : Decomposition DB + Privacy DB")

    stop_event = threading.Event()
    poller = threading.Thread(
        target=poll_and_enrich,
        args=(enr_conn, kb_index, stop_event),
        daemon=True,
    )
    poller.start()

    handler    = make_handler(enr_conn, kb_index)
    server     = http.server.HTTPServer(("", PORT), handler)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    print(f"\n  ✓  Dashboard: http://localhost:{PORT}")
    print(f"\n  Run order:")
    print(f"    Terminal 1 → python3 demo/reception.py          (port 8000)")
    print(f"    Terminal 2 → python3 demo/ingestion.py          (port 8001)")
    print(f"    Terminal 3 → python3 demo/security.py           (port 8002)")
    print(f"    Terminal 4 → python3 demo/privacy.py            (port 8003)")
    print(f"    Terminal 5 → python3 demo/analysis.py           (port 8004)")
    print(f"    Terminal 6 → python3 demo/decomposition.py      (port 8005)")
    print(f"    Terminal 7 → python3 demo/prompt_enrichment.py  (port 8006)")
    print(f"\n  Press Ctrl-C to stop.\n")
    print("─"*60)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n\n  Stopping prompt enrichment...")
        stop_event.set()
        server.shutdown()
        print("  Done.\n")


if __name__ == "__main__":
    main()
