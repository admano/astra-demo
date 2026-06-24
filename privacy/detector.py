"""
privacy/detector.py
-------------------
PII Detector — Presidio + Swiss recognisers + optional HuggingFace NER

Resilient startup: all hard dependencies (presidio, spacy, transformers)
are wrapped in try/except so the package never crashes on import even
when optional deps are missing. Falls back to a no-op detector gracefully.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import threading
from dataclasses import dataclass
from typing import List, Optional

# ── Optional deps ──────────────────────────────────────────────────────────
# Only check *presence* here (cheap, no code runs) — spaCy/presidio/transformers
# are actually imported lazily, inside the background thread started below,
# so a slow import (torch alone can take several seconds) never blocks the
# phase's HTTP server from starting and serving the dashboard.

_SPACY_OK       = importlib.util.find_spec("spacy") is not None
_PRESIDIO_OK    = importlib.util.find_spec("presidio_analyzer") is not None
_TRANSFORMERS_OK = importlib.util.find_spec("transformers") is not None

# NOTE: deliberately *not* imported at module level — swiss_recognizers.py
# itself imports presidio_analyzer eagerly, which would defeat the deferred
# loading above. Imported lazily inside build_analyzer() instead.
# from privacy.swiss_recognizers import ALL_SWISS_RECOGNIZERS

SPACY_MODEL = os.environ.get("SPACY_MODEL", "en_core_web_sm")


# ── RawEntity ─────────────────────────────────────────────────────────────

@dataclass
class RawEntity:
    entity_type: str
    # NOTE: field named `value` (not `text`) — matches usage in anonymizer.py
    value: str
    start: int
    end: int
    confidence: float
    source: str


# ── NLP engine builders ───────────────────────────────────────────────────

def _is_cached(model_name: str) -> bool:
    """True if a HF model is already on disk — checked with zero network calls."""
    try:
        from huggingface_hub import snapshot_download
        snapshot_download(model_name, local_files_only=True)
        return True
    except Exception:
        return False


def build_nlp_engine_or_none():
    """Try to build a HuggingFace TransformersNlpEngine; return None on any failure
    or if the model isn't cached yet (avoids blocking phase startup on a download)."""
    if not _PRESIDIO_OK or not _is_cached("dslim/bert-base-NER"):
        return None
    try:
        from presidio_analyzer.nlp_engine import TransformersNlpEngine
        nlp = TransformersNlpEngine({
            "nlp_engine_name": "transformers",
            "models": [{"lang_code": "en", "model_name": "dslim/bert-base-NER"}],
        })
        nlp.load()
        return nlp
    except Exception:
        return None


def build_analyzer():
    """
    Build Presidio AnalyzerEngine with Swiss custom recognizers.
    Returns None if presidio is not installed.
    """
    if not _PRESIDIO_OK:
        return None
    try:
        from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
        from privacy.swiss_recognizers import ALL_SWISS_RECOGNIZERS
        nlp_engine = build_nlp_engine_or_none()
        registry = RecognizerRegistry()
        if nlp_engine:
            registry.load_predefined_recognizers(nlp_engine=nlp_engine)
        else:
            registry.load_predefined_recognizers()
        for recognizer in ALL_SWISS_RECOGNIZERS:
            registry.add_recognizer(recognizer)
        if nlp_engine:
            return AnalyzerEngine(registry=registry, nlp_engine=nlp_engine,
                                  supported_languages=["en"])
        return AnalyzerEngine(registry=registry, supported_languages=["en"])
    except Exception as exc:
        print(f"  [Privacy/detector] WARNING: analyzer init failed: {exc}", file=sys.stderr)
        return None


def load_huggingface_ner():
    """Optional HuggingFace NER. Returns None if unavailable or not cached yet
    (avoids blocking phase startup on a slow download)."""
    if not _TRANSFORMERS_OK or not _is_cached("Davlan/bert-base-multilingual-cased-ner-hrl"):
        return None
    try:
        from transformers import pipeline as _hf_pipeline
        return _hf_pipeline(
            task="token-classification",
            model="Davlan/bert-base-multilingual-cased-ner-hrl",
            aggregation_strategy="simple",
        )
    except Exception:
        return None


# ── Entity type mapping ───────────────────────────────────────────────────

def map_presidio_entity(entity_type: str) -> str:
    return {
        "PERSON":        "FULL_NAME",
        "EMAIL_ADDRESS": "EMAIL",
        "PHONE_NUMBER":  "PHONE",
        "CREDIT_CARD":   "CREDIT_CARD",
        "IBAN_CODE":     "IBAN",
        "LOCATION":      "LOCATION",
        "DATE_TIME":     "DATE_TIME",
        "IP_ADDRESS":    "IP_ADDRESS",
        "URL":           "URL",
        "CH_IBAN":       "CH_IBAN",
        "CH_AHV":        "CH_AHV",
        "CH_UID":        "CH_UID",
    }.get(entity_type, entity_type)


def map_hf_entity(entity_group: str) -> Optional[str]:
    return {
        "PER":      "FULL_NAME",
        "PERSON":   "FULL_NAME",
        "LOC":      "LOCATION",
        "LOCATION": "LOCATION",
        "ORG":      "ORGANIZATION",
    }.get(entity_group)


# ── Overlap / merge ───────────────────────────────────────────────────────

def overlaps(a: RawEntity, b: RawEntity) -> bool:
    return not (a.end <= b.start or a.start >= b.end)


def merge_entities(entities: List[RawEntity]) -> List[RawEntity]:
    entities = sorted(entities, key=lambda e: (e.start, -e.confidence))
    cleaned: List[RawEntity] = []
    for entity in entities:
        existing = next((x for x in cleaned if overlaps(entity, x)), None)
        if existing is None:
            cleaned.append(entity)
        elif entity.confidence > existing.confidence:
            cleaned.remove(existing)
            cleaned.append(entity)
    return sorted(cleaned, key=lambda e: e.start)


# ── Module-level singletons ────────────────────────────────────────────────
# Built in a background thread so importing this module — and therefore
# starting the Privacy phase's HTTP server — never blocks on loading (or, if
# not cached, downloading) a transformer model. detect_pii() already treats
# both as optional, so requests served before loading finishes just get
# Swiss/Presidio-only results and quietly get richer once it's ready.

_ANALYZER = None
_HF_NER = None


def _load_analyzer() -> None:
    global _ANALYZER
    try:
        _ANALYZER = build_analyzer()
    except Exception as _e:
        print(f"  [Privacy/detector] WARNING: analyzer init failed: {_e}", file=sys.stderr)


def _load_hf_ner() -> None:
    global _HF_NER
    try:
        _HF_NER = load_huggingface_ner()
    except Exception:
        pass


# Two separate threads, not one running both sequentially: loading Presidio
# (spaCy-backed) and the standalone transformers pipeline back-to-back in a
# single thread has been observed to deadlock on macOS (torch's MPS backend
# appears to need its first initialization on a thread of its own).
threading.Thread(target=_load_analyzer, daemon=True).start()
threading.Thread(target=_load_hf_ner, daemon=True).start()


# ── Public API ────────────────────────────────────────────────────────────

def detect_pii(text: str) -> List[RawEntity]:
    """
    Detect PII using Presidio + Swiss recognizers + optional HuggingFace NER.
    Returns an empty list (never raises) if no engine is available.
    """
    if not text or not text.strip():
        return []

    entities: List[RawEntity] = []

    if _ANALYZER is not None:
        try:
            results = _ANALYZER.analyze(
                text=text,
                language="en",
                entities=[
                    "PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER",
                    "CREDIT_CARD", "IBAN_CODE", "LOCATION",
                    "DATE_TIME", "IP_ADDRESS", "URL",
                    "CH_IBAN", "CH_AHV", "CH_UID",
                ],
            )
            for r in results:
                entities.append(RawEntity(
                    entity_type=map_presidio_entity(r.entity_type),
                    value=text[r.start:r.end],
                    start=r.start,
                    end=r.end,
                    confidence=round(r.score, 3),
                    source="presidio",
                ))
        except Exception as exc:
            print(f"  [Privacy/detector] presidio error: {exc}", file=sys.stderr)

    if _HF_NER is not None:
        try:
            for item in _HF_NER(text):
                entity_type = map_hf_entity(item.get("entity_group", ""))
                if entity_type is None:
                    continue
                confidence = float(item.get("score", 0.0))
                if confidence < 0.70:
                    continue
                entities.append(RawEntity(
                    entity_type=entity_type,
                    value=text[int(item["start"]):int(item["end"])],
                    start=int(item["start"]),
                    end=int(item["end"]),
                    confidence=round(confidence, 3),
                    source="huggingface_ner",
                ))
        except Exception as exc:
            print(f"  [Privacy/detector] HF NER error: {exc}", file=sys.stderr)

    return merge_entities(entities)
