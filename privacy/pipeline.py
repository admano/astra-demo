"""
Pipeline Orchestrator — ASTRA Privacy / Anonymization
======================================================

Flow (matches diagram):

  User Input
      │
  Privacy Layer
      ├── 1. Identify Input           (surface: subject / body / attachment)
      ├── 2. Clean and Normalize Text
      ├── 3. Apply Privacy Rules      (GDPR + nDSG)
      ├── 4. Detect PII               (Presidio + Swiss + NER)
      ├── 5. Pseudonymize + Store     (PseudonymVault — session scoped)
      ├── 6. Risk Check               (Safe? gate)
      └── 7. Anonymized Output        → AI Processing (no real PII)

  Later (separate step):
      └── Deanonymize                 (vault.deanonymize_text(session_id, ai_response))
                                       → Restored response sent back to user

Key design:
  - Every run creates a unique session_id
  - Pseudonyms are context-preserving (AI reads "Person_A1", not "TOKEN_X")
  - Same original value → same pseudonym within a session (consistent refs)
  - Deanonymization needs only session_id + the AI output text
"""

from __future__ import annotations

import datetime
import re
from privacy.models import PrivacyRunRequest
from privacy.detector import detect_pii
from privacy.anonymizer import pseudonymize
from privacy.vault import vault
from privacy.scorer import compute_risk_score, build_confidence_scores


# ── Step 1: Identify Input ────────────────────────────────────────────────

VALID_SURFACES = {"subject", "body", "attachment"}


def identify_input(request: PrivacyRunRequest) -> tuple[str, str]:
    surface = (request.surface or "body").lower()
    if surface not in VALID_SURFACES:
        surface = "body"
    return request.text, surface


# ── Step 2: Clean and Normalize Text ─────────────────────────────────────

def clean_and_normalize(text: str) -> str:
    text = re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f\u200b-\u200f\ufeff]", "", text)
    text = re.sub(r"[^\S\n]+", " ", text)
    return text.strip()


# ── Step 3: Apply Privacy Rules ───────────────────────────────────────────

PRIVACY_RULES = {
    "what_to_hide": [
        "FULL_NAME", "EMAIL", "PHONE", "CH_PHONE",
        "SSN", "CH_AHV", "CREDIT_CARD", "IBAN", "CH_IBAN",
        "IP_ADDRESS", "PASSPORT", "CH_PASSPORT",
        "DATE_TIME", "LOCATION", "URL", "CH_UID",
        "MEDICAL_LICENSE", "NATIONALITY",
    ],
    "how_to_hide": "context-preserving pseudonymization",
    "when_to_escalate": {
        "risk_threshold_safe":     0.30,
        "risk_threshold_escalate": 0.65,
    },
    "policy": "GDPR + nDSG (Swiss Federal Act on Data Protection)",
}


def apply_privacy_rules(surface: str) -> dict:
    rules = dict(PRIVACY_RULES)
    if surface == "attachment":
        rules = dict(rules)
        rules["when_to_escalate"] = {
            "risk_threshold_safe":     0.20,
            "risk_threshold_escalate": 0.50,
        }
    return rules


# ── Step 6: Risk Check ────────────────────────────────────────────────────

def risk_check(risk_score: float, rules: dict) -> tuple[str, str | None]:
    t = rules["when_to_escalate"]
    if risk_score <= t["risk_threshold_safe"]:
        return "safe", None
    if risk_score <= t["risk_threshold_escalate"]:
        note = (
            f"Risk score {risk_score:.3f} exceeds safe threshold ({t['risk_threshold_safe']}). "
            "Flagged for Manual Review. Output conditionally released pending approval."
        )
        return "escalated", note
    note = (
        f"Risk score {risk_score:.3f} exceeds escalation threshold ({t['risk_threshold_escalate']}). "
        "Output BLOCKED. Case escalated to Data Protection Officer."
    )
    return "blocked", note


# ── Full Anonymization Pipeline ───────────────────────────────────────────

def run_privacy_pipeline(
    case_id: str,
    request: PrivacyRunRequest,
    session_id: str | None = None,
) -> dict:
    """
    Anonymize user input for AI consumption.

    If session_id is provided the existing vault session is reused, so pseudonyms
    stay consistent across multiple surfaces of the same case (subject, body,
    attachments).  When omitted a fresh session is created.

    Returns the anonymized text + the session_id for later deanonymization.
    """
    started_at = datetime.datetime.utcnow().isoformat() + "Z"

    # 1. Identify Input
    raw_text, surface = identify_input(request)

    # 2. Clean and Normalize
    normalized_text = clean_and_normalize(raw_text)

    # 3. Apply Privacy Rules
    rules = apply_privacy_rules(surface)

    # 4. Detect PII (Presidio + Swiss + NER)
    raw_entities = detect_pii(normalized_text)

    # 5. Pseudonymize — reuse caller's session or create a new one
    if session_id is None:
        session_id = vault.create_session()
    anonymized_text, actions = pseudonymize(raw_entities, normalized_text, vault, session_id)

    # 6. Risk Check
    risk_score = compute_risk_score(raw_entities, actions, anonymized_text)
    pipeline_status, manual_review_note = risk_check(risk_score, rules)

    final_output = anonymized_text
    if pipeline_status == "blocked":
        final_output = "[OUTPUT BLOCKED — PENDING DATA PROTECTION REVIEW]"

    finished_at = datetime.datetime.utcnow().isoformat() + "Z"

    return {
        # ── Core output (what goes to the AI) ─────────────────────────
        "case_id":          case_id,
        "session_id":       session_id,   # ← SAVE THIS for deanonymization
        "surface":          surface,
        "anonymized_text":  final_output, # ← SEND THIS to AI
        # ── Original (for demo display only) ──────────────────────────
        "original_text":    raw_text,
        # ── Detection results ──────────────────────────────────────────
        "detected_entities": [
            {
                "entity_type": e.entity_type,
                "value":       e.value,
                "start":       e.start,
                "end":         e.end,
                "confidence":  e.confidence,
                "detected_by": e.source,
            }
            for e in raw_entities
        ],
        # ── Pseudonymization map (what each value became) ──────────────
        "anonymization_actions": [
            {
                "entity_type":    a.entity_type,
                "original_value": a.original_value,
                "pseudonym":      a.pseudonym,
                "action":         a.action,
                "token_ref":      a.token_ref,   # same as pseudonym
            }
            for a in actions
        ],
        # ── Scoring ────────────────────────────────────────────────────
        "confidence_scores":  build_confidence_scores(raw_entities),
        "risk_score":         risk_score,
        "pipeline_status":    pipeline_status,
        # ── Audit ──────────────────────────────────────────────────────
        "audit_metadata": {
            "started_at":             started_at,
            "finished_at":            finished_at,
            "privacy_rules_applied":  rules["policy"],
            "entity_count":           len(raw_entities),
            "manual_review_note":     manual_review_note,
            "engines_active":         _engines_active(),
            # DEMO only — remove in production
            "vault_session_demo":     vault.get_session_mapping(session_id),
        },
    }


def _engines_active() -> list:
    """Report which detection engines are available at runtime."""
    engines = ["regex_fallback"]
    try:
        import presidio_analyzer  # noqa
        engines = ["presidio", "swiss_detection"]
        # Check for HuggingFace TransformersNlpEngine
        try:
            import transformers  # noqa
            import torch         # noqa
            engines.append("hf_ner(Davlan/bert-base-multilingual-cased-ner-hrl)")
        except ImportError:
            pass
    except ImportError:
        pass
    return engines
