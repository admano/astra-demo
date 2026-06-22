"""
Stage 6 — Risk Check (Residual Risk Gate)
==========================================
Scores the risk of the OUTPUT text — i.e. what actually goes to the AI —
NOT the sensitivity of what was detected in the input.

Design rationale
----------------
The previous scorer averaged ENTITY_RISK_WEIGHTS over detected entities.
That measured *input sensitivity*, which caused well-anonymized text like
  "Person_A1, SSN-ID-A1, CARD-A1, CH-IBAN-A1, user_a1@redacted.astra"
to be blocked, even though it contains zero real PII.

Correct model: residual risk = what leaked THROUGH pseudonymization.

Two failure modes that raise residual risk:
  1. MISSED entities  — original value still present verbatim in output text
  2. FAILED pseudonyms — a pseudonym was not applied (action missing)

If all entities were successfully pseudonymized and none appear verbatim
in the output, residual risk is 0.0 → gate passes as "safe".

The sensitivity weights are still used to score severity of any leak:
a leaked SSN is far worse than a leaked location name.
"""

import re
from typing import List
from detector import RawEntity

# Sensitivity of each entity type — used ONLY to weight actual leaks.
ENTITY_RISK_WEIGHTS = {
    "SSN":             1.0, "CH_AHV":          1.0,
    "CREDIT_CARD":     1.0, "IBAN":            0.95,
    "CH_IBAN":         0.95, "PASSPORT":       0.90,
    "CH_PASSPORT":     0.90, "MEDICAL_LICENSE":0.85,
    "EMAIL":           0.65, "PHONE":          0.55,
    "CH_PHONE":        0.55, "IP_ADDRESS":     0.45,
    "CH_UID":          0.40, "URL":            0.30,
    "FULL_NAME":       0.50, "DATE_TIME":      0.30,
    "LOCATION":        0.30, "NATIONALITY":    0.25,
}

# Gate thresholds — applied to RESIDUAL risk only.
# A score of 0.0 means no PII leaked → always safe.
RISK_THRESHOLD_SAFE      = 0.10   # small non-zero headroom for low-confidence leaks
RISK_THRESHOLD_ESCALATE  = 0.40

def compute_risk_score(
    detected_entities: List[RawEntity],
    actions,                   # List[AnonymizationResult]
    anonymized_text: str,
) -> float:
    """
    Simple demo version with pseudonyms + weights.

    For each detected entity:
    - original still visible  -> full risk, weighted by entity type
    - pseudonym visible       -> safe, no risk
    - neither visible         -> medium risk, half weight
    """

    if not detected_entities:
        return 0.0

    total_risk = 0.0
    max_risk = 0.0

    for entity, action in zip(detected_entities, actions):
        original = (entity.value or "").strip()
        pseudonym = (action.pseudonym or "").strip()

        weight = ENTITY_RISK_WEIGHTS.get(entity.entity_type, 0.5)
        max_risk += weight

        # Case 1: original value still visible -> leak
        if original and original in anonymized_text:
            total_risk += weight

        # Case 2: pseudonym correctly visible -> safe
        elif pseudonym and pseudonym in anonymized_text:
            pass

        # Case 3: original missing and pseudonym missing -> unclear
        else:
            total_risk += weight * 0.5

    score = total_risk / max_risk

    return round(score, 2)


def compute_risk_score_complex(
    detected_entities: List[RawEntity],
    actions,                   # List[AnonymizationResult]
    anonymized_text: str,
) -> float:
    """
    Residual risk = weighted score of PII that leaked into the output.

    For each detected entity we check two conditions:
      A) Does the original value appear verbatim in anonymized_text?
         → definite leak, full weight
      B) Was the action missing or did the pseudonym not end up in the text?
         → likely partial leak, half weight

    If neither condition is true the entity contributed zero residual risk.
    Result is normalised to [0, 1].
    """
    if not detected_entities:
        return 0.0

    # Build a set of pseudonyms that were actually applied
    applied_pseudonyms = {
        a.pseudonym
        for a in (actions or [])
        if hasattr(a, "pseudonym") and a.pseudonym
    }

    # Also accept dict-style actions (from pipeline serialisation)
    if not applied_pseudonyms and actions:
        applied_pseudonyms = {
            a.get("pseudonym", "")
            for a in actions
            if isinstance(a, dict)
        }

    leak_score = 0.0
    max_possible = sum(
        ENTITY_RISK_WEIGHTS.get(e.entity_type, 0.5)
        for e in detected_entities
    )

    for entity in detected_entities:
        weight = ENTITY_RISK_WEIGHTS.get(entity.entity_type, 0.5)

        # Condition A: raw value still in output → full leak
        if entity.value and entity.value in anonymized_text:
            leak_score += weight
            continue

        # Condition B: no pseudonym found in output for this entity
        # (means it was either missed or the replacement failed)
        entity_pseudonymized = any(
            entity.value in (
                getattr(a, "original_value", None) or
                (a.get("original_value") if isinstance(a, dict) else None) or ""
            )
            and (
                (getattr(a, "pseudonym", None) or
                 (a.get("pseudonym") if isinstance(a, dict) else None) or "")
                in anonymized_text
            )
            for a in (actions or [])
        )
        if not entity_pseudonymized:
            leak_score += weight * 0.5   # uncertain — penalise at half weight

    normalised = leak_score / max(max_possible, 1.0)
    return round(min(normalised, 1.0), 3)


def gate_decision(risk_score: float) -> str:
    """
    Residual Risk Gate — the 'Safe?' diamond in the diagram.

    safe       → risk_score ≤ 0.10  (all PII pseudonymized, output clean)
    escalated  → 0.10 < score ≤ 0.40  (minor leakage — manual review)
    blocked    → score > 0.40  (significant leakage — must not reach AI)
    """
    if risk_score <= RISK_THRESHOLD_SAFE:
        return "safe"
    if risk_score <= RISK_THRESHOLD_ESCALATE:
        return "escalated"
    return "blocked"


def build_confidence_scores(detected_entities: List[RawEntity]) -> dict:
    """Average detection confidence per entity type."""
    totals: dict = {}
    counts: dict = {}
    for e in detected_entities:
        totals[e.entity_type] = totals.get(e.entity_type, 0.0) + e.confidence
        counts[e.entity_type] = counts.get(e.entity_type, 0) + 1
    return {t: round(totals[t] / counts[t], 3) for t in totals}
