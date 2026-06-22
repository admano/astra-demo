"""
privacy/models.py
-----------------
Request / Response models for the ASTRA Privacy pipeline.

Demo version: pure stdlib dataclasses — no pydantic required.
The standalone FastAPI service (privacy/main.py) adds its own
pydantic wrappers on top of these when fastapi is available.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class PrivacyRunRequest:
    """Input to run_privacy_pipeline()."""
    text: str
    surface: Optional[str] = "body"   # "subject" | "body" | "attachment"


@dataclass
class DetectedEntity:
    entity_type: str
    value: str
    start: int
    end: int
    confidence: float
    detected_by: str


@dataclass
class AnonymizationAction:
    entity_type: str
    original_value: str
    pseudonym: str
    action: str
    token_ref: Optional[str] = None


@dataclass
class PrivacyRunResponse:
    case_id: str
    session_id: str
    surface: str
    original_text: str
    anonymized_text: str
    detected_entities: List[DetectedEntity] = field(default_factory=list)
    anonymization_actions: List[AnonymizationAction] = field(default_factory=list)
    confidence_scores: Dict[str, float] = field(default_factory=dict)
    risk_score: float = 0.0
    pipeline_status: str = "safe"
    audit_metadata: dict = field(default_factory=dict)
