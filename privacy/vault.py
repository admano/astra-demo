from __future__ import annotations

"""
Secure Pseudonym Vault
======================
The vault is the heart of the AI-context pipeline.

Design goal
-----------
The anonymized text sent to the AI must be:
  1. Free of real PII                     → AI never sees real data
  2. Semantically meaningful              → AI understands context fully
  3. Fully reversible per session         → deanonymization restores original

How it works
------------
Instead of opaque tokens like [SSN:TOKEN_A1B2], we generate
*context-preserving pseudonyms* — fake but realistic values:

  Real value          →  Pseudonym in AI text      →  Entity type hint
  ──────────────────────────────────────────────────────────────────────
  Anna Müller         →  "Person_A1"                →  FULL_NAME
  anna@example.ch     →  "user_a1@redacted.astra"   →  EMAIL
  +41 79 123 45 67    →  "+41-PHONE-A1"             →  CH_PHONE
  756.1234.5678.90    →  "AHV-ID-A1"                →  CH_AHV
  CH5604835012345678  →  "CH-IBAN-A1"               →  CH_IBAN
  123-45-6789         →  "SSN-ID-A1"                →  SSN
  4111 1111 1111 1111 →  "CARD-A1"                  →  CREDIT_CARD
  192.168.1.1         →  "IP-ADDR-A1"               →  IP_ADDRESS

The pseudonym:
  - Tells the AI *what kind* of entity it is (semantic type hint)
  - Uses a short, consistent label (no noise for the LLM)
  - Is unique per original value within the session
  - Maps 1:1 back to the original via the vault

Session isolation
-----------------
Each pipeline run gets a `session_id`. The vault stores:
  session_id → { pseudonym → original_value }

This means deanonymization requires both the session_id and the text.
Different cases never share a vault namespace.
"""

import uuid
import threading
from typing import Dict, Optional


# ── Pseudonym templates per entity type ──────────────────────────────────
# Format: prefix + short_id (e.g. "Person_A1", "user_b3@redacted.astra")
# Rules: readable, type-obvious, no real data, usable in a sentence.

PSEUDONYM_TEMPLATES = {
    # Names — reads naturally in a sentence
    "FULL_NAME":        lambda sid: f"Person_{sid}",
    # Emails — syntactically valid so AI understands it's an address
    "EMAIL":            lambda sid: f"user_{sid.lower()}@redacted.astra",
    # Phones — format hint preserved
    "PHONE":            lambda sid: f"+00-PHONE-{sid}",
    "CH_PHONE":         lambda sid: f"+41-PHONE-{sid}",
    # Financial — clearly labelled
    "SSN":              lambda sid: f"SSN-ID-{sid}",
    "CH_AHV":           lambda sid: f"AHV-ID-{sid}",
    "CREDIT_CARD":      lambda sid: f"CARD-{sid}",
    "IBAN":             lambda sid: f"IBAN-{sid}",
    "CH_IBAN":          lambda sid: f"CH-IBAN-{sid}",
    # Identity documents
    "PASSPORT":         lambda sid: f"PASSPORT-{sid}",
    "CH_PASSPORT":      lambda sid: f"CHPASS-{sid}",
    "MEDICAL_LICENSE":  lambda sid: f"MED-LIC-{sid}",
    # Network
    "IP_ADDRESS":       lambda sid: f"IP-ADDR-{sid}",
    "URL":              lambda sid: f"https://url-{sid}.redacted",
    # Company
    "CH_UID":           lambda sid: f"CHE-UID-{sid}",
    # Contextual — kept vague but meaningful
    "LOCATION":         lambda sid: f"Location_{sid}",
    "DATE_TIME":        lambda sid: f"DateTime_{sid}",
    "NATIONALITY":      lambda sid: f"Nationality_{sid}",
}

DEFAULT_TEMPLATE = lambda sid: f"VALUE_{sid}"

# ── Short ID generator ────────────────────────────────────────────────────
# Produces A1, A2 … A9, B1 … Z9 — short, easy to read, 26×9=234 per type
_LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ"   # skip I,O (confusing)
_DIGITS  = "123456789"


def _short_id(n: int) -> str:
    """Convert counter n (0-based) to a short 2-char ID like A1, B3."""
    letter = _LETTERS[n // len(_DIGITS) % len(_LETTERS)]
    digit  = _DIGITS[n % len(_DIGITS)]
    return f"{letter}{digit}"


# ── Vault ─────────────────────────────────────────────────────────────────

class PseudonymVault:
    """
    Thread-safe in-memory vault.

    Structure:
        _sessions: {
            session_id: {
                "pseudonym_to_original": { pseudonym: original_value },
                "original_to_pseudonym": { (entity_type, original): pseudonym },
                "counters":              { entity_type: int }
            }
        }

    Key design decisions:
        - Same original value + same entity type → same pseudonym within a session
          (consistent references: "Person_A1" always means the same person)
        - Different entity types can share counters independently
        - Session TTL not implemented (demo) — production would use Redis + TTL
    """

    def __init__(self):
        self._sessions: Dict[str, dict] = {}
        self._lock = threading.Lock()

    # ── Session lifecycle ─────────────────────────────────────────────────

    def create_session(self) -> str:
        session_id = uuid.uuid4().hex[:12].upper()
        with self._lock:
            self._sessions[session_id] = {
                "pseudonym_to_original": {},
                "original_to_pseudonym": {},
                "counters": {},
            }
        return session_id

    def session_exists(self, session_id: str) -> bool:
        return session_id in self._sessions

    # ── Pseudonymize ──────────────────────────────────────────────────────

    def pseudonymize(self, session_id: str, entity_type: str, original_value: str) -> str:
        """
        Return a context-preserving pseudonym for original_value.
        Identical (entity_type, original_value) pairs return the same pseudonym
        within the same session — so references are consistent across the text.
        """
        with self._lock:
            session = self._sessions[session_id]
            key = (entity_type, original_value)

            # Already pseudonymized in this session → return same pseudonym
            if key in session["original_to_pseudonym"]:
                return session["original_to_pseudonym"][key]

            # New value — allocate next short ID for this entity type
            n = session["counters"].get(entity_type, 0)
            session["counters"][entity_type] = n + 1
            short_id = _short_id(n)

            # Build pseudonym from template
            template = PSEUDONYM_TEMPLATES.get(entity_type, DEFAULT_TEMPLATE)
            pseudonym = template(short_id)

            # Store both directions
            session["pseudonym_to_original"][pseudonym] = original_value
            session["original_to_pseudonym"][key] = pseudonym

            return pseudonym

    # ── Deanonymize ───────────────────────────────────────────────────────

    def deanonymize_text(self, session_id: str, text: str) -> tuple[str, list]:
        """
        Replace all pseudonyms in text with their original values.
        Returns (restored_text, list_of_substitutions).

        Substitutions are sorted longest-first to avoid partial matches
        (e.g. "Person_A1" before "Person_A" if both existed).
        """
        if not self.session_exists(session_id):
            raise KeyError(f"Session '{session_id}' not found in vault.")

        with self._lock:
            mapping = self._sessions[session_id]["pseudonym_to_original"]

        if not mapping:
            return text, []

        substitutions = []
        restored = text

        # Sort by length descending to avoid partial-match collisions
        for pseudonym in sorted(mapping.keys(), key=len, reverse=True):
            original = mapping[pseudonym]
            if pseudonym in restored:
                restored = restored.replace(pseudonym, original)
                substitutions.append({
                    "pseudonym": pseudonym,
                    "original": original,
                })

        return restored, substitutions

    def get_session_mapping(self, session_id: str) -> dict:
        """Return full pseudonym→original mapping for a session (demo/audit)."""
        if not self.session_exists(session_id):
            return {}
        with self._lock:
            return dict(self._sessions[session_id]["pseudonym_to_original"])

    def list_sessions(self) -> list:
        with self._lock:
            return [
                {
                    "session_id": sid,
                    "pseudonym_count": len(s["pseudonym_to_original"]),
                }
                for sid, s in self._sessions.items()
            ]


# ── Global singleton ──────────────────────────────────────────────────────
# Production: replace with Redis-backed vault + session TTL
vault = PseudonymVault()
