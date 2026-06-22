"""
Swiss Data Detection — Custom Presidio Recognizers
===================================================
Covers PII types specific to Switzerland that Presidio's built-in
recognizers do not handle:

  • AHV Number     756.XXXX.XXXX.XX  (Swiss social insurance number)
  • Swiss IBAN     CH__ ____ ____ ____ ____ _
  • Swiss Phone    +41 XX XXX XX XX  /  044 XXX XX XX
  • Swiss UID      CHE-XXX.XXX.XXX   (company identifier)
  • Swiss ZIP+City  XXXX Zurich / Bern / etc.

These are registered into the Presidio AnalyzerEngine on startup.
"""

import re

try:
    from presidio_analyzer import PatternRecognizer, Pattern
    _PRESIDIO_OK = True
except ImportError:
    _PRESIDIO_OK = False
    PatternRecognizer = object   # type: ignore
    Pattern = object             # type: ignore


if _PRESIDIO_OK:


    # ── 1. AHV Number (Swiss SSN equivalent) ─────────────────────────────────
    # Format: 756.XXXX.XXXX.XX
    ahv_recognizer = PatternRecognizer(
        supported_entity="CH_AHV_NUMBER",
        name="SwissAHVRecognizer",
        patterns=[
            Pattern(
                name="ahv_dotted",
                regex=r"\b756\.\d{4}\.\d{4}\.\d{2}\b",
                score=0.97,
            ),
            Pattern(
                name="ahv_plain",
                regex=r"\b756\d{10}\b",
                score=0.90,
            ),
        ],
        context=["ahv", "avs", "social insurance", "versicherungsnummer", "svnr"],
    )


    # ── 2. Swiss IBAN ─────────────────────────────────────────────────────────
    # Format: CH__ XXXX XXXX XXXX XXXX X (21 chars)
    swiss_iban_recognizer = PatternRecognizer(
        supported_entity="CH_IBAN",
        name="SwissIBANRecognizer",
        patterns=[
            Pattern(
                name="ch_iban_spaced",
                regex=r"\bCH\d{2}(?:\s?\d{4}){4}\s?\d{1}\b",
                score=0.98,
            ),
            Pattern(
                name="ch_iban_plain",
                regex=r"\bCH\d{2}[A-Z0-9]{17}\b",
                score=0.95,
            ),
        ],
        context=["iban", "bank", "konto", "account", "kontonummer", "transfer"],
    )


    # ── 3. Swiss Phone Number ─────────────────────────────────────────────────
    swiss_phone_recognizer = PatternRecognizer(
        supported_entity="CH_PHONE",
        name="SwissPhoneRecognizer",
        patterns=[
            Pattern(
                name="ch_phone_intl",
                regex=r"\+41\s?\d{2}\s?\d{3}\s?\d{2}\s?\d{2}",
                score=0.95,
            ),
            Pattern(
                name="ch_phone_local",
                regex=r"\b0\d{2}\s\d{3}\s\d{2}\s\d{2}\b",
                score=0.88,
            ),
            Pattern(
                name="ch_mobile",
                regex=r"\b07[5-9]\s\d{3}\s\d{2}\s\d{2}\b",
                score=0.90,
            ),
        ],
        context=["phone", "tel", "mobile", "call", "reach", "contact", "telefon", "handy"],
    )


    # ── 4. Swiss UID (company number) ─────────────────────────────────────────
    swiss_uid_recognizer = PatternRecognizer(
        supported_entity="CH_UID",
        name="SwissUIDRecognizer",
        patterns=[
            Pattern(
                name="uid_dashed",
                regex=r"\bCHE-\d{3}\.\d{3}\.\d{3}\b",
                score=0.97,
            ),
            Pattern(
                name="uid_plain",
                regex=r"\bCHE\d{9}\b",
                score=0.88,
            ),
        ],
        context=["uid", "mwst", "vat", "firmennummer", "company", "unternehmens"],
    )


    # ── 5. Swiss Passport / ID card ───────────────────────────────────────────
    swiss_id_recognizer = PatternRecognizer(
        supported_entity="CH_PASSPORT",
        name="SwissPassportRecognizer",
        patterns=[
            Pattern(
                name="swiss_passport",
                regex=r"\b[A-Z]\d{7}\b",   # e.g. X1234567
                score=0.75,
            ),
        ],
        context=["passport", "reisepass", "ausweis", "identity", "id card", "pass"],
    )


    ALL_SWISS_RECOGNIZERS = [
        ahv_recognizer,
        swiss_iban_recognizer,
        swiss_phone_recognizer,
        swiss_uid_recognizer,
        swiss_id_recognizer,
    ]

else:
    ALL_SWISS_RECOGNIZERS = []
