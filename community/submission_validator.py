"""Community data submission validator.

Validates community-submitted intelligence data before ingestion into
the MDA database. Enforces PII exclusion, source attribution, and schema
conformance.
"""

import logging
import re
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger("mda.community.validator")

# Known submission types
VALID_SUBMISSION_TYPES = {
    "VESSEL_SIGHTING",
    "INTERDICTION_REPORT",
    "UAS_SIGHTING",
    "SANCTIONS_TIP",
    "ROUTE_INTELLIGENCE",
    "ORGANIZATION_INTEL",
    "PORT_ACTIVITY",
    "OTHER",
}

# Patterns that suggest PII leakage
PII_PATTERNS = [
    re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),  # US SSN
    re.compile(r"\b[A-Z]\d{8}\b"),  # US passport
    re.compile(r"\b\d{16}\b"),  # Credit card (rough)
    re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b"),  # Email
    re.compile(r"\b\d{3}[- ]?\d{3}[- ]?\d{4}\b"),  # US phone
]


class VesselSightingData(BaseModel):
    """Data schema for community vessel sighting submissions."""

    mmsi: Optional[str] = None
    imo: Optional[str] = None
    vessel_name: Optional[str] = None
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    observation_time: datetime
    description: str = Field(..., min_length=10, max_length=2000)
    vessel_behavior: Optional[str] = None  # e.g., "AIS off", "STS transfer", "suspicious cargo ops"
    photos_attached: bool = False


class UASReportData(BaseModel):
    """Data schema for community UAS sighting submissions."""

    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    altitude_estimate_m: Optional[float] = Field(None, ge=0, le=15000)
    observation_time: datetime
    description: str = Field(..., min_length=10, max_length=2000)
    drone_type: Optional[str] = None  # e.g., "DJI Mavic", "fixed-wing", "unknown"
    drone_count: int = Field(1, ge=1, le=50)
    direction_of_flight: Optional[str] = None
    photos_attached: bool = False


class InterdictionReportData(BaseModel):
    """Data schema for community interdiction/seizure report submissions."""

    event_date: datetime
    lat: Optional[float] = Field(None, ge=-90, le=90)
    lon: Optional[float] = Field(None, ge=-180, le=180)
    location_description: str = Field(..., min_length=5, max_length=500)
    agency: Optional[str] = None
    cargo_type: Optional[str] = None
    cargo_quantity_kg: Optional[float] = Field(None, ge=0)
    source_url: str = Field(..., min_length=10)
    description: str = Field(..., min_length=20, max_length=5000)


class CommunityDataSubmission(BaseModel):
    """Top-level schema for community-submitted data to the MDA database."""

    # Submitter info (for accountability, not published)
    submitter_handle: str = Field(..., min_length=2, max_length=64)
    submission_type: str

    # Data quality self-assessment
    confidence_self_reported: float = Field(..., ge=0.0, le=1.0)
    source_description: str = Field(..., min_length=10, max_length=1000)

    # Core data (type-specific, validated separately)
    data: dict

    # Provenance
    source_urls: list[str] = Field(..., min_length=1)
    license: str = Field(default="CC-BY-4.0")

    # PII confirmation
    pii_excluded: bool

    # Metadata
    submitted_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("submission_type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if v not in VALID_SUBMISSION_TYPES:
            raise ValueError(f"Invalid submission type: {v}. Must be one of: {VALID_SUBMISSION_TYPES}")
        return v

    @field_validator("pii_excluded")
    @classmethod
    def must_confirm_pii_exclusion(cls, v: bool) -> bool:
        if not v:
            raise ValueError("Submissions must confirm PII of private individuals has been excluded")
        return v

    @field_validator("source_urls")
    @classmethod
    def must_have_source(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("At least one source URL is required")
        for url in v:
            if not url.startswith(("http://", "https://")):
                raise ValueError(f"Invalid URL: {url}")
        return v


def scan_for_pii(text: str) -> list[str]:
    """Scan text for potential PII patterns.

    Returns list of PII type descriptions found.
    """
    findings = []
    labels = ["SSN", "Passport", "Credit Card", "Email Address", "Phone Number"]

    for pattern, label in zip(PII_PATTERNS, labels):
        if pattern.search(text):
            findings.append(label)

    return findings


def validate_submission(submission: dict) -> tuple[bool, list[str]]:
    """Validate a community submission.

    Returns (is_valid, list of error messages).
    """
    errors = []

    # Parse and validate top-level schema
    try:
        parsed = CommunityDataSubmission(**submission)
    except Exception as e:
        return False, [f"Schema validation failed: {e}"]

    # Scan all text fields for PII
    text_fields = [
        parsed.source_description,
        json.dumps(parsed.data) if isinstance(parsed.data, dict) else str(parsed.data),
    ]

    import json as json_mod
    for field_text in text_fields:
        pii_found = scan_for_pii(field_text)
        if pii_found:
            errors.append(f"Potential PII detected: {', '.join(pii_found)}")

    # Validate type-specific data
    try:
        if parsed.submission_type == "VESSEL_SIGHTING":
            VesselSightingData(**parsed.data)
        elif parsed.submission_type == "UAS_SIGHTING":
            UASReportData(**parsed.data)
        elif parsed.submission_type == "INTERDICTION_REPORT":
            InterdictionReportData(**parsed.data)
    except Exception as e:
        errors.append(f"Data validation failed for {parsed.submission_type}: {e}")

    # Check confidence is reasonable
    if parsed.confidence_self_reported > 0.95:
        errors.append("Self-reported confidence > 0.95 is unusual for community submissions; please verify")

    is_valid = len(errors) == 0
    return is_valid, errors


if __name__ == "__main__":
    import json

    # Example validation
    test_submission = {
        "submitter_handle": "maritime_watcher_42",
        "submission_type": "VESSEL_SIGHTING",
        "confidence_self_reported": 0.7,
        "source_description": "Visual observation from shore at Port of Cartagena",
        "data": {
            "vessel_name": "PACIFIC DAWN",
            "lat": 10.39,
            "lon": -75.51,
            "observation_time": "2026-03-28T14:30:00Z",
            "description": "Large tanker anchored offshore for 3 days with no AIS signal visible on MarineTraffic",
            "vessel_behavior": "AIS off, anchored in unusual location",
        },
        "source_urls": ["https://www.marinetraffic.com/en/ais/details/ships/1234"],
        "license": "CC-BY-4.0",
        "pii_excluded": True,
    }

    valid, errs = validate_submission(test_submission)
    print(f"Valid: {valid}")
    if errs:
        print(f"Errors: {errs}")
