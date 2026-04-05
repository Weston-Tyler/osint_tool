"""Unit tests for community data submission validator."""

import pytest

from community.submission_validator import (
    CommunityDataSubmission,
    scan_for_pii,
    validate_submission,
)


class TestPIIScanner:
    def test_detects_ssn(self):
        findings = scan_for_pii("His SSN is 123-45-6789 on file")
        assert "SSN" in findings

    def test_detects_email(self):
        findings = scan_for_pii("Contact john@example.com for info")
        assert "Email Address" in findings

    def test_detects_phone(self):
        findings = scan_for_pii("Call 555-123-4567 for details")
        assert "Phone Number" in findings

    def test_clean_text(self):
        findings = scan_for_pii("Vessel EVER FORWARD observed at anchorage near Cartagena")
        assert len(findings) == 0


class TestSubmissionValidation:
    def _valid_submission(self) -> dict:
        return {
            "submitter_handle": "maritime_watcher_42",
            "submission_type": "VESSEL_SIGHTING",
            "confidence_self_reported": 0.7,
            "source_description": "Visual observation from shore at Port of Cartagena",
            "data": {
                "vessel_name": "PACIFIC DAWN",
                "lat": 10.39,
                "lon": -75.51,
                "observation_time": "2026-03-28T14:30:00Z",
                "description": "Large tanker anchored offshore with no AIS signal visible",
            },
            "source_urls": ["https://www.marinetraffic.com/en/ais/details/ships/1234"],
            "license": "CC-BY-4.0",
            "pii_excluded": True,
        }

    def test_valid_submission(self):
        valid, errors = validate_submission(self._valid_submission())
        assert valid is True
        assert len(errors) == 0

    def test_rejects_missing_pii_confirmation(self):
        sub = self._valid_submission()
        sub["pii_excluded"] = False
        valid, errors = validate_submission(sub)
        assert valid is False

    def test_rejects_invalid_type(self):
        sub = self._valid_submission()
        sub["submission_type"] = "INVALID_TYPE"
        valid, errors = validate_submission(sub)
        assert valid is False

    def test_rejects_no_source_urls(self):
        sub = self._valid_submission()
        sub["source_urls"] = []
        valid, errors = validate_submission(sub)
        assert valid is False

    def test_warns_high_confidence(self):
        sub = self._valid_submission()
        sub["confidence_self_reported"] = 0.99
        valid, errors = validate_submission(sub)
        # Should still validate but warn
        assert any("confidence" in e.lower() for e in errors)
