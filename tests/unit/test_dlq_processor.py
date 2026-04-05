"""Unit tests for DLQ processor error categorization and failure tracking."""

import time

import pytest

from processors.graph.dlq_processor import DLQError, FailureTracker


class TestDLQErrorCategorization:
    def test_validation_error(self):
        assert DLQError.categorize("ValidationError: field 'mmsi' required") == "SCHEMA_VALIDATION"

    def test_json_decode_error(self):
        assert DLQError.categorize("JSONDecodeError: invalid syntax") == "PARSE_ERROR"

    def test_connection_error(self):
        assert DLQError.categorize("ConnectionError: refused") == "SOURCE_UNAVAILABLE"

    def test_timeout_error(self):
        assert DLQError.categorize("TimeoutError: 30s elapsed") == "SOURCE_TIMEOUT"

    def test_unknown_error(self):
        assert DLQError.categorize("SomethingWeirdHappened") == "UNKNOWN_ERROR"

    def test_retryable_categories(self):
        assert DLQError.is_retryable("SOURCE_UNAVAILABLE") is True
        assert DLQError.is_retryable("SOURCE_TIMEOUT") is True
        assert DLQError.is_retryable("HTTP_ERROR") is True
        assert DLQError.is_retryable("SCHEMA_VALIDATION") is False
        assert DLQError.is_retryable("PARSE_ERROR") is False


class TestFailureTracker:
    def test_record_and_count(self):
        tracker = FailureTracker(window_seconds=60)
        assert tracker.record("source_a") == 1
        assert tracker.record("source_a") == 2
        assert tracker.record("source_b") == 1
        assert tracker.record("source_a") == 3

    def test_counts_per_source(self):
        tracker = FailureTracker(window_seconds=60)
        tracker.record("ais")
        tracker.record("ais")
        tracker.record("gdelt")
        counts = tracker.get_counts()
        assert counts["ais"] == 2
        assert counts["gdelt"] == 1

    def test_window_expiry(self):
        tracker = FailureTracker(window_seconds=1)
        tracker.record("source_a")
        assert tracker.record("source_a") == 2
        time.sleep(1.5)
        # After window expires, count resets
        assert tracker.record("source_a") == 1
