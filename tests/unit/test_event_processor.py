"""Unit tests for AIS gap detection, STS detection, and risk scoring."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest


# Patch Kafka before importing event_processor
@pytest.fixture(autouse=True)
def mock_kafka():
    with patch("processors.graph.event_processor.KafkaProducer") as mock_prod:
        mock_instance = MagicMock()
        mock_prod.return_value = mock_instance
        yield mock_instance


def make_position(mmsi: str, lat: float, lon: float, speed: float = 10.0, nav_status: int = 0, ts: datetime | None = None):
    if ts is None:
        ts = datetime.now(timezone.utc)
    return {
        "mmsi": mmsi,
        "lat": lat,
        "lon": lon,
        "speed_kts": speed,
        "nav_status": nav_status,
        "timestamp": ts.isoformat(),
    }


class TestAISGapDetector:
    def test_gap_detection_deliberate(self, mock_kafka):
        from processors.graph.event_processor import AISGapDetector

        detector = AISGapDetector()
        detector.producer = mock_kafka

        now = datetime.now(timezone.utc)

        # First position: vessel moving
        pos1 = make_position("123456789", 5.0, -76.0, speed=15.0, nav_status=0, ts=now - timedelta(hours=13))
        detector.process_position(pos1)

        # Resume 13 hours later, 100km away
        pos2 = make_position("123456789", 4.2, -76.8, speed=14.0, nav_status=0, ts=now)
        detector.process_position(pos2)

        # Verify gap event was published
        mock_kafka.send.assert_called()
        call_args = mock_kafka.send.call_args
        assert call_args[0][0] == "mda.ais.gaps.detected"
        gap_event = call_args[0][1]
        assert gap_event["vessel_mmsi"] == "123456789"
        assert gap_event["gap_duration_hours"] >= 12.0
        assert gap_event["risk_flag"] is True

    def test_no_gap_for_short_absence(self, mock_kafka):
        from processors.graph.event_processor import AISGapDetector

        detector = AISGapDetector()
        detector.producer = mock_kafka

        now = datetime.now(timezone.utc)

        # First position
        pos1 = make_position("987654321", 5.0, -76.0, ts=now - timedelta(hours=2))
        detector.process_position(pos1)

        # Resume 2 hours later (below threshold)
        pos2 = make_position("987654321", 5.01, -76.01, ts=now)
        detector.process_position(pos2)

        # No gap event should be published
        mock_kafka.send.assert_not_called()

    def test_anchored_vessel_technical_failure(self, mock_kafka):
        from processors.graph.event_processor import AISGapDetector

        detector = AISGapDetector()
        detector.producer = mock_kafka

        now = datetime.now(timezone.utc)

        # Anchored vessel (nav_status=1, speed=0.1)
        pos1 = make_position("111222333", 5.0, -76.0, speed=0.1, nav_status=1, ts=now - timedelta(hours=8))
        detector.process_position(pos1)

        # Same position 8 hours later
        pos2 = make_position("111222333", 5.0, -76.0, speed=0.0, nav_status=1, ts=now)
        detector.process_position(pos2)

        mock_kafka.send.assert_called()
        gap_event = mock_kafka.send.call_args[0][1]
        assert gap_event["probable_cause"] == "TECHNICAL_FAILURE_ANCHORED"


class TestCompositeRiskScorer:
    def test_sanctioned_vessel_max_risk(self):
        from processors.graph.event_processor import CompositeRiskScorer

        scorer = CompositeRiskScorer()
        vessel = {"sanctions_status": "SANCTIONED", "flag_state": "PA", "beneficial_owner": "Unknown"}
        events = {
            "owner_sanctioned": True,
            "ais_gap_recent_24h": True,
            "dark_sts_transfer": True,
            "cartel_route_transit": True,
            "near_sanctioned_vessel": True,
        }
        score = scorer.compute_risk_score(vessel, events)
        assert score == 10.0

    def test_clean_vessel_low_risk(self):
        from processors.graph.event_processor import CompositeRiskScorer

        scorer = CompositeRiskScorer()
        vessel = {"sanctions_status": "CLEAR", "flag_state": "US", "beneficial_owner": "Maersk Line A/S"}
        events = {}
        score = scorer.compute_risk_score(vessel, events)
        assert score < 1.0

    def test_high_risk_flag_state(self):
        from processors.graph.event_processor import CompositeRiskScorer

        scorer = CompositeRiskScorer()
        vessel = {"sanctions_status": None, "flag_state": "KP", "beneficial_owner": "Unknown"}
        events = {}
        score = scorer.compute_risk_score(vessel, events)
        assert score >= 2.0  # flag_state + opaque ownership


class TestHaversine:
    def test_known_distance(self):
        from processors.graph.event_processor import haversine_km

        # New York to London ~5570 km
        dist = haversine_km(40.7128, -74.0060, 51.5074, -0.1278)
        assert 5500 < dist < 5650

    def test_zero_distance(self):
        from processors.graph.event_processor import haversine_km

        dist = haversine_km(0.0, 0.0, 0.0, 0.0)
        assert dist == 0.0
