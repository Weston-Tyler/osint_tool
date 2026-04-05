"""Unit tests for Pydantic validation models."""

import pytest
from datetime import datetime, timezone

from api.models.ais import AISPositionMessage, AISGapEvent
from api.models.uas import UASDetectionMessage
from api.models.entities import AlertEvent


class TestAISPositionMessage:
    def test_valid_position(self):
        pos = AISPositionMessage(
            source="test",
            mmsi="123456789",
            timestamp=datetime.now(timezone.utc),
            lat=5.0,
            lon=-76.0,
        )
        assert pos.mmsi == "123456789"
        assert pos.heading == 511  # default

    def test_invalid_mmsi_length(self):
        with pytest.raises(ValueError):
            AISPositionMessage(
                source="test",
                mmsi="12345",  # too short
                timestamp=datetime.now(timezone.utc),
                lat=5.0,
                lon=-76.0,
            )

    def test_invalid_mmsi_format(self):
        with pytest.raises(ValueError):
            AISPositionMessage(
                source="test",
                mmsi="12345ABCD",  # non-numeric
                timestamp=datetime.now(timezone.utc),
                lat=5.0,
                lon=-76.0,
            )

    def test_lat_bounds(self):
        with pytest.raises(ValueError):
            AISPositionMessage(
                source="test",
                mmsi="123456789",
                timestamp=datetime.now(timezone.utc),
                lat=91.0,  # out of bounds
                lon=-76.0,
            )

    def test_imo_normalization(self):
        pos = AISPositionMessage(
            source="test",
            mmsi="123456789",
            imo="IMO 1234567",
            timestamp=datetime.now(timezone.utc),
            lat=5.0,
            lon=-76.0,
        )
        assert pos.imo == "1234567"

    def test_imo_invalid_cleared(self):
        pos = AISPositionMessage(
            source="test",
            mmsi="123456789",
            imo="INVALID",
            timestamp=datetime.now(timezone.utc),
            lat=5.0,
            lon=-76.0,
        )
        assert pos.imo is None


class TestUASDetectionMessage:
    def test_valid_detection(self):
        det = UASDetectionMessage(
            event_id="test_001",
            source="wardragon",
            detection_timestamp=datetime.now(timezone.utc),
            detection_lat=32.543,
            detection_lon=-117.021,
            detection_alt_m=45.0,
            sensor_type="RF_PASSIVE",
            detection_method="RF_LINK_INTERCEPT",
            detection_confidence=0.92,
        )
        assert det.uas_classification == "UNKNOWN"  # default

    def test_confidence_bounds(self):
        with pytest.raises(ValueError):
            UASDetectionMessage(
                event_id="test_002",
                source="test",
                detection_timestamp=datetime.now(timezone.utc),
                detection_lat=32.0,
                detection_lon=-117.0,
                sensor_type="RADAR",
                detection_method="RADAR_RETURN",
                detection_confidence=1.5,  # out of bounds
            )


class TestAlertEvent:
    def test_valid_alert(self):
        alert = AlertEvent(
            alert_id="alert_001",
            alert_type="SANCTIONED_VESSEL_PROXIMITY",
            severity="HIGH",
            timestamp=datetime.now(timezone.utc),
        )
        assert alert.confidence == 0.5  # default
