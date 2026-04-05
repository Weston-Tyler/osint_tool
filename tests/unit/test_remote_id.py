"""Unit tests for Open Drone ID Remote ID parser."""

import struct

import pytest

from workers.uas.remote_id_receiver import RemoteIDParser, _map_ua_type


class TestRemoteIDParser:
    def setup_method(self):
        self.parser = RemoteIDParser()

    def _make_basic_id(self, uas_id: str = "FA-2024-001234", ua_type: int = 2, id_type: int = 2) -> bytes:
        """Build a BasicID message payload."""
        header = bytes([(0x0 << 4) | 0x01])  # msg_type=0, proto=1
        type_byte = bytes([(id_type << 4) | ua_type])
        id_bytes = uas_id.encode("ascii").ljust(20, b"\x00")
        return header + type_byte + id_bytes

    def _make_location(self, lat: float = 32.543, lon: float = -117.021, alt: float = 45.0) -> bytes:
        """Build a Location message payload."""
        header = bytes([(0x1 << 4) | 0x01])  # msg_type=1, proto=1
        status_byte = bytes([0x00])
        direction = bytes([170])  # ~340 degrees
        speed_h = bytes([50])  # 12.5 m/s at 0.25 mult
        speed_v = bytes([0])

        lat_int = int(lat / 1e-7)
        lon_int = int(lon / 1e-7)
        alt_pressure = int((alt + 1000) / 0.5)
        alt_geodetic = int((alt + 1000) / 0.5)
        height = int((alt + 1000) / 0.5)

        data = (
            status_byte
            + direction
            + speed_h
            + speed_v
            + struct.pack("<i", lat_int)
            + struct.pack("<i", lon_int)
            + struct.pack("<H", alt_pressure)
            + struct.pack("<H", alt_geodetic)
            + struct.pack("<H", height)
        )
        return header + data

    def test_parse_basic_id(self):
        raw = self._make_basic_id("FA-2024-001234", ua_type=2, id_type=2)
        result = self.parser.parse_message(raw, "2026-03-22T02:15:00Z", rssi=-60)
        assert result is not None
        assert result["msg_type"] == "BASIC_ID"
        assert result["uas_id"] == "FA-2024-001234"
        assert result["ua_type"] == "HELICOPTER_MULTIROTOR"
        assert result["id_type"] == "CAA_REGISTRATION"

    def test_parse_location(self):
        raw = self._make_location(32.543, -117.021, 45.0)
        result = self.parser.parse_message(raw, "2026-03-22T02:15:00Z")
        assert result is not None
        assert result["msg_type"] == "LOCATION"
        assert abs(result["lat"] - 32.543) < 0.001
        assert abs(result["lon"] - (-117.021)) < 0.001

    def test_parse_operator_id(self):
        header = bytes([(0x5 << 4) | 0x01])
        op_type = bytes([0])
        op_id = b"OP-12345-FAA\x00\x00\x00\x00\x00\x00\x00\x00"
        raw = header + op_type + op_id
        result = self.parser.parse_message(raw, "2026-03-22T02:15:00Z")
        assert result is not None
        assert result["msg_type"] == "OPERATOR_ID"
        assert result["operator_id"] == "OP-12345-FAA"

    def test_parse_too_short(self):
        assert self.parser.parse_message(b"\x00", "now") is None
        assert self.parser.parse_message(b"", "now") is None

    def test_assemble_detection(self):
        # Ingest BasicID first
        basic = self._make_basic_id("TEST-UAS-001")
        basic_parsed = self.parser.parse_message(basic, "2026-03-22T02:15:00Z")
        self.parser.ingest_message(basic_parsed)

        # Then Location
        loc = self._make_location(32.0, -117.0, 50.0)
        loc_parsed = self.parser.parse_message(loc, "2026-03-22T02:15:01Z")
        detection = self.parser.ingest_message(loc_parsed)

        assert detection is not None
        assert detection["remote_id_uas_id"] == "TEST-UAS-001"
        assert detection["sensor_type"] == "REMOTE_ID"
        assert detection["detection_confidence"] == 0.95


class TestUATypeMapping:
    def test_multirotor(self):
        assert _map_ua_type("HELICOPTER_MULTIROTOR") == "MULTIROTOR"

    def test_fixed_wing(self):
        assert _map_ua_type("AEROPLANE") == "FIXED_WING"

    def test_unknown(self):
        assert _map_ua_type("SOMETHING_NEW") == "UNKNOWN"
