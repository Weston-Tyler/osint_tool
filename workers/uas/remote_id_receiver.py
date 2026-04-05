"""Open Drone ID Remote ID receiver and parser.

Parses FAA Part 89 Remote ID broadcasts received via WiFi NAN or BLE 5.
Produces structured UAS detection events and feeds them to Kafka.

Standard: https://mavlink.io/en/services/opendroneid.html

Supports:
- BasicID (UAS serial number / registration)
- Location (lat, lon, alt, speed, heading)
- OperatorID (FAA registration number)
- SelfID (operator description)
- System (operator location, area count, timestamps)
"""

import json
import logging
import os
import struct
from datetime import datetime

from kafka import KafkaProducer

logger = logging.getLogger("mda.uas.remote_id")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# Open Drone ID message type constants
ODID_MSG_BASIC_ID = 0x0
ODID_MSG_LOCATION = 0x1
ODID_MSG_AUTH = 0x2
ODID_MSG_SELF_ID = 0x3
ODID_MSG_SYSTEM = 0x4
ODID_MSG_OPERATOR_ID = 0x5
ODID_MSG_PACK = 0xF

# UA type enumeration
UA_TYPE = {
    0: "NONE",
    1: "AEROPLANE",
    2: "HELICOPTER_MULTIROTOR",
    3: "GYROPLANE",
    4: "HYBRID_LIFT",
    5: "ORNITHOPTER",
    6: "GLIDER",
    7: "KITE",
    8: "FREE_BALLOON",
    9: "CAPTIVE_BALLOON",
    10: "AIRSHIP",
    11: "FREE_FALL_PARACHUTE",
    12: "ROCKET",
    13: "TETHERED_POWERED_AIRCRAFT",
    14: "GROUND_OBSTACLE",
    15: "OTHER",
}

# ID type enumeration
ID_TYPE = {
    0: "NONE",
    1: "SERIAL_NUMBER",
    2: "CAA_REGISTRATION",
    3: "UTM_ASSIGNED",
    4: "SPECIFIC_SESSION",
}

# Operator location type
OP_LOCATION_TYPE = {
    0: "TAKEOFF",
    1: "LIVE_GNSS",
    2: "FIXED",
}


class RemoteIDParser:
    """Parse raw Open Drone ID messages from WiFi NAN or BLE payloads."""

    def __init__(self):
        # Track partial messages per UAS (need BasicID + Location to form complete detection)
        self.uas_state: dict[str, dict] = {}

    def parse_message(self, raw_bytes: bytes, received_at: str, rssi: int = 0,
                      receiver_id: str = "unknown") -> dict | None:
        """Parse a raw Open Drone ID message.

        Returns parsed message dict, or None if unparseable.
        """
        if len(raw_bytes) < 2:
            return None

        msg_type = (raw_bytes[0] & 0xF0) >> 4
        proto_version = raw_bytes[0] & 0x0F
        msg_data = raw_bytes[1:]

        base = {
            "received_at": received_at,
            "rssi_dbm": rssi,
            "source": "remote_id",
            "receiver_id": receiver_id,
            "proto_version": proto_version,
        }

        if msg_type == ODID_MSG_BASIC_ID:
            return self._parse_basic_id(msg_data, base)
        elif msg_type == ODID_MSG_LOCATION:
            return self._parse_location(msg_data, base)
        elif msg_type == ODID_MSG_SELF_ID:
            return self._parse_self_id(msg_data, base)
        elif msg_type == ODID_MSG_SYSTEM:
            return self._parse_system(msg_data, base)
        elif msg_type == ODID_MSG_OPERATOR_ID:
            return self._parse_operator_id(msg_data, base)
        elif msg_type == ODID_MSG_AUTH:
            return {**base, "msg_type": "AUTH", "raw_hex": msg_data[:25].hex()}

        return None

    def _parse_basic_id(self, data: bytes, base: dict) -> dict | None:
        if len(data) < 21:
            return None
        id_type = (data[0] & 0xF0) >> 4
        ua_type = data[0] & 0x0F
        uas_id = data[1:21].decode("ascii", errors="replace").rstrip("\x00").strip()

        return {
            **base,
            "msg_type": "BASIC_ID",
            "uas_id": uas_id,
            "id_type": ID_TYPE.get(id_type, f"UNKNOWN_{id_type}"),
            "ua_type": UA_TYPE.get(ua_type, f"UNKNOWN_{ua_type}"),
        }

    def _parse_location(self, data: bytes, base: dict) -> dict | None:
        if len(data) < 18:
            return None

        status = (data[0] & 0xF0) >> 4
        height_type = (data[0] & 0x04) >> 2
        ew_direction = (data[0] & 0x02) >> 1
        speed_mult = data[0] & 0x01

        direction = data[1]  # units of 2 degrees (0-179 = 0-358 deg)
        if ew_direction:
            direction += 180
        direction = direction * 2 if direction < 180 else (direction - 180) * 2

        speed_h_raw = data[2]
        speed_v_raw = data[3]  # signed, units of 0.5 m/s
        speed_mult_factor = 0.75 if speed_mult else 0.25
        speed_h = speed_h_raw * speed_mult_factor

        lat = struct.unpack_from("<i", data, 4)[0] * 1e-7
        lon = struct.unpack_from("<i", data, 8)[0] * 1e-7

        alt_pressure = struct.unpack_from("<H", data, 12)[0] * 0.5 - 1000
        alt_geodetic = struct.unpack_from("<H", data, 14)[0] * 0.5 - 1000
        height = struct.unpack_from("<H", data, 16)[0] * 0.5 - 1000

        # Validate coordinates
        if lat == 0 and lon == 0:
            return None
        if abs(lat) > 90 or abs(lon) > 180:
            return None

        return {
            **base,
            "msg_type": "LOCATION",
            "op_status": status,
            "lat": round(lat, 7),
            "lon": round(lon, 7),
            "altitude_pressure_m": round(alt_pressure, 1),
            "altitude_geodetic_m": round(alt_geodetic, 1),
            "height_above_ground_m": round(height, 1),
            "height_type": "AGL" if height_type == 0 else "MSL",
            "speed_horizontal_mps": round(speed_h, 2),
            "heading_deg": direction,
        }

    def _parse_self_id(self, data: bytes, base: dict) -> dict | None:
        if len(data) < 24:
            return None
        desc_type = data[0]
        description = data[1:24].decode("ascii", errors="replace").rstrip("\x00").strip()
        return {
            **base,
            "msg_type": "SELF_ID",
            "description_type": desc_type,
            "description": description,
        }

    def _parse_system(self, data: bytes, base: dict) -> dict | None:
        if len(data) < 18:
            return None

        op_location_type = (data[0] & 0xC0) >> 6
        classification = (data[0] & 0x0E) >> 1

        op_lat = struct.unpack_from("<i", data, 1)[0] * 1e-7
        op_lon = struct.unpack_from("<i", data, 5)[0] * 1e-7
        area_count = struct.unpack_from("<H", data, 9)[0]
        area_radius_m = data[11] * 10
        area_ceiling_m = struct.unpack_from("<H", data, 12)[0] * 0.5 - 1000
        area_floor_m = struct.unpack_from("<H", data, 14)[0] * 0.5 - 1000

        result = {
            **base,
            "msg_type": "SYSTEM",
            "operator_location_type": OP_LOCATION_TYPE.get(op_location_type, "UNKNOWN"),
            "classification": classification,
            "area_count": area_count,
            "area_radius_m": area_radius_m,
            "area_ceiling_m": round(area_ceiling_m, 1),
            "area_floor_m": round(area_floor_m, 1),
        }

        if abs(op_lat) <= 90 and abs(op_lon) <= 180 and not (op_lat == 0 and op_lon == 0):
            result["operator_lat"] = round(op_lat, 7)
            result["operator_lon"] = round(op_lon, 7)

        return result

    def _parse_operator_id(self, data: bytes, base: dict) -> dict | None:
        if len(data) < 21:
            return None
        op_id_type = data[0]
        operator_id = data[1:21].decode("ascii", errors="replace").rstrip("\x00").strip()
        return {
            **base,
            "msg_type": "OPERATOR_ID",
            "operator_id_type": op_id_type,
            "operator_id": operator_id,
        }

    def assemble_detection(self, uas_id: str) -> dict | None:
        """Assemble a complete UAS detection from accumulated partial messages."""
        state = self.uas_state.get(uas_id)
        if not state:
            return None

        basic = state.get("BASIC_ID")
        location = state.get("LOCATION")
        if not basic or not location:
            return None

        operator = state.get("OPERATOR_ID", {})
        system = state.get("SYSTEM", {})

        return {
            "event_id": f"rid_{uas_id}_{int(datetime.utcnow().timestamp())}",
            "source": "remote_id",
            "detection_timestamp": location.get("received_at", datetime.utcnow().isoformat()),
            "detection_lat": location["lat"],
            "detection_lon": location["lon"],
            "detection_alt_m": location.get("altitude_geodetic_m", 0),
            "sensor_type": "REMOTE_ID",
            "detection_method": "REMOTE_ID_BROADCAST",
            "detection_confidence": 0.95,
            "uas_classification": _map_ua_type(basic.get("ua_type", "")),
            "remote_id_broadcast": True,
            "remote_id_uas_id": uas_id,
            "remote_id_operator_id": operator.get("operator_id"),
            "speed_mps": location.get("speed_horizontal_mps", 0),
            "heading_deg": location.get("heading_deg"),
            "height_above_ground_m": location.get("height_above_ground_m"),
            "operator_lat": system.get("operator_lat"),
            "operator_lon": system.get("operator_lon"),
        }

    def ingest_message(self, parsed: dict) -> dict | None:
        """Ingest a parsed message, accumulate state, and emit detection if complete."""
        if not parsed:
            return None

        msg_type = parsed.get("msg_type")

        # Identify the UAS from BasicID
        uas_id = parsed.get("uas_id")
        if msg_type == "BASIC_ID" and uas_id:
            if uas_id not in self.uas_state:
                self.uas_state[uas_id] = {}
            self.uas_state[uas_id]["BASIC_ID"] = parsed
            return self.assemble_detection(uas_id)

        # For non-BasicID messages, try to match to known UAS by timing
        # In practice, messages arrive in rapid succession for the same UAS
        if msg_type in ("LOCATION", "OPERATOR_ID", "SYSTEM", "SELF_ID"):
            # Associate with most recently seen UAS
            if self.uas_state:
                latest_id = list(self.uas_state.keys())[-1]
                self.uas_state[latest_id][msg_type] = parsed
                if msg_type == "LOCATION":
                    return self.assemble_detection(latest_id)

        return None


def _map_ua_type(ua_type: str) -> str:
    """Map Open Drone ID UA type to MDA classification."""
    mapping = {
        "HELICOPTER_MULTIROTOR": "MULTIROTOR",
        "AEROPLANE": "FIXED_WING",
        "HYBRID_LIFT": "HYBRID",
        "GLIDER": "FIXED_WING",
        "KITE": "OTHER",
    }
    return mapping.get(ua_type, "UNKNOWN")


class RemoteIDKafkaPublisher:
    """Publishes assembled Remote ID detections to Kafka."""

    def __init__(self):
        self.parser = RemoteIDParser()
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )

    def process_raw_frame(self, raw_bytes: bytes, rssi: int = 0, receiver_id: str = "sensor_01"):
        """Process a raw Remote ID frame and publish to Kafka if complete."""
        now = datetime.utcnow().isoformat()
        parsed = self.parser.parse_message(raw_bytes, now, rssi, receiver_id)

        if parsed:
            # Publish raw message to remote_id topic
            self.producer.send("mda.uas.remote_id", parsed)

            # Try to assemble a complete detection
            detection = self.parser.ingest_message(parsed)
            if detection:
                self.producer.send("mda.uas.detections.raw", detection)
                logger.info(
                    "Remote ID detection: %s at (%.5f, %.5f) alt=%.0fm",
                    detection.get("remote_id_uas_id"),
                    detection.get("detection_lat", 0),
                    detection.get("detection_lon", 0),
                    detection.get("detection_alt_m", 0),
                )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    logger.info("Remote ID receiver ready — waiting for frames")
    # In production, this would be called from a WiFi monitor mode listener
    # or BLE scanner. See: WarDragon hardware integration guide.
    publisher = RemoteIDKafkaPublisher()
    logger.info("Use publisher.process_raw_frame(bytes, rssi, receiver_id) to ingest frames")
