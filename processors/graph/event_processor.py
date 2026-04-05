"""Real-time event processing engine.

Implements AIS gap detection, STS transfer correlation, sanctioned vessel
proximity monitoring, and composite risk scoring.
"""

import json
import logging
import math
import os
from datetime import datetime, timedelta

from kafka import KafkaConsumer, KafkaProducer

logger = logging.getLogger("mda.event_processor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance in kilometers."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance in nautical miles."""
    return haversine_km(lat1, lon1, lat2, lon2) / 1.852


class AISGapDetector:
    """Detect AIS gaps by tracking last-seen positions per vessel.

    A gap is defined as: vessel not seen for > GAP_THRESHOLD_HOURS
    AND vessel was previously active.
    """

    GAP_THRESHOLD_HOURS = 6.0
    CRITICAL_THRESHOLD_HOURS = 24.0

    def __init__(self):
        self.last_seen: dict = {}
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )

    def process_position(self, position: dict):
        mmsi = position.get("mmsi")
        if not mmsi:
            return

        try:
            ts = position.get("timestamp", "")
            if isinstance(ts, str):
                now = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            else:
                now = ts
        except (ValueError, TypeError):
            return

        if mmsi in self.last_seen:
            last = self.last_seen[mmsi]
            gap_hours = (now - last["timestamp"]).total_seconds() / 3600

            if gap_hours >= self.GAP_THRESHOLD_HOURS:
                displacement_km = haversine_km(last["lat"], last["lon"], position["lat"], position["lon"])
                implied_speed = displacement_km / (gap_hours * 1.852) if gap_hours > 0 else 0

                probable_cause = self._assess_gap_cause(
                    gap_hours, displacement_km, implied_speed, last["nav_status"], last["speed_kts"]
                )

                severity = "CRITICAL" if gap_hours >= 24.0 else "HIGH" if gap_hours >= 12.0 else "MEDIUM"

                gap_event = {
                    "event_id": f"ais_gap_{mmsi}_{int(last['timestamp'].timestamp())}",
                    "vessel_mmsi": mmsi,
                    "gap_start_time": last["timestamp"].isoformat(),
                    "gap_end_time": now.isoformat(),
                    "gap_duration_hours": round(gap_hours, 2),
                    "last_position_lat": last["lat"],
                    "last_position_lon": last["lon"],
                    "last_speed_kts": last["speed_kts"],
                    "last_nav_status": last["nav_status"],
                    "resume_position_lat": position["lat"],
                    "resume_position_lon": position["lon"],
                    "resume_speed_kts": position.get("speed_kts", 0),
                    "displacement_km": round(displacement_km, 2),
                    "implied_speed_kts": round(implied_speed, 2),
                    "probable_cause": probable_cause,
                    "risk_flag": gap_hours >= 12.0 or implied_speed > 5.0,
                    "severity": severity,
                    "source": "ais_gap_detector_v1",
                    "confidence": 0.80,
                }
                self.producer.send("mda.ais.gaps.detected", gap_event)
                logger.info("AIS gap: MMSI %s, %.1fh, cause=%s", mmsi, gap_hours, probable_cause)

        self.last_seen[mmsi] = {
            "timestamp": now,
            "lat": position["lat"],
            "lon": position["lon"],
            "speed_kts": position.get("speed_kts", 0),
            "nav_status": position.get("nav_status", 15),
        }

    def _assess_gap_cause(
        self, gap_hours: float, displacement_km: float, implied_speed: float, nav_status: int, last_speed: float
    ) -> str:
        if nav_status in (1, 5) and displacement_km < 1.0:
            return "TECHNICAL_FAILURE_ANCHORED"
        if last_speed > 5.0 and displacement_km > 50.0:
            return "AIS_OFF_DELIBERATE"
        if gap_hours > 24.0 and displacement_km > 100.0:
            return "AIS_OFF_DELIBERATE"
        if 3.0 < implied_speed < 20.0:
            return "COVERAGE_GAP"
        if implied_speed > 40.0:
            return "SPOOFING_SUSPECTED"
        return "UNKNOWN"


class STSDetector:
    """Detect probable ship-to-ship transfers by monitoring vessel proximity.

    Two vessels within 0.5nm for > 30 minutes = STS candidate.
    """

    PROXIMITY_NM = 0.5
    DURATION_THRESHOLD_MIN = 30

    def __init__(self):
        self.active_positions: dict = {}
        self.candidate_pairs: dict = {}
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )

    def process_position(self, position: dict):
        mmsi = position.get("mmsi")
        if not mmsi:
            return

        lat = position["lat"]
        lon = position["lon"]
        try:
            ts = position.get("timestamp", "")
            if isinstance(ts, str):
                now = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            else:
                now = ts
        except (ValueError, TypeError):
            return

        for other_mmsi, other_pos in list(self.active_positions.items()):
            if other_mmsi == mmsi:
                continue

            dist_nm = haversine_nm(lat, lon, other_pos["lat"], other_pos["lon"])
            pair_key = frozenset({mmsi, other_mmsi})

            if dist_nm <= self.PROXIMITY_NM:
                if pair_key not in self.candidate_pairs:
                    self.candidate_pairs[pair_key] = {
                        "first_seen": now,
                        "lat": (lat + other_pos["lat"]) / 2,
                        "lon": (lon + other_pos["lon"]) / 2,
                        "mmsi_a": mmsi,
                        "mmsi_b": other_mmsi,
                        "emitted": False,
                    }
                else:
                    candidate = self.candidate_pairs[pair_key]
                    duration_min = (now - candidate["first_seen"]).total_seconds() / 60

                    if duration_min >= self.DURATION_THRESHOLD_MIN and not candidate.get("emitted"):
                        event = {
                            "event_id": f"sts_{candidate['mmsi_a']}_{candidate['mmsi_b']}_{int(candidate['first_seen'].timestamp())}",
                            "event_type": "STS_PROBABLE",
                            "vessel_a_mmsi": candidate["mmsi_a"],
                            "vessel_b_mmsi": candidate["mmsi_b"],
                            "start_time": candidate["first_seen"].isoformat(),
                            "end_time": now.isoformat(),
                            "duration_min": round(duration_min, 1),
                            "lat": candidate["lat"],
                            "lon": candidate["lon"],
                            "source": "sts_detector_v1",
                            "confidence": 0.70,
                            "risk_flag": True,
                        }
                        self.producer.send("mda.ais.encounters.detected", event)
                        self.candidate_pairs[pair_key]["emitted"] = True
                        logger.info("STS: %s + %s, %.0fmin", candidate["mmsi_a"], candidate["mmsi_b"], duration_min)
            else:
                if pair_key in self.candidate_pairs:
                    del self.candidate_pairs[pair_key]

        self.active_positions[mmsi] = {"lat": lat, "lon": lon, "timestamp": now}


class CompositeRiskScorer:
    """Compute composite risk score (0-10) for a vessel based on all known indicators."""

    WEIGHTS = {
        "sanctions_status": 4.0,
        "owner_sanctioned": 3.0,
        "ais_gap_recent_24h": 2.0,
        "ais_gap_count_30d": 0.2,
        "sts_transfer_recent": 2.5,
        "dark_sts_transfer": 3.0,
        "high_risk_port_visit": 1.5,
        "flag_state_high_risk": 1.0,
        "flag_changes_past_year": 0.5,
        "ownership_opaque": 1.0,
        "cartel_route_transit": 2.5,
        "near_sanctioned_vessel": 1.5,
    }

    HIGH_RISK_FLAGS = {"PA", "LR", "MH", "KH", "TG", "SL", "KP", "IR", "SY", "VE", "CU"}

    def compute_risk_score(self, vessel: dict, recent_events: dict) -> float:
        score = 0.0

        if vessel.get("sanctions_status") == "SANCTIONED":
            score += self.WEIGHTS["sanctions_status"]
        if recent_events.get("owner_sanctioned"):
            score += self.WEIGHTS["owner_sanctioned"]
        if recent_events.get("ais_gap_recent_24h"):
            score += self.WEIGHTS["ais_gap_recent_24h"]

        gap_count = min(recent_events.get("ais_gap_count_30d", 0), 5)
        score += gap_count * self.WEIGHTS["ais_gap_count_30d"]

        if recent_events.get("sts_transfer_recent"):
            score += self.WEIGHTS["sts_transfer_recent"]
        if recent_events.get("dark_sts_transfer"):
            score += self.WEIGHTS["dark_sts_transfer"]
        if recent_events.get("high_risk_port_visit"):
            score += self.WEIGHTS["high_risk_port_visit"]

        flag = vessel.get("flag_state", "")
        if flag in self.HIGH_RISK_FLAGS:
            score += self.WEIGHTS["flag_state_high_risk"]

        flag_changes = min(recent_events.get("flag_changes_past_year", 0), 3)
        score += flag_changes * self.WEIGHTS["flag_changes_past_year"]

        if not vessel.get("beneficial_owner") or vessel.get("beneficial_owner") == "Unknown":
            score += self.WEIGHTS["ownership_opaque"]

        if recent_events.get("cartel_route_transit"):
            score += self.WEIGHTS["cartel_route_transit"]
        if recent_events.get("near_sanctioned_vessel"):
            score += self.WEIGHTS["near_sanctioned_vessel"]

        return min(round(score, 2), 10.0)


def run_event_processor():
    """Main event processing loop — consumes AIS positions and runs detectors."""
    gap_detector = AISGapDetector()
    sts_detector = STSDetector()

    consumer = KafkaConsumer(
        "mda.ais.positions.raw",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="mda-event-processor",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        max_poll_records=500,
    )

    logger.info("Event processor started")

    for msg in consumer:
        position = msg.value
        gap_detector.process_position(position)
        sts_detector.process_position(position)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    run_event_processor()
