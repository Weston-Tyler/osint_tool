"""ADS-B Exchange aircraft tracking ingester.

Fetches aircraft positions from ADS-B Exchange (via RapidAPI or direct), filters
for military hex codes, detects ISR surveillance patterns (orbiting/loitering),
correlates tracks with interdiction events, and publishes to Kafka at high
frequency for real-time situational awareness.

Source: https://www.adsbexchange.com/data/
"""

import json
import logging
import math
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.geospatial.adsb")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
ADSB_API_KEY = os.getenv("ADSB_API_KEY", "")
ADSB_API_URL = os.getenv(
    "ADSB_API_URL", "https://adsbexchange-com1.p.rapidapi.com/v2"
)

# Military/government ICAO hex-code prefixes.  This is a representative
# subset; expand for broader coverage.
MILITARY_HEX_PREFIXES = [
    "AE",    # US military (Army/Air Force)
    "AF",    # US military
    "A0",    # US government/military (block)
    "43C",   # UK military
    "3F",    # Germany military
    "3A",    # France military
    "300",   # France military
    "33",    # Italy military
    "340",   # Spain military
    "7C8",   # Australia military
    "710",   # Japan military
    "501",   # Portugal military
]

# MDA monitoring zones: name -> (centre_lat, centre_lon, radius_nm).
MDA_ZONES = {
    "eastern_pacific": (10.0, -90.0, 250),
    "caribbean": (17.0, -75.0, 200),
    "gulf_of_mexico": (25.0, -90.0, 200),
    "us_mx_border": (31.0, -110.0, 150),
    "west_africa": (5.0, 0.0, 200),
    "horn_of_africa": (10.0, 45.0, 200),
}

# Pattern detection thresholds.
ORBIT_MIN_TURNS = 3
ORBIT_HEADING_DELTA = 60  # degrees
LOITER_MAX_DISPLACEMENT_NM = 30  # max displacement from mean position


class ADSBIngester:
    """Fetches ADS-B data, filters military aircraft, detects surveillance
    patterns, and publishes to Kafka."""

    def __init__(
        self,
        api_key: str | None = None,
        kafka_bootstrap: str | None = None,
        zones: dict[str, tuple[float, float, int]] | None = None,
    ):
        self.api_key = api_key or ADSB_API_KEY
        self.kafka_bootstrap = kafka_bootstrap or KAFKA_BOOTSTRAP
        self.zones = zones or MDA_ZONES

        self._producer: KafkaProducer | None = None
        self.headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "adsbexchange-com1.p.rapidapi.com",
        }

        # In-memory track history: hex -> list of position dicts.
        # Used for pattern detection across polling cycles.
        self._track_history: dict[str, list[dict]] = defaultdict(list)
        self._max_track_points = 200  # keep last N positions per aircraft

    # ------------------------------------------------------------------
    # Kafka
    # ------------------------------------------------------------------

    def _get_producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
        return self._producer

    def _send(self, topic: str, record: dict) -> None:
        try:
            self._get_producer().send(topic, record)
        except Exception as exc:
            logger.error("Kafka send to %s failed: %s", topic, exc)
            self._get_producer().send(
                "mda.dlq",
                {"source": "adsb", "topic": topic, "error": str(exc), "raw": str(record)[:500]},
            )

    # ------------------------------------------------------------------
    # Data fetchers
    # ------------------------------------------------------------------

    def fetch_aircraft_near_point(
        self, lat: float, lon: float, radius_nm: int = 250
    ) -> list[dict]:
        """Fetch all aircraft within *radius_nm* of a point.

        Parameters
        ----------
        lat, lon : float
            Centre coordinates (WGS-84).
        radius_nm : int
            Search radius in nautical miles.

        Returns
        -------
        list[dict]  Raw ADS-B Exchange aircraft objects.
        """
        if not self.api_key:
            logger.warning("ADSB_API_KEY not set — skipping fetch")
            return []

        url = f"{ADSB_API_URL}/lat/{lat}/lon/{lon}/dist/{radius_nm}/"
        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            aircraft = data.get("ac", [])
            logger.debug("Fetched %d aircraft near (%.2f, %.2f)", len(aircraft), lat, lon)
            return aircraft
        except Exception as exc:
            logger.error("ADS-B API error at (%.2f, %.2f): %s", lat, lon, exc)
            return []

    def fetch_military_aircraft(self, bbox: list[float] | None = None) -> list[dict]:
        """Fetch aircraft and filter to those with military hex codes.

        Parameters
        ----------
        bbox : list[float], optional
            ``[west, south, east, north]``.  If provided, the bounding-box
            centre and diagonal half are used for the proximity query.
            If ``None``, all configured MDA zones are queried.

        Returns
        -------
        list[dict]  Military aircraft only.
        """
        if bbox:
            west, south, east, north = bbox
            lat = (south + north) / 2.0
            lon = (west + east) / 2.0
            radius_nm = int(self._haversine_nm(south, west, north, east) / 2) + 10
            all_ac = self.fetch_aircraft_near_point(lat, lon, min(radius_nm, 250))
        else:
            all_ac = []
            for _, (lat, lon, radius) in self.zones.items():
                all_ac.extend(self.fetch_aircraft_near_point(lat, lon, radius))

        military = [ac for ac in all_ac if self._is_military(ac)]
        logger.info("Found %d military aircraft out of %d total", len(military), len(all_ac))
        return military

    # ------------------------------------------------------------------
    # Military classification
    # ------------------------------------------------------------------

    @staticmethod
    def _is_military(aircraft: dict) -> bool:
        """Check if an aircraft hex code matches known military prefixes."""
        hex_code = aircraft.get("hex", "").upper().strip()
        return any(hex_code.startswith(p) for p in MILITARY_HEX_PREFIXES)

    # ------------------------------------------------------------------
    # Surveillance pattern detection
    # ------------------------------------------------------------------

    def detect_surveillance_patterns(
        self, aircraft_tracks: dict[str, list[dict]]
    ) -> list[dict]:
        """Identify orbiting / loitering patterns indicative of ISR missions.

        An ISR orbit is characterised by:
        * Repeated significant heading changes (> 60 deg) in a small area.
        * Low displacement from a central point over an extended period.

        Parameters
        ----------
        aircraft_tracks : dict[str, list[dict]]
            Mapping of hex code -> ordered list of position dicts.  Each
            position dict should have ``lat``, ``lon``, ``track`` (heading),
            ``timestamp``.

        Returns
        -------
        list[dict]
            Detected ISR orbit events.
        """
        patterns: list[dict] = []

        for hex_code, positions in aircraft_tracks.items():
            if len(positions) < ORBIT_MIN_TURNS * 2:
                continue

            # --- Heading-change detection ---
            headings = [
                p.get("track") for p in positions if p.get("track") is not None
            ]
            if len(headings) < ORBIT_MIN_TURNS * 2:
                continue

            turns = 0
            for i in range(1, len(headings)):
                delta = abs(headings[i] - headings[i - 1])
                if delta > 180:
                    delta = 360 - delta
                if delta > ORBIT_HEADING_DELTA:
                    turns += 1

            if turns < ORBIT_MIN_TURNS:
                continue

            # --- Spatial compactness check ---
            lats = [p["lat"] for p in positions if p.get("lat") is not None]
            lons = [p["lon"] for p in positions if p.get("lon") is not None]
            if not lats or not lons:
                continue

            mean_lat = sum(lats) / len(lats)
            mean_lon = sum(lons) / len(lons)

            max_disp_nm = max(
                self._haversine_nm(mean_lat, mean_lon, lat, lon)
                for lat, lon in zip(lats, lons)
            )

            if max_disp_nm > LOITER_MAX_DISPLACEMENT_NM:
                continue

            first_ts = positions[0].get("timestamp", "")
            last_ts = positions[-1].get("timestamp", "")

            pattern = {
                "event_type": "ISR_ORBIT_DETECTED",
                "source": "adsb_exchange",
                "hex": hex_code,
                "flight": positions[-1].get("flight", ""),
                "aircraft_type": positions[-1].get("aircraft_type", ""),
                "orbit_centre_lat": round(mean_lat, 4),
                "orbit_centre_lon": round(mean_lon, 4),
                "max_displacement_nm": round(max_disp_nm, 1),
                "heading_turns": turns,
                "track_points": len(positions),
                "first_seen": first_ts,
                "last_seen": last_ts,
                "confidence": min(0.95, 0.5 + turns * 0.05),
                "detected_at": datetime.now(timezone.utc).isoformat(),
            }
            patterns.append(pattern)

        if patterns:
            logger.info("Detected %d ISR orbit patterns", len(patterns))
        return patterns

    # ------------------------------------------------------------------
    # Interdiction correlation
    # ------------------------------------------------------------------

    def correlate_with_interdictions(
        self,
        tracks: dict[str, list[dict]],
        interdiction_events: list[dict],
        time_window_minutes: int = 120,
        distance_threshold_nm: float = 50.0,
    ) -> list[dict]:
        """Match ISR aircraft orbits with interdiction event times/locations.

        Parameters
        ----------
        tracks : dict[str, list[dict]]
            hex -> positions (same as detect_surveillance_patterns input).
        interdiction_events : list[dict]
            Each must have ``lat``, ``lon``, ``timestamp`` (ISO string).
        time_window_minutes : int
            Maximum time difference to consider a correlation.
        distance_threshold_nm : float
            Maximum distance between orbit centre and interdiction location.

        Returns
        -------
        list[dict]  Correlation events.
        """
        orbits = self.detect_surveillance_patterns(tracks)
        correlations: list[dict] = []

        for orbit in orbits:
            o_lat = orbit["orbit_centre_lat"]
            o_lon = orbit["orbit_centre_lon"]

            for event in interdiction_events:
                e_lat = event.get("lat")
                e_lon = event.get("lon")
                if e_lat is None or e_lon is None:
                    continue

                dist_nm = self._haversine_nm(o_lat, o_lon, e_lat, e_lon)
                if dist_nm > distance_threshold_nm:
                    continue

                # Time proximity check.
                try:
                    e_time = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
                    o_last = datetime.fromisoformat(orbit["last_seen"].replace("Z", "+00:00"))
                    delta_min = abs((e_time - o_last).total_seconds()) / 60
                except (KeyError, ValueError):
                    delta_min = float("inf")

                if delta_min > time_window_minutes:
                    continue

                correlations.append({
                    "event_type": "ISR_INTERDICTION_CORRELATION",
                    "source": "adsb_exchange",
                    "aircraft_hex": orbit["hex"],
                    "aircraft_flight": orbit.get("flight", ""),
                    "orbit_lat": o_lat,
                    "orbit_lon": o_lon,
                    "interdiction_lat": e_lat,
                    "interdiction_lon": e_lon,
                    "distance_nm": round(dist_nm, 1),
                    "time_delta_minutes": round(delta_min, 1),
                    "confidence": round(min(0.95, orbit["confidence"] + 0.1), 2),
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                    "interdiction_event": event,
                })

        if correlations:
            logger.info("Found %d ISR-interdiction correlations", len(correlations))
        return correlations

    # ------------------------------------------------------------------
    # Normalizer
    # ------------------------------------------------------------------

    def _normalize_aircraft(self, ac: dict, zone_name: str) -> dict:
        """Normalize raw ADS-B Exchange aircraft dict."""
        return {
            "event_id": f"adsb_{ac.get('hex', '')}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
            "source": "adsb_exchange",
            "event_type": "AIRCRAFT_POSITION",
            "hex": ac.get("hex", ""),
            "flight": (ac.get("flight") or "").strip(),
            "registration": ac.get("r", ""),
            "aircraft_type": ac.get("t", ""),
            "lat": ac.get("lat"),
            "lon": ac.get("lon"),
            "altitude_ft": ac.get("alt_baro"),
            "ground_speed_kts": ac.get("gs"),
            "track": ac.get("track"),
            "vertical_rate": ac.get("baro_rate"),
            "squawk": ac.get("squawk", ""),
            "is_military": self._is_military(ac),
            "category": ac.get("category", ""),
            "zone": zone_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Track history management
    # ------------------------------------------------------------------

    def _update_track_history(self, aircraft_list: list[dict]) -> None:
        """Append latest positions to in-memory track history."""
        for ac in aircraft_list:
            hex_code = ac.get("hex", "")
            if not hex_code:
                continue
            point = {
                "lat": ac.get("lat"),
                "lon": ac.get("lon"),
                "track": ac.get("track"),
                "altitude_ft": ac.get("alt_baro"),
                "ground_speed_kts": ac.get("gs"),
                "flight": (ac.get("flight") or "").strip(),
                "aircraft_type": ac.get("t", ""),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            history = self._track_history[hex_code]
            history.append(point)
            # Trim to max length.
            if len(history) > self._max_track_points:
                self._track_history[hex_code] = history[-self._max_track_points:]

    # ------------------------------------------------------------------
    # Publisher
    # ------------------------------------------------------------------

    def publish(self) -> int:
        """Full polling cycle: fetch all zones, publish positions, detect
        patterns, and publish pattern events."""
        producer = self._get_producer()
        total = 0

        for zone_name, (lat, lon, radius) in self.zones.items():
            aircraft = self.fetch_aircraft_near_point(lat, lon, radius)

            for ac in aircraft:
                normalized = self._normalize_aircraft(ac, zone_name)
                self._send("mda.adsb.aircraft", normalized)
                total += 1

            # Update track history for pattern detection.
            self._update_track_history(aircraft)

            logger.info("Zone %s: %d aircraft", zone_name, len(aircraft))
            # Brief delay to respect API rate limits.
            time.sleep(1)

        # --- Surveillance pattern detection on accumulated tracks ---
        # Only check military tracks.
        mil_tracks = {
            hex_code: pts
            for hex_code, pts in self._track_history.items()
            if any(
                hex_code.upper().startswith(p) for p in MILITARY_HEX_PREFIXES
            )
        }
        patterns = self.detect_surveillance_patterns(mil_tracks)
        for pat in patterns:
            self._send("mda.adsb.aircraft", pat)
            total += 1

        producer.flush()
        logger.info("ADS-B publish cycle complete: %d records", total)
        return total

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run_polling_loop(self, interval_minutes: int = 5) -> None:
        """Run the high-frequency polling loop for real-time SA.

        Parameters
        ----------
        interval_minutes : int
            Polling interval (default 5 min).
        """
        logger.info(
            "Starting ADS-B polling loop (interval=%dm, zones=%d)",
            interval_minutes, len(self.zones),
        )
        while True:
            cycle_start = time.monotonic()
            try:
                self.publish()
            except Exception as exc:
                logger.exception("ADS-B polling cycle failed: %s", exc)

            elapsed = time.monotonic() - cycle_start
            sleep_secs = max(0, interval_minutes * 60 - elapsed)
            logger.debug("ADS-B cycle in %.1fs, sleeping %.0fs", elapsed, sleep_secs)
            time.sleep(sleep_secs)

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            self._producer = None

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Great-circle distance in nautical miles."""
        R_NM = 3440.065
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(math.radians(lat1))
            * math.cos(math.radians(lat2))
            * math.sin(dlon / 2) ** 2
        )
        return R_NM * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    ingester = ADSBIngester()
    try:
        ingester.run_polling_loop()
    except KeyboardInterrupt:
        logger.info("Shutting down ADS-B ingester")
    finally:
        ingester.close()
