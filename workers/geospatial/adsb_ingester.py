"""ADS-B Exchange aircraft tracking ingester.

Tracks military/government aircraft near interdiction zones.
Source: https://www.adsbexchange.com/data/ (free tier via RapidAPI)
"""

import json
import logging
import math
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.geospatial.adsb")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
ADSB_API_KEY = os.getenv("ADSB_API_KEY", "")
ADSB_API_URL = os.getenv("ADSB_API_URL", "https://adsbexchange-com1.p.rapidapi.com/v2")

# Military/government hex code prefixes (partial list)
MILITARY_HEX_PREFIXES = [
    "AE",   # US military
    "AF",   # US military
    "43C",  # UK military
    "3F",   # Germany military
]

# MDA monitoring zones (lat, lon, radius_nm)
MDA_ZONES = {
    "eastern_pacific": (10.0, -90.0, 250),
    "caribbean": (17.0, -75.0, 200),
    "gulf_of_mexico": (25.0, -90.0, 200),
    "us_mx_border": (31.0, -110.0, 150),
}


class ADSBIngester:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )
        self.headers = {
            "X-RapidAPI-Key": ADSB_API_KEY,
            "X-RapidAPI-Host": "adsbexchange-com1.p.rapidapi.com",
        }

    def fetch_aircraft_near_point(self, lat: float, lon: float, radius_nm: int = 250) -> list[dict]:
        """Fetch aircraft within radius of a point."""
        if not ADSB_API_KEY:
            logger.warning("ADSB_API_KEY not set")
            return []

        url = f"{ADSB_API_URL}/lat/{lat}/lon/{lon}/dist/{radius_nm}/"
        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            return data.get("ac", [])
        except Exception as e:
            logger.error("ADS-B API error: %s", e)
            return []

    def is_military(self, aircraft: dict) -> bool:
        """Check if aircraft hex code suggests military."""
        hex_code = aircraft.get("hex", "").upper()
        return any(hex_code.startswith(prefix) for prefix in MILITARY_HEX_PREFIXES)

    def detect_orbit_pattern(self, positions: list[dict], min_turns: int = 3) -> bool:
        """Detect orbiting/loitering pattern (ISR indication)."""
        if len(positions) < min_turns * 2:
            return False
        headings = [p.get("track", 0) for p in positions if p.get("track") is not None]
        if len(headings) < min_turns * 2:
            return False
        # Count significant heading changes (>60 degrees)
        turns = 0
        for i in range(1, len(headings)):
            delta = abs(headings[i] - headings[i - 1])
            if delta > 180:
                delta = 360 - delta
            if delta > 60:
                turns += 1
        return turns >= min_turns

    def normalize_aircraft(self, ac: dict, zone_name: str) -> dict:
        return {
            "hex": ac.get("hex", ""),
            "flight": (ac.get("flight") or "").strip(),
            "registration": ac.get("r", ""),
            "aircraft_type": ac.get("t", ""),
            "lat": ac.get("lat"),
            "lon": ac.get("lon"),
            "altitude_ft": ac.get("alt_baro"),
            "ground_speed_kts": ac.get("gs"),
            "track": ac.get("track"),
            "squawk": ac.get("squawk", ""),
            "is_military": self.is_military(ac),
            "category": ac.get("category", ""),
            "zone": zone_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "adsb_exchange",
        }

    def ingest_mda_zones(self):
        """Fetch aircraft from all MDA monitoring zones."""
        total = 0
        for zone_name, (lat, lon, radius) in MDA_ZONES.items():
            aircraft = self.fetch_aircraft_near_point(lat, lon, radius)
            for ac in aircraft:
                normalized = self.normalize_aircraft(ac, zone_name)
                self.producer.send("mda.adsb.aircraft", normalized)
                total += 1
            logger.info("Zone %s: %d aircraft", zone_name, len(aircraft))
            time.sleep(1)

        self.producer.flush()
        logger.info("Total aircraft published: %d", total)

    def run_polling_loop(self, interval_minutes: int = 5):
        logger.info("ADS-B poller started (interval=%dm)", interval_minutes)
        while True:
            try:
                self.ingest_mda_zones()
            except Exception as e:
                logger.error("ADS-B polling error: %s", e)
            time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ADSBIngester().run_polling_loop()
