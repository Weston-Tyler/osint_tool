"""AISHub real-time AIS stream consumer.

Connects to AISHub.net community feed and forwards AIS messages to Kafka.
Requires: AISHub account (free with data sharing). Register at https://www.aishub.net

Supports both the JSON HTTP API and raw TCP NMEA stream.
"""

import json
import logging
import os
import socket
import time
from datetime import datetime

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.ais.aishub")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
AISHUB_USERNAME = os.getenv("AISHUB_USERNAME", "")

# AISHub HTTP API endpoint (alternative to TCP stream)
AISHUB_API_URL = "https://data.aishub.net/ws.php"


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        compression_type="gzip",
    )


def decode_ais_type(msg_type: int) -> str:
    """Map AIS message type to vessel navigation status description."""
    nav_map = {
        0: "UNDERWAY_ENGINE",
        1: "AT_ANCHOR",
        2: "NOT_UNDER_COMMAND",
        3: "RESTRICTED_MANEUVERABILITY",
        4: "CONSTRAINED_DRAFT",
        5: "MOORED",
        6: "AGROUND",
        7: "FISHING",
        8: "UNDERWAY_SAIL",
        15: "NOT_DEFINED",
    }
    return nav_map.get(msg_type, "NOT_DEFINED")


def normalize_aishub_record(record: dict) -> dict:
    """Normalize an AISHub JSON record to MDA schema."""
    mmsi = str(record.get("MMSI", "")).strip()
    if not mmsi or len(mmsi) != 9:
        return {}

    lat = record.get("LATITUDE")
    lon = record.get("LONGITUDE")
    if lat is None or lon is None:
        return {}

    # AISHub uses scaled integers for some fields
    lat = float(lat) / 600000 if abs(float(lat)) > 180 else float(lat)
    lon = float(lon) / 600000 if abs(float(lon)) > 360 else float(lon)

    return {
        "source": "aishub",
        "mmsi": mmsi,
        "imo": str(record.get("IMO", "")) or "",
        "timestamp": record.get("TIME", datetime.utcnow().isoformat()),
        "lat": lat,
        "lon": lon,
        "speed_kts": float(record.get("SOG", 0) or 0) / 10.0,
        "course": float(record.get("COG", 0) or 0) / 10.0,
        "heading": int(record.get("HEADING", 511) or 511),
        "nav_status": int(record.get("NAVSTAT", 15) or 15),
        "vessel_name": str(record.get("NAME", "") or "").strip(),
        "vessel_type": int(record.get("TYPE", 0) or 0),
        "length_m": float(record.get("A", 0) or 0) + float(record.get("B", 0) or 0),
        "draft_m": float(record.get("DRAUGHT", 0) or 0) / 10.0,
    }


def poll_aishub_api(producer: KafkaProducer, bbox: dict | None = None):
    """Fetch latest AIS data from AISHub HTTP API.

    Args:
        bbox: Optional bounding box dict {minlat, maxlat, minlon, maxlon}
    """
    if not AISHUB_USERNAME:
        logger.error("AISHUB_USERNAME not set")
        return

    params = {
        "username": AISHUB_USERNAME,
        "format": "1",  # JSON
        "output": "json",
        "compress": "0",
    }

    if bbox:
        params.update({
            "latmin": bbox["minlat"],
            "latmax": bbox["maxlat"],
            "lonmin": bbox["minlon"],
            "lonmax": bbox["maxlon"],
        })

    try:
        resp = requests.get(AISHUB_API_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.error("AISHub API error: %s", e)
        return
    except json.JSONDecodeError:
        logger.error("AISHub returned non-JSON response")
        return

    if not isinstance(data, list):
        data = data.get("data", []) if isinstance(data, dict) else []

    count = 0
    for record in data:
        normalized = normalize_aishub_record(record)
        if normalized:
            producer.send("mda.ais.positions.raw", normalized)
            count += 1

    producer.flush()
    logger.info("Published %d AIS positions from AISHub API", count)


def run_polling_loop(interval_seconds: int = 60):
    """Continuously poll AISHub API at the specified interval.

    AISHub rate limits: ~1 request per minute for free tier.
    """
    producer = create_producer()
    logger.info("AISHub polling started (interval=%ds)", interval_seconds)

    # Define bounding boxes for MDA-relevant regions
    bboxes = [
        # JIATF-South Eastern Pacific
        {"name": "E_PACIFIC", "minlat": -5, "maxlat": 20, "minlon": -105, "maxlon": -75},
        # Caribbean
        {"name": "CARIBBEAN", "minlat": 8, "maxlat": 25, "minlon": -90, "maxlon": -60},
        # Gulf of Mexico
        {"name": "GULF_MEX", "minlat": 18, "maxlat": 31, "minlon": -100, "maxlon": -80},
    ]

    bbox_idx = 0
    while True:
        bbox = bboxes[bbox_idx % len(bboxes)]
        logger.info("Polling AISHub for region: %s", bbox["name"])
        poll_aishub_api(producer, bbox)

        bbox_idx += 1
        time.sleep(interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    run_polling_loop()
