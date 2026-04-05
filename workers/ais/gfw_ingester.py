"""Global Fishing Watch API ingestion for encounter, gap, and loitering events.

Reference: https://globalfishingwatch.org/our-apis/
"""

import json
import logging
import os
from datetime import datetime

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.ais.gfw")

GFW_API_KEY = os.getenv("GFW_API_KEY")
GFW_BASE_URL = "https://gateway.api.globalfishingwatch.org/v3"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def _headers() -> dict:
    if not GFW_API_KEY:
        raise RuntimeError("GFW_API_KEY environment variable not set")
    return {"Authorization": f"Bearer {GFW_API_KEY}", "Content-Type": "application/json"}


def fetch_vessel_events(event_type: str, start_date: str, end_date: str) -> list:
    """Fetch vessel events from GFW Events API.

    event_types: PORT_VISIT | ENCOUNTER | LOITERING | FISHING | GAP
    """
    url = f"{GFW_BASE_URL}/events"
    params = {
        "datasets[0]": f"public-global-{event_type.lower()}-events:latest",
        "start-date": start_date,
        "end-date": end_date,
        "limit": 99999,
        "offset": 0,
    }

    all_events = []
    while True:
        resp = requests.get(url, headers=_headers(), params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        events = data.get("entries", [])
        all_events.extend(events)

        if len(events) < params["limit"]:
            break
        params["offset"] += params["limit"]

    logger.info("Fetched %d %s events from GFW", len(all_events), event_type)
    return all_events


def normalize_gfw_encounter(event: dict) -> dict:
    """Normalize a GFW encounter event to MDA schema."""
    vessels = event.get("vessels", [])
    vessel_a = vessels[0] if len(vessels) > 0 else {}
    vessel_b = vessels[1] if len(vessels) > 1 else {}

    return {
        "event_id": f"gfw_enc_{event['id']}",
        "source": "global_fishing_watch",
        "event_type": "ENCOUNTER",
        "start_time": event.get("start"),
        "end_time": event.get("end"),
        "lat": event.get("position", {}).get("lat"),
        "lon": event.get("position", {}).get("lon"),
        "vessel_a_mmsi": vessel_a.get("ssvid"),
        "vessel_a_imo": vessel_a.get("imo"),
        "vessel_a_name": vessel_a.get("shipName"),
        "vessel_b_mmsi": vessel_b.get("ssvid"),
        "vessel_b_imo": vessel_b.get("imo"),
        "vessel_b_name": vessel_b.get("shipName"),
        "confidence": 0.85,
        "raw": event,
    }


def normalize_gfw_gap(event: dict) -> dict:
    """Normalize a GFW AIS gap event to MDA schema."""
    vessel = event.get("vessels", [{}])[0]
    return {
        "event_id": f"gfw_gap_{event['id']}",
        "source": "global_fishing_watch",
        "event_type": "AIS_GAP",
        "start_time": event.get("start"),
        "end_time": event.get("end"),
        "lat": event.get("position", {}).get("lat"),
        "lon": event.get("position", {}).get("lon"),
        "vessel_mmsi": vessel.get("ssvid"),
        "vessel_imo": vessel.get("imo"),
        "vessel_name": vessel.get("shipName"),
        "confidence": 0.80,
        "raw": event,
    }


def ingest_gfw_events(event_type: str, start_date: str, end_date: str):
    """Fetch GFW events and publish to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    events = fetch_vessel_events(event_type, start_date, end_date)

    normalizer = {
        "ENCOUNTER": normalize_gfw_encounter,
        "GAP": normalize_gfw_gap,
    }.get(event_type.upper())

    topic = {
        "ENCOUNTER": "mda.ais.encounters.detected",
        "GAP": "mda.ais.gaps.detected",
    }.get(event_type.upper(), "mda.events.gfw.raw")

    for event in events:
        try:
            if normalizer:
                normalized = normalizer(event)
            else:
                normalized = {"source": "global_fishing_watch", "event_type": event_type, "raw": event}
            producer.send(topic, normalized)
        except Exception as e:
            producer.send("mda.dlq", {"source": "gfw", "error": str(e), "raw": str(event)[:500]})

    producer.flush()
    producer.close()
    logger.info("Published %d GFW %s events to %s", len(events), event_type, topic)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    # Default: fetch last 30 days of encounters and gaps
    from datetime import timedelta

    end = datetime.utcnow().strftime("%Y-%m-%d")
    start = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")

    ingest_gfw_events("ENCOUNTER", start, end)
    ingest_gfw_events("GAP", start, end)
