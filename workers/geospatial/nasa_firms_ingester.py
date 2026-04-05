"""NASA FIRMS active fire data ingester.

Source: https://firms.modaps.eosdis.nasa.gov/api/
Detects: conflict (burning infrastructure), deforestation, crop burning.
Free API key from NASA Earthdata.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.geospatial.firms")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
NASA_FIRMS_KEY = os.getenv("NASA_FIRMS_KEY", "")
FIRMS_API_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

MDA_COUNTRIES = ["MEX", "COL", "VEN", "GTM", "HND", "SLV", "NIC", "HTI",
                  "SDN", "SOM", "YEM", "ETH", "AFG", "MMR"]


def fetch_fires(country_iso3: str, days: int = 2, source: str = "VIIRS_SNPP_NRT") -> list[dict]:
    """Fetch active fire data from NASA FIRMS."""
    url = f"{FIRMS_API_URL}/{NASA_FIRMS_KEY}/{source}/{country_iso3}/{days}"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    fires = []
    lines = resp.text.strip().split("\n")
    if len(lines) < 2:
        return fires

    headers = lines[0].split(",")
    for line in lines[1:]:
        vals = line.split(",")
        if len(vals) < len(headers):
            continue
        row = dict(zip(headers, vals))
        fires.append({
            "event_id": f"firms_{row.get('bright_ti4', '')}_{row.get('acq_date', '')}_{row.get('latitude', '')}",
            "event_type": "ACTIVE_FIRE",
            "domain": "environmental",
            "detection_timestamp": f"{row.get('acq_date', '')}T{row.get('acq_time', '0000')[:2]}:{row.get('acq_time', '0000')[2:]}:00Z",
            "lat": float(row.get("latitude", 0)),
            "lon": float(row.get("longitude", 0)),
            "brightness": float(row.get("bright_ti4", 0) or 0),
            "frp": float(row.get("frp", 0) or 0),  # Fire Radiative Power (MW)
            "confidence": row.get("confidence", "nominal"),
            "satellite": source,
            "country_iso3": country_iso3,
        })
    return fires


def ingest_all_countries():
    if not NASA_FIRMS_KEY:
        logger.error("NASA_FIRMS_KEY not set")
        return

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    total = 0
    for country in MDA_COUNTRIES:
        try:
            fires = fetch_fires(country)
            for fire in fires:
                producer.send("mda.geospatial.fires", fire)
            total += len(fires)
            logger.info("Fetched %d fires for %s", len(fires), country)
            time.sleep(1)  # Rate limit
        except Exception as e:
            logger.error("FIRMS error for %s: %s", country, e)

    producer.flush()
    producer.close()
    logger.info("Total fires ingested: %d", total)


def run_polling_loop(interval_hours: int = 6):
    logger.info("NASA FIRMS poller started (interval=%dh)", interval_hours)
    while True:
        try:
            ingest_all_countries()
        except Exception as e:
            logger.error("FIRMS polling error: %s", e)
        time.sleep(interval_hours * 3600)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_polling_loop()
