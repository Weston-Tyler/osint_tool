"""IOM Displacement Tracking Matrix ingester.

Source: https://dtm.iom.int/data-and-analysis
Tracks population displacement flows in real-time.
"""

import json
import logging
import os
import time

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.humanitarian.iom_dtm")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DTM_API_URL = "https://dtm.iom.int/api/v2"


def fetch_dtm_data(country_iso3: str | None = None, operation: str = "idp-admin0") -> list[dict]:
    """Fetch IOM DTM displacement data."""
    url = f"{DTM_API_URL}/{operation}"
    params = {}
    if country_iso3:
        params["CountryName"] = country_iso3

    try:
        resp = requests.get(url, params=params, timeout=30,
                           headers={"Accept": "application/json"})
        resp.raise_for_status()
        return resp.json() if isinstance(resp.json(), list) else resp.json().get("results", [])
    except Exception as e:
        logger.error("IOM DTM API error: %s", e)
        return []


def normalize_dtm_record(raw: dict) -> dict:
    return {
        "event_id": f"iom_dtm_{raw.get('id', '')}",
        "event_type": "DISPLACEMENT_TRACKING",
        "domain": "humanitarian",
        "country": raw.get("admin0Name", raw.get("country", "")),
        "admin1": raw.get("admin1Name", ""),
        "admin2": raw.get("admin2Name", ""),
        "lat": raw.get("latitude"),
        "lon": raw.get("longitude"),
        "idp_count": raw.get("numPresentIdpInd", 0),
        "reporting_date": raw.get("reportingDate", ""),
        "operation": raw.get("operation", ""),
        "source": "iom_dtm",
    }


def ingest_dtm():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    monitored = ["MEX", "COL", "VEN", "HTI", "SDN", "SOM", "YEM", "ETH", "AFG", "MMR", "HND", "GTM", "SLV"]
    total = 0
    for country in monitored:
        records = fetch_dtm_data(country)
        for raw in records:
            normalized = normalize_dtm_record(raw)
            producer.send("mda.humanitarian.displacement", normalized)
            total += 1
        time.sleep(1)

    producer.flush()
    producer.close()
    logger.info("Ingested %d IOM DTM records", total)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_dtm()
