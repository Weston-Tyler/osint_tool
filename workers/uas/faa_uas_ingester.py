"""FAA UAS sighting reports ingestion.

Processes FAA UAS sighting reports (FOIA releases via ERAU database).
Source: https://commons.erau.edu/db-aeronautical-science/6/
"""

import json
import logging
import os

import pandas as pd
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.uas.faa")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# US-Mexico border states and maritime-relevant keywords
BORDER_STATES = {"CA", "AZ", "NM", "TX"}
MARITIME_KEYWORDS = ["ocean", "coast", "harbor", "port", "bay", "gulf", "ship", "vessel", "maritime", "sea"]


def classify_faa_uas(aircraft_type: str) -> str:
    """Map FAA aircraft type description to MDA classification."""
    if not aircraft_type:
        return "UNKNOWN"
    t = aircraft_type.lower()
    if any(k in t for k in ("dji", "phantom", "mavic", "inspire")):
        return "SMALL_COMMERCIAL"
    if any(k in t for k in ("fixed", "wing")):
        return "FIXED_WING"
    if any(k in t for k in ("multi", "rotor", "octo", "quad", "hex")):
        return "MULTIROTOR"
    return "UNKNOWN"


def normalize_faa_sighting(row: pd.Series) -> dict:
    """Normalize FAA UAS sighting to MDA schema."""
    alt_ft = row.get("altitude", 0) or 0
    return {
        "event_id": f"faa_uas_{row.name}",
        "source": "faa_uas_sightings",
        "detection_timestamp": str(row.get("event_datetime", "")),
        "detection_lat": float(row.get("latitude", 0) or 0),
        "detection_lon": float(row.get("longitude", 0) or 0),
        "detection_alt_m": float(alt_ft) * 0.3048,
        "sensor_type": "VISUAL",
        "detection_method": "VISUAL_ID",
        "detection_confidence": 0.70,
        "uas_classification": classify_faa_uas(str(row.get("aircraft_type", ""))),
        "location_description": f"{row.get('city', '')}, {row.get('state', '')}",
        "description": str(row.get("pilot_description", "") or ""),
    }


def ingest_faa_sightings(filepath: str):
    """Load FAA UAS sighting CSV and publish to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    logger.info("Loading FAA UAS sightings from %s", filepath)
    df = pd.read_csv(filepath, encoding="latin1")

    # Normalize column names
    df.columns = [c.lower().replace(" ", "_").replace("/", "_") for c in df.columns]

    # Parse timestamps
    if "date" in df.columns and "time" in df.columns:
        df["event_datetime"] = pd.to_datetime(
            df["date"].astype(str) + " " + df["time"].astype(str),
            errors="coerce",
        )

    # Filter to border/maritime relevant sightings
    state_mask = df.get("state", pd.Series(dtype=str)).isin(BORDER_STATES)
    desc_col = df.get("pilot_description", pd.Series(dtype=str)).fillna("")
    maritime_mask = desc_col.str.lower().str.contains("|".join(MARITIME_KEYWORDS), na=False)
    df_relevant = df[state_mask | maritime_mask].copy()

    logger.info("Found %d relevant sightings out of %d total", len(df_relevant), len(df))

    count = 0
    for idx, row in df_relevant.iterrows():
        try:
            event = normalize_faa_sighting(row)
            producer.send("mda.uas.detections.raw", event)
            count += 1
        except Exception as e:
            producer.send("mda.dlq", {"source": "faa_uas", "error": str(e), "index": str(idx)})

    producer.flush()
    producer.close()
    logger.info("Published %d FAA UAS sightings to Kafka", count)


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    if len(sys.argv) < 2:
        logger.error("Usage: python faa_uas_ingester.py <csv_filepath>")
        sys.exit(1)
    ingest_faa_sightings(sys.argv[1])
