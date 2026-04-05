"""GDELT 2.0 event ingestion for MDA-relevant events.

Fetches the latest 15-minute GDELT event files and filters for maritime,
drug trafficking, and law enforcement events in MDA-relevant regions.

Source: https://analysis.gdeltproject.org/module-event-exporter.html
"""

import csv
import io
import json
import logging
import os
import zipfile
from datetime import datetime

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.gdelt")

GDELT_MASTER_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# GDELT 2.0 event column header (56 columns)
GDELT_EVENTS_HEADER = [
    "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources", "NumArticles",
    "AvgTone", "Actor1Geo_Type", "Actor1Geo_FullName", "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code", "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code", "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL",
]

# CAMEO event codes relevant to MDA
MDA_RELEVANT_CAMEO_CODES = {
    "17": "COERCE",
    "171": "SEIZE_POSSESSION_OR_PROPERTY",
    "172": "ARREST_DETAIN",
    "173": "EXPEL_OR_DEPORT",
    "19": "FIGHT",
    "190": "FIGHT_GENERAL",
    "195": "FIGHT_WITH_WEAPONS",
    "196": "IMPOSE_BLOCKADE",
}

# Countries of interest for MDA
MDA_COUNTRIES = {"US", "MX", "CO", "VE", "EC", "PA", "GT", "HN", "CU", "JM", "PE", "BO", "BR", "NI", "CR", "BZ"}


def is_mda_relevant(row: dict) -> bool:
    """Filter GDELT events to MDA-relevant subset."""
    if row.get("EventCode") in MDA_RELEVANT_CAMEO_CODES:
        return True

    action_country = row.get("ActionGeo_CountryCode", "")
    if action_country in MDA_COUNTRIES:
        return True

    if row.get("Actor1CountryCode") in MDA_COUNTRIES or row.get("Actor2CountryCode") in MDA_COUNTRIES:
        return True

    return False


def normalize_gdelt_event(row: dict) -> dict:
    """Normalize GDELT row to MDA event schema."""
    return {
        "event_id": f"gdelt_{row.get('GlobalEventID', '')}",
        "source": "gdelt",
        "event_date": row.get("Day", ""),
        "event_code": row.get("EventCode", ""),
        "event_code_desc": MDA_RELEVANT_CAMEO_CODES.get(row.get("EventCode", ""), "OTHER"),
        "goldstein_scale": float(row.get("GoldsteinScale") or 0),
        "avg_tone": float(row.get("AvgTone") or 0),
        "actor1_name": row.get("Actor1Name", ""),
        "actor1_country": row.get("Actor1CountryCode", ""),
        "actor2_name": row.get("Actor2Name", ""),
        "actor2_country": row.get("Actor2CountryCode", ""),
        "action_geo_country": row.get("ActionGeo_CountryCode", ""),
        "action_geo_fullname": row.get("ActionGeo_FullName", ""),
        "action_lat": float(row.get("ActionGeo_Lat") or 0),
        "action_lon": float(row.get("ActionGeo_Long") or 0),
        "num_mentions": int(row.get("NumMentions") or 0),
        "source_url": row.get("SOURCEURL", ""),
    }


def fetch_latest_gdelt_events_url() -> str | None:
    """Get URL of latest GDELT 2.0 events export file."""
    resp = requests.get(GDELT_MASTER_URL, timeout=30)
    resp.raise_for_status()

    for line in reversed(resp.text.strip().split("\n")):
        if ".export.CSV.zip" in line:
            parts = line.split()
            return parts[-1].strip()
    return None


def ingest_latest():
    """Fetch latest 15-minute GDELT events and publish MDA-relevant ones to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    url = fetch_latest_gdelt_events_url()
    if not url:
        logger.error("Could not find latest GDELT events file")
        return

    logger.info("Downloading GDELT events: %s", url)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    count = 0
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open(zf.namelist()[0]) as f:
            text = f.read().decode("utf-8", errors="replace")
            reader = csv.DictReader(
                io.StringIO(text),
                fieldnames=GDELT_EVENTS_HEADER,
                delimiter="\t",
            )

            for row in reader:
                if is_mda_relevant(row):
                    event = normalize_gdelt_event(row)
                    producer.send("mda.events.gdelt.raw", event)
                    count += 1

    producer.flush()
    producer.close()
    logger.info("Published %d MDA-relevant GDELT events", count)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    ingest_latest()
