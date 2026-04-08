"""GDELT 2.0 event + mentions ingestion via bulk file downloads.

Fetches the latest 15-minute GDELT event AND mentions files from the
public bulk-download mirror and filters for maritime, drug trafficking,
and law enforcement activity in MDA-relevant regions.

Bulk downloads bypass the rate-limited search APIs (DOC/GEO/TV) entirely,
so this ingester is reliable from any IP.

Source: https://analysis.gdeltproject.org/module-event-exporter.html
        http://data.gdeltproject.org/gdeltv2/masterfilelist.txt
"""

import csv
import io
import json
import logging
import os
import time
import zipfile
from datetime import datetime

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.gdelt")

GDELT_MASTER_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_EVENTS = "mda.events.gdelt.raw"
TOPIC_MENTIONS = "mda.events.gdelt.mentions"

# GDELT 2.0 mentions column header (16 columns)
GDELT_MENTIONS_HEADER = [
    "GlobalEventID", "EventTimeDate", "MentionTimeDate", "MentionType",
    "MentionSourceName", "MentionIdentifier", "SentenceID", "Actor1CharOffset",
    "Actor2CharOffset", "ActionCharOffset", "InRawText", "Confidence",
    "MentionDocLen", "MentionDocTone", "MentionDocTranslationInfo", "Extras",
]

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
    "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code", "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code", "ActionGeo_ADM2Code", "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",
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


def _safe_float(v) -> float:
    try:
        return float(v) if v not in (None, "") else 0.0
    except (ValueError, TypeError):
        return 0.0


def _safe_int(v) -> int:
    try:
        return int(v) if v not in (None, "") else 0
    except (ValueError, TypeError):
        return 0


def normalize_gdelt_event(row: dict) -> dict:
    """Normalize GDELT row to MDA event schema."""
    return {
        "event_id": f"gdelt_{row.get('GlobalEventID', '')}",
        "source": "gdelt",
        "event_date": row.get("Day", ""),
        "event_code": row.get("EventCode", ""),
        "event_code_desc": MDA_RELEVANT_CAMEO_CODES.get(row.get("EventCode", ""), "OTHER"),
        "goldstein_scale": _safe_float(row.get("GoldsteinScale")),
        "avg_tone": _safe_float(row.get("AvgTone")),
        "actor1_name": row.get("Actor1Name", ""),
        "actor1_country": row.get("Actor1CountryCode", ""),
        "actor2_name": row.get("Actor2Name", ""),
        "actor2_country": row.get("Actor2CountryCode", ""),
        "action_geo_country": row.get("ActionGeo_CountryCode", ""),
        "action_geo_fullname": row.get("ActionGeo_FullName", ""),
        "action_lat": _safe_float(row.get("ActionGeo_Lat")),
        "action_lon": _safe_float(row.get("ActionGeo_Long")),
        "num_mentions": _safe_int(row.get("NumMentions")),
        "source_url": row.get("SOURCEURL", ""),
    }


def fetch_latest_url(suffix: str) -> str | None:
    """Get URL of the most recent file matching a suffix in the master list.

    suffix examples: ".export.CSV.zip", ".mentions.CSV.zip", ".gkg.csv.zip"
    """
    resp = requests.get(GDELT_MASTER_URL, timeout=30)
    resp.raise_for_status()

    for line in reversed(resp.text.strip().split("\n")):
        if suffix in line:
            parts = line.split()
            return parts[-1].strip()
    return None


def fetch_latest_gdelt_events_url() -> str | None:
    """Compatibility shim for older callers."""
    return fetch_latest_url(".export.CSV.zip")


def ingest_events(producer: KafkaProducer) -> int:
    """Fetch + publish the latest 15-minute GDELT events file. Returns count."""
    url = fetch_latest_url(".export.CSV.zip")
    if not url:
        logger.error("Could not find latest GDELT events file")
        return 0

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
                    producer.send(TOPIC_EVENTS, event)
                    count += 1
    logger.info("Published %d MDA-relevant GDELT events", count)
    return count


def ingest_mentions(producer: KafkaProducer) -> int:
    """Fetch + publish the latest 15-minute GDELT mentions file.

    Mentions records every news article that references a given event,
    so they're keyed by GlobalEventID and provide the article-level
    discovery layer for events. We publish them all (no MDA filter)
    because the join key is the event ID, and downstream consumers
    can filter against the events topic.
    """
    url = fetch_latest_url(".mentions.CSV.zip")
    if not url:
        logger.error("Could not find latest GDELT mentions file")
        return 0

    logger.info("Downloading GDELT mentions: %s", url)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    count = 0
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open(zf.namelist()[0]) as f:
            text = f.read().decode("utf-8", errors="replace")
            reader = csv.DictReader(
                io.StringIO(text),
                fieldnames=GDELT_MENTIONS_HEADER,
                delimiter="\t",
            )
            for row in reader:
                mention = {
                    "event_id": f"gdelt_{row.get('GlobalEventID', '')}",
                    "source": "gdelt_mentions",
                    "event_time": row.get("EventTimeDate", ""),
                    "mention_time": row.get("MentionTimeDate", ""),
                    "mention_type": row.get("MentionType", ""),
                    "mention_source": row.get("MentionSourceName", ""),
                    "mention_url": row.get("MentionIdentifier", ""),
                    "confidence": _safe_float(row.get("Confidence")),
                    "tone": _safe_float(row.get("MentionDocTone")),
                    "doc_length": _safe_int(row.get("MentionDocLen")),
                    "ingest_time": datetime.utcnow().isoformat(),
                }
                producer.send(TOPIC_MENTIONS, mention)
                count += 1
    logger.info("Published %d GDELT mention records", count)
    return count


def ingest_latest():
    """Fetch latest events + mentions in one pass and publish to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    try:
        events = ingest_events(producer)
        mentions = ingest_mentions(producer)
        logger.info(
            "Iteration complete: %d events + %d mentions", events, mentions
        )
    finally:
        producer.flush()
        producer.close()


def main():
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    ap = argparse.ArgumentParser(
        description="GDELT 2.0 events + mentions bulk ingester"
    )
    ap.add_argument(
        "--loop",
        type=int,
        default=0,
        metavar="SECONDS",
        help="Re-fetch every N seconds (default: one-shot)",
    )
    args = ap.parse_args()

    iteration = 0
    while True:
        iteration += 1
        logger.info("GDELT events+mentions iteration %d starting", iteration)
        try:
            ingest_latest()
        except Exception:
            logger.exception("Iteration %d failed", iteration)
        if args.loop <= 0:
            break
        logger.info("Sleeping %ds before next iteration", args.loop)
        time.sleep(args.loop)


if __name__ == "__main__":
    main()
