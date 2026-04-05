"""GDELT 2.0 Global Knowledge Graph (GKG) ingestion for MDA intelligence.

Fetches the latest 15-minute GKG files and extracts entities, themes,
locations, and tone data relevant to Maritime Domain Awareness.

Source: http://data.gdeltproject.org/gdeltv2/masterfilelist.txt
GKG Codebook: http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf
"""

import csv
import io
import json
import logging
import os
import time
import zipfile
from datetime import datetime, timezone
from typing import Any

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.gdelt.gkg")

GDELT_MASTER_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
POLL_INTERVAL_SECONDS = int(os.getenv("GKG_POLL_INTERVAL", "900"))  # 15 minutes

TOPIC_GKG_RAW = "mda.gdelt.gkg.raw"
TOPIC_GKG_ENTITIES = "mda.gdelt.gkg.entities"

# GKG 2.0 column names (27 columns, tab-separated)
GKG_HEADER = [
    "GKGRECORDID",
    "DATE",
    "SourceCollectionIdentifier",
    "SourceCommonName",
    "DocumentIdentifier",
    "Counts",
    "V2Counts",
    "Themes",
    "V2Themes",
    "Locations",
    "V2Locations",
    "Persons",
    "V2Persons",
    "Organizations",
    "V2Organizations",
    "V2Tone",
    "Dates",
    "GCAM",
    "SharingImage",
    "RelatedImages",
    "SocialImageEmbeds",
    "SocialVideoEmbeds",
    "Quotations",
    "AllNames",
    "Amounts",
    "TranslationInfo",
    "Extras",
]

# MDA-relevant GKG theme codes. Entries ending with '*' are treated as prefixes.
MDA_THEME_CODES = [
    "TAX_FAMINE*",
    "FOOD_SECURITY*",
    "DRUG_TRADE",
    "DRUG_TRAFFICKING",
    "MARITIME_INCIDENT",
    "MARITIME_PIRACY",
    "CARTEL",
    "SMUGGLING",
    "CRIME_ILLICIT",
    "MILITARY_DRONE",
    "DISPLACEMENT",
    "REFUGEES",
    "SANCTIONS",
    "BLOCKADE",
    "OIL_SPILL",
    "PROTEST",
    "RIOT",
    "WMD",
    "HUMAN_TRAFFICKING",
    "KIDNAPPING",
    "EXTORTION",
    "ASSASSINATION",
    "POLITICAL_TURMOIL",
    "NATURAL_DISASTER",
]

# Pre-compile into exact and prefix lists for fast matching
_EXACT_THEMES: set[str] = set()
_PREFIX_THEMES: list[str] = []
for _code in MDA_THEME_CODES:
    if _code.endswith("*"):
        _PREFIX_THEMES.append(_code[:-1])
    else:
        _EXACT_THEMES.add(_code)


def _theme_is_mda_relevant(theme: str) -> bool:
    """Check whether a single theme code is MDA-relevant."""
    if theme in _EXACT_THEMES:
        return True
    for prefix in _PREFIX_THEMES:
        if theme.startswith(prefix):
            return True
    return False


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_v2_themes(raw: str) -> list[str]:
    """Parse semicolon-separated V2Themes field.

    Each entry may include an offset like ``THEME,offset``. We extract just
    the theme code.
    """
    if not raw:
        return []
    themes: list[str] = []
    for entry in raw.split(";"):
        entry = entry.strip()
        if not entry:
            continue
        # V2Themes entries are THEME_CODE,charOffset  -- take only the code
        code = entry.split(",")[0].strip()
        if code:
            themes.append(code)
    return themes


def parse_v2_persons(raw: str) -> list[str]:
    """Parse semicolon-separated V2Persons field."""
    if not raw:
        return []
    persons: list[str] = []
    for entry in raw.split(";"):
        name = entry.split(",")[0].strip()
        if name:
            persons.append(name)
    return persons


def parse_v2_organizations(raw: str) -> list[str]:
    """Parse semicolon-separated V2Organizations field."""
    if not raw:
        return []
    orgs: list[str] = []
    for entry in raw.split(";"):
        name = entry.split(",")[0].strip()
        if name:
            orgs.append(name)
    return orgs


def parse_v2_locations(raw: str) -> list[dict[str, Any]]:
    """Parse semicolon-separated V2Locations field.

    Each location is ``#``-delimited:
    LocationType#LocationName#CountryCode#ADM1Code#Latitude#Longitude#FeatureID
    """
    if not raw:
        return []
    locations: list[dict[str, Any]] = []
    for entry in raw.split(";"):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split("#")
        if len(parts) < 7:
            continue
        lat = _safe_float(parts[4])
        lon = _safe_float(parts[5])
        locations.append({
            "location_type": parts[0],
            "location_name": parts[1],
            "country_code": parts[2],
            "adm1_code": parts[3],
            "latitude": lat,
            "longitude": lon,
            "feature_id": parts[6],
        })
    return locations


def parse_v2_tone(raw: str) -> dict[str, float]:
    """Parse comma-separated V2Tone field (7 values)."""
    defaults = {
        "tone": 0.0,
        "positive_score": 0.0,
        "negative_score": 0.0,
        "polarity": 0.0,
        "activity_reference_density": 0.0,
        "self_group_reference_density": 0.0,
        "word_count": 0.0,
    }
    if not raw:
        return defaults
    parts = raw.split(",")
    keys = list(defaults.keys())
    for i, key in enumerate(keys):
        if i < len(parts):
            defaults[key] = _safe_float(parts[i])
    return defaults


def _safe_float(val: str) -> float:
    """Convert string to float, returning 0.0 on failure."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Record-level processing
# ---------------------------------------------------------------------------

def is_mda_relevant(themes: list[str]) -> bool:
    """Return True if any theme in the record is MDA-relevant."""
    return any(_theme_is_mda_relevant(t) for t in themes)


def parse_gkg_record(row: dict[str, str]) -> dict[str, Any]:
    """Parse a raw GKG tab-separated row dict into a structured record."""
    themes = parse_v2_themes(row.get("V2Themes", ""))
    persons = parse_v2_persons(row.get("V2Persons", ""))
    organizations = parse_v2_organizations(row.get("V2Organizations", ""))
    locations = parse_v2_locations(row.get("V2Locations", ""))
    tone = parse_v2_tone(row.get("V2Tone", ""))

    return {
        "gkg_record_id": row.get("GKGRECORDID", ""),
        "date": row.get("DATE", ""),
        "source_collection_id": row.get("SourceCollectionIdentifier", ""),
        "source_name": row.get("SourceCommonName", ""),
        "document_identifier": row.get("DocumentIdentifier", ""),
        "themes": themes,
        "persons": persons,
        "organizations": organizations,
        "locations": locations,
        "tone": tone,
        "counts_raw": row.get("V2Counts", ""),
        "sharing_image": row.get("SharingImage", ""),
        "quotations": row.get("Quotations", ""),
        "all_names": row.get("AllNames", ""),
        "amounts": row.get("Amounts", ""),
        "translation_info": row.get("TranslationInfo", ""),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def build_entity_messages(record: dict[str, Any]) -> list[dict[str, Any]]:
    """Build entity-resolution messages from a parsed GKG record.

    Returns a list of entity dicts suitable for the entities Kafka topic.
    """
    entities: list[dict[str, Any]] = []
    gkg_id = record["gkg_record_id"]
    doc_id = record["document_identifier"]
    record_date = record["date"]

    for person in record["persons"]:
        entities.append({
            "entity_type": "PERSON",
            "entity_name": person,
            "source_record": gkg_id,
            "document_identifier": doc_id,
            "record_date": record_date,
            "themes": record["themes"],
        })

    for org in record["organizations"]:
        entities.append({
            "entity_type": "ORGANIZATION",
            "entity_name": org,
            "source_record": gkg_id,
            "document_identifier": doc_id,
            "record_date": record_date,
            "themes": record["themes"],
        })

    for loc in record["locations"]:
        entities.append({
            "entity_type": "LOCATION",
            "entity_name": loc.get("location_name", ""),
            "country_code": loc.get("country_code", ""),
            "latitude": loc.get("latitude", 0.0),
            "longitude": loc.get("longitude", 0.0),
            "feature_id": loc.get("feature_id", ""),
            "source_record": gkg_id,
            "document_identifier": doc_id,
            "record_date": record_date,
            "themes": record["themes"],
        })

    return entities


# ---------------------------------------------------------------------------
# Master file list & file selection
# ---------------------------------------------------------------------------

def fetch_latest_gkg_url() -> str | None:
    """Get the URL of the most recent GKG file from the GDELT master file list."""
    resp = requests.get(GDELT_MASTER_URL, timeout=30)
    resp.raise_for_status()

    for line in reversed(resp.text.strip().split("\n")):
        if ".gkg.csv.zip" in line:
            parts = line.split()
            if parts:
                return parts[-1].strip()
    return None


# ---------------------------------------------------------------------------
# Ingestion loop
# ---------------------------------------------------------------------------

class GKGIngester:
    """Continuous GKG ingestion worker."""

    def __init__(self) -> None:
        self._last_processed_url: str | None = None
        self._producer: KafkaProducer | None = None

    def _get_producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
        return self._producer

    def _close_producer(self) -> None:
        if self._producer is not None:
            try:
                self._producer.flush()
                self._producer.close()
            except Exception:
                logger.exception("Error closing Kafka producer")
            self._producer = None

    def ingest_latest(self) -> int:
        """Fetch and process the latest GKG file.

        Returns the number of MDA-relevant records published.
        """
        url = fetch_latest_gkg_url()
        if not url:
            logger.error("Could not find latest GKG file in master file list")
            return 0

        if url == self._last_processed_url:
            logger.debug("GKG file already processed: %s", url)
            return 0

        logger.info("Downloading GKG file: %s", url)
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()

        producer = self._get_producer()
        raw_count = 0
        entity_count = 0

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for name in zf.namelist():
                with zf.open(name) as f:
                    text = f.read().decode("utf-8", errors="replace")
                    reader = csv.DictReader(
                        io.StringIO(text),
                        fieldnames=GKG_HEADER,
                        delimiter="\t",
                    )

                    for row in reader:
                        record = parse_gkg_record(row)

                        if not is_mda_relevant(record["themes"]):
                            continue

                        # Publish full parsed record
                        producer.send(TOPIC_GKG_RAW, record)
                        raw_count += 1

                        # Publish entity messages
                        for entity_msg in build_entity_messages(record):
                            producer.send(TOPIC_GKG_ENTITIES, entity_msg)
                            entity_count += 1

        producer.flush()
        self._last_processed_url = url
        logger.info(
            "Published %d MDA-relevant GKG records and %d entity messages from %s",
            raw_count,
            entity_count,
            url,
        )
        return raw_count

    def run_forever(self) -> None:
        """Continuous polling loop with configurable interval."""
        logger.info(
            "Starting GKG ingester loop (poll interval=%ds)", POLL_INTERVAL_SECONDS
        )
        while True:
            try:
                self.ingest_latest()
            except requests.RequestException as exc:
                logger.error("Network error during GKG ingestion: %s", exc)
            except Exception:
                logger.exception("Unexpected error during GKG ingestion")
            time.sleep(POLL_INTERVAL_SECONDS)

    def shutdown(self) -> None:
        """Gracefully shut down the producer."""
        self._close_producer()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    ingester = GKGIngester()
    try:
        ingester.run_forever()
    except KeyboardInterrupt:
        logger.info("GKG ingester shutting down")
    finally:
        ingester.shutdown()


if __name__ == "__main__":
    main()
