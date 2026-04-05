"""OFAC SDN list ingestion — vessels, persons, and organizations.

Downloads and parses the OFAC Specially Designated Nationals (SDN) list.
Source: https://ofac.treasury.gov/sanctions-list-service
"""

import json
import logging
import os
import re
from datetime import datetime

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.sanctions.ofac")

OFAC_SDN_JSON_URL = "https://www.treasury.gov/ofac/downloads/sanctions/1.0/sdn_advanced.json"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def fetch_ofac_sdn() -> dict:
    """Download the OFAC SDN list in advanced JSON format."""
    logger.info("Downloading OFAC SDN from %s", OFAC_SDN_JSON_URL)
    resp = requests.get(OFAC_SDN_JSON_URL, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    logger.info("Downloaded OFAC SDN: %d entries", len(data.get("sdnEntries", [])))
    return data


def parse_ofac_vessel(entry: dict) -> dict | None:
    """Parse an OFAC SDN entry that is a vessel."""
    if entry.get("sdnType", "").upper() != "VESSEL":
        return None

    imo = None
    mmsi = None
    for id_entry in entry.get("ids", []):
        id_type = id_entry.get("idType", "").upper()
        id_number = id_entry.get("idNumber", "").strip()
        if "IMO" in id_type:
            imo = re.sub(r"^IMO\s*", "", id_number).strip()
        elif "MMSI" in id_type:
            mmsi = id_number

    programs = [p.get("program", "") for p in entry.get("programs", [])]

    return {
        "entity_id": f"ofac_vessel_{entry['uid']}",
        "entity_type": "vessel",
        "ofac_uid": entry["uid"],
        "name": entry.get("lastName", ""),
        "aliases": [a.get("lastName", "") for a in entry.get("akas", []) if a.get("lastName")],
        "imo": imo,
        "mmsi": mmsi,
        "sanctions_status": "SANCTIONED",
        "ofac_programs": programs,
        "remarks": entry.get("remarks", ""),
        "source": "ofac_sdn",
        "ingest_time": datetime.utcnow().isoformat(),
    }


def parse_ofac_individual(entry: dict) -> dict | None:
    """Parse an OFAC SDN entry that is an individual."""
    if entry.get("sdnType", "").upper() != "INDIVIDUAL":
        return None

    return {
        "entity_id": f"ofac_person_{entry['uid']}",
        "entity_type": "person",
        "ofac_uid": entry["uid"],
        "name_last": entry.get("lastName", ""),
        "name_first": entry.get("firstName", ""),
        "name_full": f"{entry.get('firstName', '')} {entry.get('lastName', '')}".strip(),
        "aliases": [
            f"{a.get('firstName', '')} {a.get('lastName', '')}".strip()
            for a in entry.get("akas", [])
            if a.get("lastName")
        ],
        "dob": entry.get("dateOfBirth", ""),
        "place_of_birth": entry.get("placeOfBirth", ""),
        "nationality": entry.get("nationality", ""),
        "sanctions_status": "SANCTIONED",
        "ofac_programs": [p.get("program", "") for p in entry.get("programs", [])],
        "remarks": entry.get("remarks", ""),
        "source": "ofac_sdn",
        "ingest_time": datetime.utcnow().isoformat(),
    }


def parse_ofac_entity(entry: dict) -> dict | None:
    """Parse an OFAC SDN entry that is an organization/entity."""
    if entry.get("sdnType", "").upper() != "ENTITY":
        return None

    return {
        "entity_id": f"ofac_org_{entry['uid']}",
        "entity_type": "organization",
        "ofac_uid": entry["uid"],
        "name": entry.get("lastName", ""),
        "aliases": [a.get("lastName", "") for a in entry.get("akas", []) if a.get("lastName")],
        "sanctions_status": "SANCTIONED",
        "ofac_programs": [p.get("program", "") for p in entry.get("programs", [])],
        "remarks": entry.get("remarks", ""),
        "source": "ofac_sdn",
        "ingest_time": datetime.utcnow().isoformat(),
    }


def ingest_ofac_sdn():
    """Download and publish OFAC SDN to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    sdn_data = fetch_ofac_sdn()

    counts = {"vessel": 0, "person": 0, "organization": 0, "skipped": 0}

    for entry in sdn_data.get("sdnEntries", []):
        parsed = parse_ofac_vessel(entry)
        if parsed:
            producer.send("mda.sanctions.updates", parsed)
            counts["vessel"] += 1
            continue

        parsed = parse_ofac_individual(entry)
        if parsed:
            producer.send("mda.sanctions.updates", parsed)
            counts["person"] += 1
            continue

        parsed = parse_ofac_entity(entry)
        if parsed:
            producer.send("mda.sanctions.updates", parsed)
            counts["organization"] += 1
            continue

        counts["skipped"] += 1

    producer.flush()
    producer.close()
    logger.info("OFAC SDN ingestion complete: %s", counts)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    ingest_ofac_sdn()
