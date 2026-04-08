"""OpenSanctions FollowTheMoney (FTM) data ingestion.

Downloads and parses the OpenSanctions default dataset (40+ sanctions lists merged).
Source: https://www.opensanctions.org/datasets/us_ofac_sdn/
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Iterator

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.sanctions.opensanctions")

OPENSANCTIONS_URL = "https://data.opensanctions.org/datasets/latest/default/entities.ftm.json"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DATA_DIR = Path("/data/sanctions")


def download_opensanctions(output_path: Path | None = None) -> Path:
    """Download the latest OpenSanctions default dataset."""
    if output_path is None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        output_path = DATA_DIR / "opensanctions_default_latest.ftm.json"

    logger.info("Downloading OpenSanctions from %s", OPENSANCTIONS_URL)
    resp = requests.get(OPENSANCTIONS_URL, timeout=300, stream=True)
    resp.raise_for_status()

    with open(output_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info("Downloaded OpenSanctions to %s", output_path)
    return output_path


def stream_entities(filepath: Path, schema_filter: list[str] | None = None) -> Iterator[dict]:
    """Stream OpenSanctions FTM entities from local file."""
    with open(filepath) as f:
        for line in f:
            entity = json.loads(line)
            if schema_filter is None or entity.get("schema") in schema_filter:
                yield entity


def ftm_to_vessel(entity: dict) -> dict:
    """Convert FTM Vessel entity to MDA schema."""
    props = entity.get("properties", {})
    return {
        "entity_id": entity["id"],
        "entity_type": "vessel",
        "imo": (props.get("imoNumber") or [None])[0],
        "mmsi": (props.get("mmsi") or [None])[0],
        "name": (props.get("name") or [None])[0],
        "aliases": (props.get("name") or [])[1:] + (props.get("weakAlias") or []),
        "flag_state": (props.get("flag") or [None])[0],
        "call_sign": (props.get("callSign") or [None])[0],
        "vessel_type": (props.get("type") or [None])[0],
        "gross_tonnage": (props.get("grossRegisteredTonnage") or [None])[0],
        "sanctions_status": "SANCTIONED",
        "sanctions_datasets": entity.get("datasets", []),
        "source": "opensanctions",
        "ingest_time": datetime.utcnow().isoformat(),
    }


def ftm_to_person(entity: dict) -> dict:
    """Convert FTM Person entity to MDA schema."""
    props = entity.get("properties", {})
    return {
        "entity_id": entity["id"],
        "entity_type": "person",
        "name_full": (props.get("name") or [None])[0],
        "aliases": (props.get("name") or [])[1:] + (props.get("weakAlias") or []),
        "dob": (props.get("birthDate") or [None])[0],
        "nationality": (props.get("nationality") or [None])[0],
        "place_of_birth": (props.get("birthPlace") or [None])[0],
        "sanctions_status": "SANCTIONED",
        "sanctions_datasets": entity.get("datasets", []),
        "source": "opensanctions",
        "ingest_time": datetime.utcnow().isoformat(),
    }


def ftm_to_organization(entity: dict) -> dict:
    """Convert FTM Organization/Company entity to MDA schema."""
    props = entity.get("properties", {})
    return {
        "entity_id": entity["id"],
        "entity_type": "organization",
        "name": (props.get("name") or [None])[0],
        "aliases": (props.get("name") or [])[1:] + (props.get("weakAlias") or []),
        "jurisdiction": (props.get("jurisdiction") or [None])[0],
        "sanctions_status": "SANCTIONED",
        "sanctions_datasets": entity.get("datasets", []),
        "source": "opensanctions",
        "ingest_time": datetime.utcnow().isoformat(),
    }


FTM_CONVERTERS = {
    "Vessel": ftm_to_vessel,
    "Person": ftm_to_person,
    "Organization": ftm_to_organization,
    "Company": ftm_to_organization,
    "LegalEntity": ftm_to_organization,
}


def ingest_opensanctions(filepath: Path | None = None):
    """Download and publish OpenSanctions data to Kafka."""
    if filepath is None or not filepath.exists():
        filepath = download_opensanctions()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    counts = {}
    for entity in stream_entities(filepath, schema_filter=list(FTM_CONVERTERS.keys())):
        schema = entity.get("schema", "")
        converter = FTM_CONVERTERS.get(schema)
        if not converter:
            continue

        try:
            normalized = converter(entity)
            producer.send("mda.sanctions.updates", normalized)
            counts[schema] = counts.get(schema, 0) + 1
        except Exception as e:
            producer.send("mda.dlq", {"source": "opensanctions", "error": str(e), "entity_id": entity.get("id")})

    producer.flush()
    producer.close()
    logger.info("OpenSanctions ingestion complete: %s", counts)


def run_loop(interval_seconds: int) -> None:
    """Re-ingest OpenSanctions on a fixed interval forever."""
    import time

    iteration = 0
    while True:
        iteration += 1
        logger.info("OpenSanctions ingest iteration %d starting", iteration)
        try:
            ingest_opensanctions()
            sleep_for = interval_seconds
        except Exception as exc:
            logger.exception("Iteration %d failed: %s", iteration, exc)
            sleep_for = min(interval_seconds, 600)
        logger.info("Iteration %d done, sleeping %ds", iteration, sleep_for)
        time.sleep(sleep_for)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    ap = argparse.ArgumentParser(description="OpenSanctions FtM Kafka ingester")
    ap.add_argument(
        "--loop",
        type=int,
        default=0,
        metavar="SECONDS",
        help="Run continuously, re-fetching every N seconds (default: one-shot)",
    )
    args = ap.parse_args()

    if args.loop > 0:
        run_loop(args.loop)
    else:
        ingest_opensanctions()
