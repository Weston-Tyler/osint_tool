"""EM-DAT International Disaster Database ingester.

Source: https://www.emdat.be/ (free with registration)
22,000+ natural and technological disasters since 1900.
"""

import csv
import json
import logging
import os
from datetime import datetime

from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.humanitarian.emdat")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

DISASTER_TYPE_MAP = {
    "Flood": "FLOOD",
    "Storm": "STORM",
    "Earthquake": "EARTHQUAKE",
    "Drought": "DROUGHT",
    "Volcanic activity": "VOLCANIC",
    "Epidemic": "EPIDEMIC",
    "Landslide": "LANDSLIDE",
    "Wildfire": "WILDFIRE",
    "Extreme temperature": "EXTREME_TEMP",
    "Mass movement (dry)": "MASS_MOVEMENT",
}


def normalize_disaster(row: dict) -> dict | None:
    dtype = row.get("Disaster Type", "")
    mapped = DISASTER_TYPE_MAP.get(dtype, dtype.upper().replace(" ", "_"))
    country = row.get("ISO", row.get("Country", ""))
    year = row.get("Start Year", "")
    month = row.get("Start Month", "1")
    deaths = int(row.get("Total Deaths", 0) or 0)
    affected = int(row.get("Total Affected", 0) or 0)
    damage_usd = float(row.get("Total Damage ('000 US$)", 0) or 0) * 1000

    if not year:
        return None

    severity = min(10.0, (deaths / 100) + (affected / 100000))

    return {
        "event_id": f"emdat_{row.get('Dis No', '')}",
        "event_type": f"NATURAL_DISASTER_{mapped}",
        "domain": "environmental",
        "occurred_at": f"{year}-{str(month).zfill(2)}-01T00:00:00Z",
        "location_country_iso": country[:2] if len(country) >= 2 else "",
        "location_region": row.get("Country", ""),
        "location_lat": float(row.get("Latitude", 0) or 0) or None,
        "location_lon": float(row.get("Longitude", 0) or 0) or None,
        "severity": round(severity, 2),
        "confidence": 0.95,
        "source_ids": ["emdat"],
        "description": f"{dtype} in {row.get('Country', '')} ({year}): {deaths} deaths, {affected:,} affected, ${damage_usd:,.0f} damage",
        "metadata": {"deaths": deaths, "affected": affected, "damage_usd": damage_usd, "disaster_subtype": row.get("Disaster Subtype", "")},
    }


def ingest_emdat(filepath: str):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    count = 0
    with open(filepath, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            event = normalize_disaster(row)
            if event:
                producer.send("mda.humanitarian.disasters", event)
                count += 1
    producer.flush()
    producer.close()
    logger.info("Ingested %d EM-DAT disaster events", count)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) < 2:
        print("Usage: python emdat_ingester.py <csv_path>")
    else:
        ingest_emdat(sys.argv[1])
