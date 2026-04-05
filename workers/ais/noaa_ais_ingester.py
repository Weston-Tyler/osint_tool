"""NOAA AIS / MarineCadastre GeoParquet ingestion into Kafka.

Downloads and processes bulk AIS data from NOAA MarineCadastre archives.
Source: https://marinecadastre.gov/downloads/data/ais/
       https://github.com/ocm-marinecadastre/ais-vessel-traffic
"""

import glob
import json
import logging
import os
import re

import geopandas as gpd
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.ais.noaa")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        compression_type="gzip",
        batch_size=65536,
        linger_ms=50,
    )


def ingest_ais_geoparquet(filepath: str, producer: KafkaProducer) -> int:
    """Ingest a MarineCadastre GeoParquet file into Kafka.

    Returns number of records published.
    """
    logger.info("Loading %s", filepath)
    gdf = gpd.read_parquet(filepath)

    # Normalize columns (MarineCadastre schema)
    column_map = {
        "MMSI": "mmsi",
        "BaseDateTime": "timestamp",
        "LAT": "lat",
        "LON": "lon",
        "SOG": "speed_kts",
        "COG": "course",
        "Heading": "heading",
        "VesselName": "vessel_name",
        "IMO": "imo",
        "CallSign": "call_sign",
        "VesselType": "vessel_type_code",
        "Status": "nav_status",
        "Length": "length_m",
        "Width": "width_m",
        "Draft": "draft_m",
        "Cargo": "cargo_type",
        "TranscieverClass": "transceiver_class",
    }
    gdf = gdf.rename(columns={k: v for k, v in column_map.items() if k in gdf.columns})

    # Drop invalid records
    gdf = gdf[gdf["mmsi"].notna() & gdf["lat"].notna() & gdf["lon"].notna()]
    gdf = gdf[gdf["mmsi"].astype(str).str.match(r"^\d{9}$")]

    logger.info("Loaded %s records from %s", f"{len(gdf):,}", filepath)

    count = 0
    batch = []
    for _, row in gdf.iterrows():
        record = {
            "source": "noaa_marinecadastre",
            "mmsi": str(row.get("mmsi", "")),
            "imo": str(row.get("imo", "")) if row.get("imo") else "",
            "timestamp": str(row.get("timestamp", "")),
            "lat": float(row.get("lat", 0)),
            "lon": float(row.get("lon", 0)),
            "speed_kts": float(row.get("speed_kts", 0) or 0),
            "course": float(row.get("course", 0) or 0),
            "heading": int(row.get("heading", 511) or 511),
            "nav_status": int(row.get("nav_status", 15) or 15),
            "vessel_name": str(row.get("vessel_name", "") or ""),
            "vessel_type": int(row.get("vessel_type_code", 0) or 0),
            "length_m": float(row.get("length_m", 0) or 0),
            "draft_m": float(row.get("draft_m", 0) or 0),
        }
        batch.append(record)

        if len(batch) >= 1000:
            for r in batch:
                producer.send("mda.ais.positions.raw", r)
            count += len(batch)
            batch = []

    # Flush remaining
    for r in batch:
        producer.send("mda.ais.positions.raw", r)
    count += len(batch)

    producer.flush()
    logger.info("Published %s AIS records to Kafka", f"{count:,}")
    return count


def ingest_all(data_dir: str = "/data/ais/raw"):
    """Ingest all GeoParquet files found in data_dir."""
    producer = create_producer()
    files = glob.glob(os.path.join(data_dir, "**/*.parquet"), recursive=True)
    logger.info("Found %d parquet files in %s", len(files), data_dir)

    total = 0
    for f in sorted(files):
        total += ingest_ais_geoparquet(f, producer)

    logger.info("Total AIS records ingested: %s", f"{total:,}")
    producer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    ingest_all()
