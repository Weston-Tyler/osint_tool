"""World Port Index (NGA) + Equasis vessel registry ingester.

World Port Index: 3,700+ ports from National Geospatial-Intelligence Agency.
Equasis: Vessel ownership, inspection history (free registration).
UN Comtrade: Bilateral trade flows by commodity.
UNCTAD Liner Shipping: Port connectivity index.
"""

import csv
import json
import logging
import os
import time

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.maritime")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


# ── World Port Index ─────────────────────────────────────────

def ingest_world_port_index(filepath: str):
    """Ingest NGA World Port Index CSV."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    count = 0
    with open(filepath, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            port = {
                "port_id": f"wpi_{row.get('INDEX_NO', '')}",
                "name": row.get("PORT_NAME", ""),
                "country": row.get("COUNTRY", ""),
                "lat": float(row.get("LATITUDE_DEG", 0) or 0) + float(row.get("LATITUDE_MIN", 0) or 0) / 60,
                "lon": float(row.get("LONGITUDE_DEG", 0) or 0) + float(row.get("LONGITUDE_MIN", 0) or 0) / 60,
                "harbor_size": row.get("HARBOR_SIZE", ""),
                "harbor_type": row.get("HARBOR_TYPE", ""),
                "shelter": row.get("SHELTER", ""),
                "max_vessel_length": row.get("MAX_VESSEL_LENGTH", ""),
                "channel_depth": row.get("CHANNEL_DEPTH", ""),
                "cargo_pier_depth": row.get("CARGO_PIER_DEPTH", ""),
                "repairs": row.get("REPAIRS", ""),
                "drydock": row.get("DRYDOCK", ""),
                "source": "nga_wpi",
            }
            producer.send("mda.maritime.ports", port)
            count += 1
    producer.flush()
    producer.close()
    logger.info("Ingested %d ports from World Port Index", count)


# ── UN Comtrade ──────────────────────────────────────────────

COMTRADE_API = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"

def fetch_comtrade_flows(reporter_iso: str, partner_iso: str, commodity_code: str = "TOTAL",
                          year: int = 2023) -> list[dict]:
    """Fetch bilateral trade flows from UN Comtrade API (free, no key for preview)."""
    params = {
        "reporterCode": reporter_iso,
        "partnerCode": partner_iso,
        "period": str(year),
        "cmdCode": commodity_code,
        "flowCode": "M,X",  # imports and exports
    }
    try:
        resp = requests.get(COMTRADE_API, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])
    except Exception as e:
        logger.error("Comtrade API error: %s", e)
        return []


def ingest_comtrade_mda_pairs():
    """Ingest trade flows between MDA-relevant country pairs."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    # Key MDA trade pairs
    pairs = [
        ("484", "170"),  # Mexico-Colombia
        ("484", "840"),  # Mexico-US
        ("170", "840"),  # Colombia-US
        ("484", "320"),  # Mexico-Guatemala
        ("591", "484"),  # Panama-Mexico
    ]
    count = 0
    for reporter, partner in pairs:
        flows = fetch_comtrade_flows(reporter, partner)
        for flow in flows:
            record = {
                "reporter": flow.get("reporterDesc", ""),
                "partner": flow.get("partnerDesc", ""),
                "flow_type": flow.get("flowDesc", ""),
                "commodity": flow.get("cmdDesc", ""),
                "year": flow.get("period", ""),
                "value_usd": flow.get("primaryValue", 0),
                "quantity": flow.get("qty", 0),
                "source": "un_comtrade",
            }
            producer.send("mda.maritime.trade_flows", record)
            count += 1
        time.sleep(2)  # Rate limit

    producer.flush()
    producer.close()
    logger.info("Ingested %d trade flow records from Comtrade", count)


# ── UNCTAD Liner Shipping Connectivity ───────────────────────

def ingest_unctad_lsci(filepath: str):
    """Ingest UNCTAD Liner Shipping Connectivity Index CSV."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    count = 0
    with open(filepath, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            record = {
                "country": row.get("Country", row.get("Economy", "")),
                "year": row.get("Year", ""),
                "lsci_score": float(row.get("LSCI", row.get("Value", 0)) or 0),
                "source": "unctad_lsci",
            }
            producer.send("mda.maritime.connectivity", record)
            count += 1
    producer.flush()
    producer.close()
    logger.info("Ingested %d UNCTAD LSCI records", count)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) < 2:
        print("Usage: python maritime_ingester.py <wpi_csv|comtrade|unctad_csv>")
    elif sys.argv[1] == "comtrade":
        ingest_comtrade_mda_pairs()
    else:
        ingest_world_port_index(sys.argv[1])
