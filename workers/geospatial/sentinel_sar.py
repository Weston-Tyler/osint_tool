"""Sentinel-1 SAR dark vessel detection processor.

Uses Copernicus Data Space (free registration) for Sentinel-1 GRD imagery.
Simplified CFAR vessel detection suitable for Mac mini processing.
Source: https://dataspace.copernicus.eu/
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.geospatial.sentinel_sar")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
COPERNICUS_CLIENT_ID = os.getenv("COPERNICUS_CLIENT_ID", "")
COPERNICUS_CLIENT_SECRET = os.getenv("COPERNICUS_CLIENT_SECRET", "")
COPERNICUS_TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
COPERNICUS_CATALOG_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"


def get_access_token() -> str:
    """Get OAuth2 token from Copernicus Data Space."""
    if not COPERNICUS_CLIENT_ID or not COPERNICUS_CLIENT_SECRET:
        raise RuntimeError("COPERNICUS_CLIENT_ID and COPERNICUS_CLIENT_SECRET required")

    resp = requests.post(COPERNICUS_TOKEN_URL, data={
        "grant_type": "client_credentials",
        "client_id": COPERNICUS_CLIENT_ID,
        "client_secret": COPERNICUS_CLIENT_SECRET,
    }, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def search_sentinel1_imagery(bbox: tuple[float, float, float, float],
                              start_date: str, end_date: str,
                              max_results: int = 10) -> list[dict]:
    """Search Copernicus STAC catalog for Sentinel-1 GRD products."""
    west, south, east, north = bbox
    footprint = f"POLYGON(({west} {south},{east} {south},{east} {north},{west} {north},{west} {south}))"

    filter_str = (
        f"Collection/Name eq 'SENTINEL-1' and "
        f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq 'GRD') and "
        f"ContentDate/Start gt {start_date}T00:00:00.000Z and "
        f"ContentDate/Start lt {end_date}T23:59:59.999Z and "
        f"OData.CSC.Intersects(area=geography'SRID=4326;{footprint}')"
    )

    params = {
        "$filter": filter_str,
        "$top": max_results,
        "$orderby": "ContentDate/Start desc",
    }

    try:
        resp = requests.get(COPERNICUS_CATALOG_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("value", [])
    except Exception as e:
        logger.error("Copernicus catalog search error: %s", e)
        return []


def detect_vessels_simplified(product_metadata: dict) -> list[dict]:
    """Simplified vessel detection from SAR product metadata.

    In production, this would download the GRD product and run CFAR detection.
    For now, we log the product as a potential dark vessel detection zone.
    """
    footprint = product_metadata.get("Footprint", "")
    name = product_metadata.get("Name", "")
    start_date = product_metadata.get("ContentDate", {}).get("Start", "")

    return [{
        "event_id": f"sar_{product_metadata.get('Id', '')}",
        "source": "sentinel1_sar",
        "product_name": name,
        "acquisition_time": start_date,
        "footprint": footprint,
        "detection_type": "SAR_COVERAGE_ZONE",
        "note": "Full CFAR detection requires GRD download + processing pipeline",
        "confidence": 0.60,
    }]


def search_mda_zones():
    """Search for Sentinel-1 imagery in MDA-relevant zones."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    zones = {
        "eastern_pacific": (-105, 0, -75, 20),
        "caribbean": (-90, 10, -60, 25),
        "gulf_of_guinea": (-5, -5, 15, 10),
    }

    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")

    total = 0
    for zone_name, bbox in zones.items():
        products = search_sentinel1_imagery(bbox, start, end)
        for product in products:
            detections = detect_vessels_simplified(product)
            for det in detections:
                det["zone"] = zone_name
                producer.send("mda.sentinel.dark_vessels", det)
                total += 1
        logger.info("Zone %s: %d SAR products found", zone_name, len(products))

    producer.flush()
    producer.close()
    logger.info("Published %d SAR detection zones", total)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    search_mda_zones()
