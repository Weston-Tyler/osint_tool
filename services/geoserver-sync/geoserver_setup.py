"""GeoServer layer publisher — creates MDA workspace and publishes PostGIS layers.

Configures GeoServer programmatically via REST API to expose PostGIS tables
as WMS/WFS layers for operational map clients (Kepler.gl, ATAK, etc.).
"""

import logging
import os

import requests

logger = logging.getLogger("mda.geoserver_setup")

GS_BASE = os.getenv("GEOSERVER_URL", "http://localhost:8085/geoserver")
GS_USER = os.getenv("GEOSERVER_ADMIN_USER", "admin")
GS_PASS = os.getenv("GEOSERVER_ADMIN_PASSWORD", "geoserver")
GS_AUTH = (GS_USER, GS_PASS)
WS = "mda"


def create_workspace():
    resp = requests.post(
        f"{GS_BASE}/rest/workspaces",
        auth=GS_AUTH,
        headers={"Content-Type": "application/json"},
        json={"workspace": {"name": WS}},
    )
    if resp.status_code not in (201, 409):
        resp.raise_for_status()
    logger.info("Workspace '%s' ready", WS)


def create_postgis_datastore():
    """Create a PostGIS datastore pointing to MDA database."""
    payload = {
        "dataStore": {
            "name": "mda_postgis",
            "connectionParameters": {
                "entry": [
                    {"@key": "host", "$": "postgres"},
                    {"@key": "port", "$": "5432"},
                    {"@key": "database", "$": "mda"},
                    {"@key": "user", "$": "mda"},
                    {"@key": "passwd", "$": os.getenv("POSTGRES_PASSWORD", "mda")},
                    {"@key": "dbtype", "$": "postgis"},
                    {"@key": "schema", "$": "public"},
                    {"@key": "Expose primary keys", "$": "true"},
                ]
            },
        }
    }
    resp = requests.post(
        f"{GS_BASE}/rest/workspaces/{WS}/datastores",
        auth=GS_AUTH,
        headers={"Content-Type": "application/json"},
        json=payload,
    )
    if resp.status_code not in (201, 409):
        resp.raise_for_status()
    logger.info("PostGIS datastore 'mda_postgis' ready")


def publish_layer(table_name: str, layer_name: str, title: str):
    """Publish a PostGIS table as a GeoServer WMS/WFS layer."""
    payload = {
        "featureType": {
            "name": layer_name,
            "nativeName": table_name,
            "title": title,
            "srs": "EPSG:4326",
            "projectionPolicy": "FORCE_DECLARED",
            "enabled": True,
        }
    }
    resp = requests.post(
        f"{GS_BASE}/rest/workspaces/{WS}/datastores/mda_postgis/featuretypes",
        auth=GS_AUTH,
        headers={"Content-Type": "application/json"},
        json=payload,
    )
    if resp.status_code not in (201, 409):
        resp.raise_for_status()
    logger.info("Published layer: %s:%s", WS, layer_name)


# MDA layers to publish
MDA_LAYERS = [
    ("maritime_zones", "maritime_zones", "Maritime Zones (EEZ, JIATF AOR, etc.)"),
    ("vessel_tracks", "vessel_tracks", "Vessel Tracks (30-day)"),
    ("sts_zones", "sts_zones", "Known STS Transfer Zones"),
    ("trafficking_routes", "trafficking_routes", "Known Drug Trafficking Routes"),
    ("interdiction_locations", "interdiction_locations", "Interdiction Events"),
    ("uas_flight_paths", "uas_flight_paths", "UAS Flight Paths"),
    ("ais_positions", "ais_positions", "Live AIS Positions"),
]


def setup_geoserver():
    """Full GeoServer setup: workspace, datastore, all layers."""
    create_workspace()
    create_postgis_datastore()
    for table, layer, title in MDA_LAYERS:
        publish_layer(table, layer, title)
    logger.info("GeoServer setup complete")
    logger.info("WMS: %s/%s/wms?service=WMS&version=1.1.0&request=GetCapabilities", GS_BASE, WS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    setup_geoserver()
