"""Elasticsearch sync service — mirrors Memgraph entities to Elasticsearch.

Periodically syncs vessel, person, and organization entities from Memgraph
to Elasticsearch for full-text search, fuzzy name matching, and aggregation.
"""

import json
import logging
import os
import time
from datetime import datetime

from elasticsearch import Elasticsearch
from gqlalchemy import Memgraph

logger = logging.getLogger("mda.es_sync")

MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")


def sync_vessels(mg: Memgraph, es: Elasticsearch):
    """Sync all vessels from Memgraph to Elasticsearch."""
    results = list(mg.execute_and_fetch(
        """
        MATCH (v:Vessel)
        WHERE v.valid_to IS NULL
        RETURN v
        """
    ))

    actions = []
    for r in results:
        v = r["v"]
        props = dict(v) if hasattr(v, "__iter__") else {}

        doc = {
            "imo": props.get("imo"),
            "mmsi": props.get("mmsi"),
            "name": props.get("name"),
            "former_names": props.get("former_names", []),
            "call_sign": props.get("call_sign"),
            "flag_state": props.get("flag_state"),
            "vessel_type": props.get("vessel_type"),
            "risk_score": props.get("risk_score", 0),
            "sanctions_status": props.get("sanctions_status"),
            "ofac_programs": props.get("ofac_programs", []),
            "ais_status": props.get("ais_status"),
            "registered_owner": props.get("registered_owner"),
            "beneficial_owner": props.get("beneficial_owner"),
            "gross_tonnage": props.get("gross_tonnage"),
            "source_ids": props.get("source_ids", []),
            "system_updated_at": props.get("system_updated_at"),
        }

        # Add geo_point if position available
        lat = props.get("last_ais_position_lat")
        lon = props.get("last_ais_position_lon")
        if lat is not None and lon is not None:
            doc["last_position"] = {"lat": float(lat), "lon": float(lon)}
            doc["last_ais_timestamp"] = props.get("last_ais_timestamp")

        doc_id = props.get("imo") or props.get("mmsi") or props.get("entity_id", "unknown")
        actions.append({"index": {"_index": "mda_vessels", "_id": doc_id}})
        actions.append(doc)

    if actions:
        body = "\n".join(json.dumps(a) for a in actions) + "\n"
        resp = es.bulk(body=body, refresh=True)
        errors = resp.get("errors", False)
        items = resp.get("items", [])
        logger.info("Synced %d vessels to ES (errors=%s)", len(items), errors)
    else:
        logger.info("No vessels to sync")


def sync_persons(mg: Memgraph, es: Elasticsearch):
    """Sync sanctioned persons from Memgraph to Elasticsearch."""
    results = list(mg.execute_and_fetch(
        """
        MATCH (p:Person)
        WHERE p.sanctions_status = 'SANCTIONED'
        RETURN p
        """
    ))

    actions = []
    for r in results:
        p = r["p"]
        props = dict(p) if hasattr(p, "__iter__") else {}

        doc = {
            "entity_id": props.get("entity_id"),
            "name_full": props.get("name_full"),
            "aliases": props.get("aliases", []),
            "dob": props.get("dob"),
            "nationality": props.get("nationality"),
            "sanctions_status": props.get("sanctions_status"),
            "ofac_programs": props.get("ofac_programs", []),
            "source_ids": props.get("source_ids", []),
        }

        doc_id = props.get("entity_id", "unknown")
        actions.append({"index": {"_index": "mda_persons", "_id": doc_id}})
        actions.append(doc)

    if actions:
        body = "\n".join(json.dumps(a) for a in actions) + "\n"
        resp = es.bulk(body=body, refresh=True)
        logger.info("Synced %d persons to ES", len(resp.get("items", [])))


def sync_organizations(mg: Memgraph, es: Elasticsearch):
    """Sync organizations from Memgraph to Elasticsearch."""
    results = list(mg.execute_and_fetch(
        """
        MATCH (o:Organization)
        RETURN o
        """
    ))

    actions = []
    for r in results:
        o = r["o"]
        props = dict(o) if hasattr(o, "__iter__") else {}

        doc = {
            "entity_id": props.get("entity_id"),
            "name": props.get("name"),
            "aliases": props.get("aliases", []),
            "org_type": props.get("org_type"),
            "threat_level": props.get("threat_level"),
            "primary_country": props.get("primary_country"),
            "sanctions_status": props.get("sanctions_status"),
            "source_ids": props.get("source_ids", []),
        }

        doc_id = props.get("entity_id", "unknown")
        actions.append({"index": {"_index": "mda_organizations", "_id": doc_id}})
        actions.append(doc)

    if actions:
        body = "\n".join(json.dumps(a) for a in actions) + "\n"
        resp = es.bulk(body=body, refresh=True)
        logger.info("Synced %d organizations to ES", len(resp.get("items", [])))


def run_sync(interval_seconds: int = 300):
    """Run continuous sync loop."""
    mg = Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
    es = Elasticsearch(ELASTICSEARCH_URL)

    logger.info("ES sync service started (interval=%ds)", interval_seconds)

    while True:
        try:
            sync_vessels(mg, es)
            sync_persons(mg, es)
            sync_organizations(mg, es)
        except Exception as e:
            logger.error("Sync error: %s", e, exc_info=True)

        time.sleep(interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    run_sync()
