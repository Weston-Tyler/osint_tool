"""Stardog Causal Exporter — exports causal graph to RDF/Turtle for Stardog.

Aligns with CCO (Common Core Ontologies) and PROV-O.
"""

from __future__ import annotations

import logging
import os

import requests
from gqlalchemy import Memgraph

logger = logging.getLogger("mda.stardog.causal_export")

MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))
STARDOG_URL = os.getenv("STARDOG_URL", "http://localhost:5820")
STARDOG_DB = os.getenv("STARDOG_DB", "mda")
STARDOG_USER = os.getenv("STARDOG_USER", "admin")
STARDOG_PASS = os.getenv("STARDOG_PASSWORD", "admin")

MDA_NS = "http://mda.local/ontology/causal#"
INST_NS = "http://mda.local/instances/"

EDGE_PREDICATE_MAP = {
    "DIRECTLY_CAUSES": "mda:directlyCauses",
    "ENABLES": "mda:enables",
    "INHIBITS": "mda:inhibits",
    "GRANGER_CAUSES": "mda:grangerCauses",
    "CORRELATES_WITH": "mda:correlatesWith",
    "FEEDBACK_INTO": "mda:feedsBackInto",
}


def event_to_turtle(event: dict, edges: list[dict]) -> str:
    """Convert a CausalEvent + its edges to Turtle RDF."""
    eid = event.get("event_id", "unknown")
    lines = [
        f"@prefix mda:  <{MDA_NS}> .",
        f"@prefix inst: <{INST_NS}> .",
        f"@prefix prov: <http://www.w3.org/ns/prov#> .",
        f"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
        f"@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .",
        "",
        f"inst:{eid}",
        f"    a mda:CausalEvent ;",
        f'    rdfs:label "{_escape(event.get("description", eid))}" ;',
        f'    mda:confidence "{event.get("confidence", 0.5):.4f}"^^xsd:float ;',
        f'    mda:eventType "{event.get("event_type", "")}" ;',
        f'    mda:domain "{event.get("domain", "")}" ;',
        f'    mda:riskScore "{event.get("risk_score", 0)}"^^xsd:float ;',
        f'    mda:locationRegion "{_escape(event.get("location_region", ""))}" .',
        "",
    ]

    for edge in edges:
        pred = EDGE_PREDICATE_MAP.get(edge.get("type", ""), "mda:directlyCauses")
        target = edge.get("target_event_id", "")
        lines.append(f"inst:{eid} {pred} inst:{target} .")

    return "\n".join(lines)


def prediction_to_turtle(pred: dict) -> str:
    """Convert a CausalPrediction to Turtle RDF."""
    pid = pred.get("prediction_id", "unknown")
    return "\n".join([
        f"@prefix mda:  <{MDA_NS}> .",
        f"@prefix inst: <{INST_NS}> .",
        f"@prefix prov: <http://www.w3.org/ns/prov#> .",
        f"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
        f"@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .",
        "",
        f"inst:pred_{pid}",
        f"    a mda:CausalPrediction ;",
        f'    rdfs:label "{pred.get("predicted_event_type", "")} prediction" ;',
        f'    mda:confidence "{pred.get("confidence", 0.0):.4f}"^^xsd:float ;',
        f'    mda:eventType "{pred.get("predicted_event_type", "")}" ;',
        f'    mda:locationRegion "{_escape(pred.get("predicted_location_region", ""))}" ;',
        f"    mda:predictedFrom inst:{pred.get('trigger_event_id', 'unknown')} .",
    ])


def _escape(s: str) -> str:
    return s.replace('"', '\\"').replace("\n", " ")[:200]


def push_turtle(turtle: str, graph_uri: str):
    """Upload Turtle RDF to Stardog."""
    resp = requests.post(
        f"{STARDOG_URL}/{STARDOG_DB}",
        params={"graph-uri": graph_uri},
        headers={"Content-Type": "text/turtle"},
        data=turtle.encode("utf-8"),
        auth=(STARDOG_USER, STARDOG_PASS),
        timeout=60,
    )
    if resp.status_code not in (200, 201, 204):
        logger.error("Stardog upload failed: HTTP %d", resp.status_code)
    else:
        logger.info("Uploaded to Stardog graph: %s", graph_uri)


def export_all_causal_events():
    """Export all high-confidence causal events to Stardog."""
    mg = Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)

    events = list(mg.execute_and_fetch("""
        MATCH (e:CausalEvent)
        WHERE e.confidence >= 0.5
        RETURN properties(e) AS props
        LIMIT 1000
    """))

    combined_turtle_parts = []
    for r in events:
        event = dict(r["props"])
        eid = event.get("event_id", "")

        # Get edges
        edges = list(mg.execute_and_fetch("""
            MATCH (e:CausalEvent {event_id: $eid})-[r]->(t:CausalEvent)
            RETURN type(r) AS type, t.event_id AS target_event_id, r.confidence AS confidence
        """, {"eid": eid}))
        edge_list = [dict(e) for e in edges]

        turtle = event_to_turtle(event, edge_list)
        combined_turtle_parts.append(turtle)

    if combined_turtle_parts:
        combined = "\n\n".join(combined_turtle_parts)
        push_turtle(combined, "http://mda.local/graphs/causal/events")
        logger.info("Exported %d causal events to Stardog", len(events))


def export_all_predictions():
    """Export all active predictions to Stardog."""
    mg = Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)

    preds = list(mg.execute_and_fetch("""
        MATCH (p:CausalPrediction)
        WHERE p.confidence >= 0.4
        RETURN properties(p) AS props
        LIMIT 500
    """))

    parts = [prediction_to_turtle(dict(r["props"])) for r in preds]
    if parts:
        push_turtle("\n\n".join(parts), "http://mda.local/graphs/causal/predictions")
        logger.info("Exported %d predictions to Stardog", len(preds))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    export_all_causal_events()
    export_all_predictions()
