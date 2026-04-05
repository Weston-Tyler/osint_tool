"""RDF exporter — Memgraph to Stardog via Turtle RDF.

Exports graph entities to RDF format aligned with EUCISE-OWL maritime ontology
for loading into the Stardog semantic layer.

Reference: https://www.semantic-web-journal.net/system/files/swj2234.pdf
"""

import logging
import os

import requests
from gqlalchemy import Memgraph
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import OWL, RDF, RDFS, XSD

logger = logging.getLogger("mda.rdf_exporter")

# Namespaces aligned with EUCISE-OWL
MDA = Namespace("http://mda.opendata.org/ontology#")
EUCISE = Namespace("http://www.w3c.org/eucise/eucise-owl#")
IMO_NS = Namespace("http://imo.int/vessel/")

STARDOG_URL = os.getenv("STARDOG_URL", "http://localhost:5820")
STARDOG_DB = os.getenv("STARDOG_DB", "mda")
STARDOG_USER = os.getenv("STARDOG_USER", "admin")
STARDOG_PASS = os.getenv("STARDOG_PASSWORD", "admin")


def export_vessel_to_rdf(vessel: dict) -> Graph:
    """Convert a Vessel property graph node to RDF."""
    g = Graph()
    g.bind("mda", MDA)
    g.bind("eucise", EUCISE)
    g.bind("imo", IMO_NS)

    vessel_uri = IMO_NS[vessel["imo"]] if vessel.get("imo") else MDA[f"vessel/mmsi/{vessel.get('mmsi', 'unknown')}"]

    g.add((vessel_uri, RDF.type, EUCISE.Vessel))
    g.add((vessel_uri, RDF.type, MDA.Vessel))

    if vessel.get("imo"):
        g.add((vessel_uri, MDA.imoNumber, Literal(vessel["imo"])))
    if vessel.get("mmsi"):
        g.add((vessel_uri, MDA.mmsi, Literal(vessel["mmsi"])))
    if vessel.get("name"):
        g.add((vessel_uri, RDFS.label, Literal(vessel["name"])))
    if vessel.get("flag_state"):
        g.add((vessel_uri, MDA.flagState, Literal(vessel["flag_state"])))
    if vessel.get("risk_score") is not None:
        g.add((vessel_uri, MDA.riskScore, Literal(float(vessel["risk_score"]), datatype=XSD.decimal)))
    if vessel.get("sanctions_status"):
        g.add((vessel_uri, MDA.sanctionsStatus, Literal(vessel["sanctions_status"])))

    return g


def push_to_stardog(rdf_graph: Graph, named_graph: str = "http://mda.opendata.org/graphs/vessels"):
    """POST RDF Turtle to Stardog via REST API."""
    turtle_data = rdf_graph.serialize(format="turtle")
    resp = requests.post(
        f"{STARDOG_URL}/{STARDOG_DB}",
        params={"graph-uri": named_graph},
        headers={"Content-Type": "text/turtle"},
        data=turtle_data.encode("utf-8"),
        auth=(STARDOG_USER, STARDOG_PASS),
        timeout=60,
    )
    resp.raise_for_status()
    logger.info("Pushed %d triples to Stardog graph: %s", len(rdf_graph), named_graph)


def sync_sanctioned_vessels_to_stardog():
    """Export all sanctioned vessels from Memgraph to Stardog."""
    mg = Memgraph(
        host=os.getenv("MEMGRAPH_HOST", "localhost"),
        port=int(os.getenv("MEMGRAPH_PORT", "7687")),
    )

    results = mg.execute_and_fetch(
        """
        MATCH (v:Vessel)
        WHERE v.sanctions_status = 'SANCTIONED'
        RETURN v
        """
    )

    combined = Graph()
    combined.bind("mda", MDA)
    combined.bind("eucise", EUCISE)

    count = 0
    for result in results:
        vessel = dict(result["v"]) if hasattr(result["v"], "__iter__") else {}
        combined += export_vessel_to_rdf(vessel)
        count += 1

    push_to_stardog(combined, "http://mda.opendata.org/graphs/sanctioned_vessels")
    logger.info("Exported %d sanctioned vessels to Stardog", count)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    sync_sanctioned_vessels_to_stardog()
