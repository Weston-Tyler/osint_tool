"""STIX 2.1 threat intelligence export.

Exports MDA graph entities and relationships to STIX 2.1 bundle format
for sharing with ISAC/ISAO partners, law enforcement, and threat intel platforms.

Reference: https://docs.oasis-open.org/cti/stix/v2.1/stix-v2.1.html
"""

import json
import logging
import os
import uuid
from datetime import datetime

from gqlalchemy import Memgraph

logger = logging.getLogger("mda.stix_export")

MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))

# STIX 2.1 identity for this system (the producing organization)
MDA_IDENTITY = {
    "type": "identity",
    "spec_version": "2.1",
    "id": "identity--mda-osint-database",
    "created": "2026-01-01T00:00:00.000Z",
    "modified": "2026-01-01T00:00:00.000Z",
    "name": "MDA OSINT Database",
    "description": "Open-source Maritime Domain Awareness intelligence database",
    "identity_class": "system",
    "sectors": ["defense", "government-national"],
}


def _stix_id(prefix: str, entity_id: str) -> str:
    """Generate deterministic STIX ID from entity ID."""
    ns = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    return f"{prefix}--{uuid.uuid5(ns, entity_id)}"


def vessel_to_stix_indicator(vessel: dict) -> dict | None:
    """Convert a sanctioned vessel to a STIX 2.1 Indicator."""
    imo = vessel.get("imo")
    mmsi = vessel.get("mmsi")
    name = vessel.get("name", "Unknown Vessel")

    if not imo and not mmsi:
        return None

    # Build STIX pattern (vessel identifiers)
    patterns = []
    if imo:
        patterns.append(f"[x-maritime-vessel:imo = '{imo}']")
    if mmsi:
        patterns.append(f"[x-maritime-vessel:mmsi = '{mmsi}']")

    pattern = " OR ".join(patterns)

    programs = vessel.get("ofac_programs", [])
    labels = ["malicious-activity", "sanctions"]
    if programs:
        labels.extend([f"ofac-{p.lower()}" for p in programs[:3]])

    return {
        "type": "indicator",
        "spec_version": "2.1",
        "id": _stix_id("indicator", f"vessel-{imo or mmsi}"),
        "created": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "modified": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "name": f"Sanctioned Vessel: {name}",
        "description": (
            f"Vessel {name} (IMO: {imo or 'N/A'}, MMSI: {mmsi or 'N/A'}) "
            f"is listed on sanctions programs: {', '.join(programs) or 'N/A'}. "
            f"Flag: {vessel.get('flag_state', 'N/A')}. "
            f"Risk score: {vessel.get('risk_score', 'N/A')}/10."
        ),
        "pattern": pattern,
        "pattern_type": "stix",
        "valid_from": vessel.get("valid_from", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")),
        "labels": labels,
        "confidence": int((vessel.get("confidence", 0.9)) * 100),
        "created_by_ref": MDA_IDENTITY["id"],
        "external_references": [
            {
                "source_name": "OFAC SDN",
                "description": f"OFAC programs: {', '.join(programs)}",
            }
        ],
    }


def person_to_stix_threat_actor(person: dict) -> dict | None:
    """Convert a sanctioned person to a STIX 2.1 Threat Actor."""
    name = person.get("name_full", "Unknown Person")
    entity_id = person.get("entity_id", "")

    if not entity_id:
        return None

    aliases = person.get("aliases", [])
    roles = []
    if person.get("role"):
        roles.append(person["role"].lower().replace("_", "-"))

    return {
        "type": "threat-actor",
        "spec_version": "2.1",
        "id": _stix_id("threat-actor", entity_id),
        "created": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "modified": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "name": name,
        "description": f"Sanctioned individual: {name}. Nationality: {person.get('nationality', 'N/A')}.",
        "aliases": aliases[:10],
        "roles": roles or ["unknown"],
        "sophistication": "expert",
        "resource_level": "organization",
        "primary_motivation": "personal-gain",
        "labels": ["crime-syndicate"],
        "confidence": int((person.get("confidence", 0.9)) * 100),
        "created_by_ref": MDA_IDENTITY["id"],
    }


def org_to_stix_threat_actor(org: dict) -> dict | None:
    """Convert a cartel/TCO to a STIX 2.1 Threat Actor."""
    name = org.get("name", "Unknown Organization")
    entity_id = org.get("entity_id", "")

    if not entity_id:
        return None

    return {
        "type": "threat-actor",
        "spec_version": "2.1",
        "id": _stix_id("threat-actor", entity_id),
        "created": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "modified": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "name": name,
        "description": (
            f"Organization: {name}. Type: {org.get('org_type', 'N/A')}. "
            f"Threat level: {org.get('threat_level', 'N/A')}. "
            f"Primary country: {org.get('primary_country', 'N/A')}."
        ),
        "aliases": org.get("aliases", [])[:10],
        "roles": ["director"] if org.get("org_type") == "CARTEL" else ["unknown"],
        "sophistication": "expert" if org.get("threat_level") == "CRITICAL" else "advanced",
        "resource_level": "organization",
        "primary_motivation": "personal-gain",
        "labels": ["crime-syndicate"] if org.get("org_type") == "CARTEL" else ["criminal"],
        "confidence": int((org.get("confidence", 0.8)) * 100),
        "created_by_ref": MDA_IDENTITY["id"],
    }


def interdiction_to_stix_sighting(event: dict) -> dict | None:
    """Convert an interdiction event to a STIX 2.1 Sighting."""
    event_id = event.get("event_id", "")
    if not event_id:
        return None

    return {
        "type": "sighting",
        "spec_version": "2.1",
        "id": _stix_id("sighting", event_id),
        "created": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "modified": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "first_seen": event.get("event_time", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")),
        "last_seen": event.get("event_time", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")),
        "count": 1,
        "description": (
            f"Interdiction event: {event.get('event_type', 'N/A')} by {event.get('lead_agency', 'N/A')}. "
            f"Cargo: {event.get('cargo_type', 'N/A')} ({event.get('cargo_quantity_kg', 'N/A')} kg). "
            f"Location: ({event.get('lat', 'N/A')}, {event.get('lon', 'N/A')})."
        ),
        "confidence": int((event.get("confidence", 0.9)) * 100),
        "created_by_ref": MDA_IDENTITY["id"],
    }


def export_stix_bundle(
    include_vessels: bool = True,
    include_persons: bool = True,
    include_orgs: bool = True,
    include_interdictions: bool = True,
    output_path: str | None = None,
) -> dict:
    """Export MDA entities as a STIX 2.1 Bundle.

    Returns the bundle dict. Optionally writes to file.
    """
    mg = Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)

    objects = [MDA_IDENTITY]

    if include_vessels:
        vessels = list(mg.execute_and_fetch(
            "MATCH (v:Vessel) WHERE v.sanctions_status = 'SANCTIONED' RETURN v"
        ))
        for r in vessels:
            v = dict(r["v"]) if hasattr(r["v"], "__iter__") else {}
            stix_obj = vessel_to_stix_indicator(v)
            if stix_obj:
                objects.append(stix_obj)
        logger.info("Exported %d sanctioned vessels", len(vessels))

    if include_persons:
        persons = list(mg.execute_and_fetch(
            "MATCH (p:Person) WHERE p.sanctions_status = 'SANCTIONED' RETURN p"
        ))
        for r in persons:
            p = dict(r["p"]) if hasattr(r["p"], "__iter__") else {}
            stix_obj = person_to_stix_threat_actor(p)
            if stix_obj:
                objects.append(stix_obj)
        logger.info("Exported %d sanctioned persons", len(persons))

    if include_orgs:
        orgs = list(mg.execute_and_fetch(
            "MATCH (o:Organization) WHERE o.sanctions_status = 'SANCTIONED' OR o.threat_level IN ['CRITICAL', 'HIGH'] RETURN o"
        ))
        for r in orgs:
            o = dict(r["o"]) if hasattr(r["o"], "__iter__") else {}
            stix_obj = org_to_stix_threat_actor(o)
            if stix_obj:
                objects.append(stix_obj)
        logger.info("Exported %d organizations", len(orgs))

    if include_interdictions:
        events = list(mg.execute_and_fetch(
            "MATCH (e:Interdiction_Event) RETURN e LIMIT 500"
        ))
        for r in events:
            e = dict(r["e"]) if hasattr(r["e"], "__iter__") else {}
            stix_obj = interdiction_to_stix_sighting(e)
            if stix_obj:
                objects.append(stix_obj)
        logger.info("Exported %d interdiction events", len(events))

    bundle = {
        "type": "bundle",
        "id": f"bundle--{uuid.uuid4()}",
        "objects": objects,
    }

    if output_path:
        with open(output_path, "w") as f:
            json.dump(bundle, f, indent=2)
        logger.info("STIX bundle written to %s (%d objects)", output_path, len(objects))

    return bundle


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    bundle = export_stix_bundle(output_path="/data/exports/mda_stix_bundle.json")
    print(f"Exported STIX 2.1 bundle with {len(bundle['objects'])} objects")
