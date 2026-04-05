"""Memgraph native Kafka stream transformation module.

Deployed to Memgraph via MAGE Python API. Converts raw Kafka JSON messages
into Cypher MERGE statements executed directly in-graph, bypassing the
external consumer for maximum throughput.

Usage:
    1. Copy this file into the Memgraph query modules directory
    2. Run: CALL mg.load_all();
    3. Create stream:
        CREATE KAFKA STREAM ais_positions_stream
        TOPICS mda.ais.positions.raw
        TRANSFORM memgraph_transform.ais_position
        BOOTSTRAP_SERVERS "redpanda:9092"
        BATCH_INTERVAL 100
        BATCH_SIZE 1000;
    4. Start: START STREAM ais_positions_stream;
"""

import json
import mgp


@mgp.transformation
def ais_position(messages: mgp.Messages) -> mgp.Record(query=str, parameters=mgp.Map):
    """Transform raw AIS position JSON into vessel upsert Cypher."""
    results = []

    for i in range(messages.total_messages()):
        msg = messages.message_at(i)
        try:
            payload = msg.payload().decode("utf-8")
            pos = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        mmsi = pos.get("mmsi")
        if not mmsi or len(str(mmsi)) != 9:
            continue

        cypher = """
        MERGE (v:Vessel {mmsi: $mmsi})
        ON CREATE SET
            v.name = $name,
            v.imo = $imo,
            v.ais_status = 'ACTIVE',
            v.risk_score = 0.0,
            v.valid_from = localDateTime(),
            v.system_created_at = localDateTime(),
            v.source_ids = [$source],
            v.confidence = 0.9
        SET v.last_ais_position_lat = $lat,
            v.last_ais_position_lon = $lon,
            v.last_ais_timestamp = $ts,
            v.last_ais_speed_kts = $speed,
            v.last_ais_heading = $heading,
            v.navigational_status = $nav_status,
            v.ais_status = 'ACTIVE',
            v.system_updated_at = localDateTime()
        """

        params = {
            "mmsi": str(mmsi),
            "imo": str(pos.get("imo", "")) or None,
            "name": str(pos.get("vessel_name", "")) or None,
            "lat": float(pos.get("lat", 0)),
            "lon": float(pos.get("lon", 0)),
            "ts": str(pos.get("timestamp", "")),
            "speed": float(pos.get("speed_kts", 0) or 0),
            "heading": int(pos.get("heading", 511) or 511),
            "nav_status": int(pos.get("nav_status", 15) or 15),
            "source": str(pos.get("source", "unknown")),
        }

        results.append(mgp.Record(query=cypher, parameters=params))

    return results


@mgp.transformation
def sanctions_update(messages: mgp.Messages) -> mgp.Record(query=str, parameters=mgp.Map):
    """Transform sanctions update JSON into entity upsert Cypher."""
    results = []

    for i in range(messages.total_messages()):
        msg = messages.message_at(i)
        try:
            payload = msg.payload().decode("utf-8")
            entity = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        entity_type = entity.get("entity_type", "")

        if entity_type == "vessel":
            imo = entity.get("imo")
            mmsi = entity.get("mmsi")
            if not imo and not mmsi:
                continue

            if imo:
                cypher = """
                MERGE (v:Vessel {imo: $imo})
                ON CREATE SET
                    v.mmsi = $mmsi,
                    v.name = $name,
                    v.system_created_at = localDateTime(),
                    v.valid_from = localDateTime(),
                    v.risk_score = 4.0
                SET v.sanctions_status = 'SANCTIONED',
                    v.ofac_programs = $programs,
                    v.source_ids = $sources,
                    v.system_updated_at = localDateTime()
                """
            else:
                cypher = """
                MERGE (v:Vessel {mmsi: $mmsi})
                ON CREATE SET
                    v.name = $name,
                    v.system_created_at = localDateTime(),
                    v.valid_from = localDateTime(),
                    v.risk_score = 4.0
                SET v.sanctions_status = 'SANCTIONED',
                    v.ofac_programs = $programs,
                    v.source_ids = $sources,
                    v.system_updated_at = localDateTime()
                """

            results.append(mgp.Record(
                query=cypher,
                parameters={
                    "imo": imo,
                    "mmsi": mmsi,
                    "name": entity.get("name"),
                    "programs": entity.get("ofac_programs", []),
                    "sources": [entity.get("source", "unknown")],
                },
            ))

        elif entity_type == "person":
            cypher = """
            MERGE (p:Person {entity_id: $id})
            ON CREATE SET
                p.system_created_at = localDateTime(),
                p.valid_from = localDateTime()
            SET p.name_full = $name,
                p.aliases = $aliases,
                p.dob = $dob,
                p.nationality = $nationality,
                p.sanctions_status = 'SANCTIONED',
                p.ofac_programs = $programs,
                p.source_ids = $sources,
                p.system_updated_at = localDateTime()
            """
            results.append(mgp.Record(
                query=cypher,
                parameters={
                    "id": entity["entity_id"],
                    "name": entity.get("name_full") or entity.get("name_last", ""),
                    "aliases": entity.get("aliases", []),
                    "dob": entity.get("dob"),
                    "nationality": entity.get("nationality"),
                    "programs": entity.get("ofac_programs", []),
                    "sources": [entity.get("source", "unknown")],
                },
            ))

        elif entity_type == "organization":
            cypher = """
            MERGE (o:Organization {entity_id: $id})
            ON CREATE SET
                o.system_created_at = localDateTime(),
                o.valid_from = localDateTime()
            SET o.name = $name,
                o.aliases = $aliases,
                o.sanctions_status = 'SANCTIONED',
                o.ofac_programs = $programs,
                o.source_ids = $sources,
                o.system_updated_at = localDateTime()
            """
            results.append(mgp.Record(
                query=cypher,
                parameters={
                    "id": entity["entity_id"],
                    "name": entity.get("name"),
                    "aliases": entity.get("aliases", []),
                    "programs": entity.get("ofac_programs", []),
                    "sources": [entity.get("source", "unknown")],
                },
            ))

    return results


@mgp.transformation
def uas_detection(messages: mgp.Messages) -> mgp.Record(query=str, parameters=mgp.Map):
    """Transform UAS detection JSON into detection event upsert Cypher."""
    results = []

    for i in range(messages.total_messages()):
        msg = messages.message_at(i)
        try:
            payload = msg.payload().decode("utf-8")
            det = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        event_id = det.get("event_id")
        if not event_id:
            continue

        cypher = """
        MERGE (e:UAS_Detection_Event {event_id: $event_id})
        ON CREATE SET
            e.detection_timestamp = $ts,
            e.detection_lat = $lat,
            e.detection_lon = $lon,
            e.detection_alt_m = $alt,
            e.sensor_type = $sensor_type,
            e.detection_method = $method,
            e.detection_confidence = $confidence,
            e.uas_classification = $classification,
            e.source_ids = [$source],
            e.system_created_at = localDateTime(),
            e.valid_from = $ts
        """

        results.append(mgp.Record(
            query=cypher,
            parameters={
                "event_id": event_id,
                "ts": det.get("detection_timestamp"),
                "lat": float(det.get("detection_lat", 0)),
                "lon": float(det.get("detection_lon", 0)),
                "alt": float(det.get("detection_alt_m", 0)),
                "sensor_type": det.get("sensor_type", "UNKNOWN"),
                "method": det.get("detection_method", "UNKNOWN"),
                "confidence": float(det.get("detection_confidence", 0.5)),
                "classification": det.get("uas_classification", "UNKNOWN"),
                "source": det.get("source", "unknown"),
            },
        ))

    return results
