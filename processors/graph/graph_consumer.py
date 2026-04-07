"""Graph processor — consumes Kafka topics and writes to Memgraph + PostGIS.

Listens to processed data topics and upserts entities and relationships
into Memgraph (graph store) and PostGIS (spatial store).
"""

import json
import logging
import os
from datetime import datetime

import asyncpg
from gqlalchemy import Memgraph
from kafka import KafkaConsumer

logger = logging.getLogger("mda.processor.graph")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://mda:mda@localhost:5432/mda")

# Topics to consume
TOPICS = [
    "mda.ais.positions.raw",
    "mda.sanctions.updates",
    "mda.ais.gaps.detected",
    "mda.ais.encounters.detected",
    "mda.uas.detections.raw",
    "mda.events.gdelt.raw",
    "mda.interdictions.new",
    # GFW v3 — full-coverage ingester topics (see workers/ais/gfw_full_ingester.py)
    "mda.gfw.encounters",
    "mda.gfw.loitering",
    "mda.gfw.port_visits",
    "mda.gfw.fishing",
    "mda.gfw.gaps",
    "mda.gfw.vessels",
    "mda.gfw.insights",
    "mda.gfw.sar_detections",
]


class GraphProcessor:
    """Consumes Kafka events and writes to Memgraph and PostGIS."""

    def __init__(self):
        self.memgraph = Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
        self.pg_pool = None

    async def init_postgres(self):
        self.pg_pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=5, max_size=20)

    def upsert_vessel_position(self, msg: dict):
        """Update vessel node with latest AIS position."""
        mmsi = msg.get("mmsi")
        if not mmsi:
            return

        self.memgraph.execute(
            """
            MERGE (v:Vessel {mmsi: $mmsi})
            ON CREATE SET
                v.name = $name,
                v.imo = $imo,
                v.ais_status = 'ACTIVE',
                v.valid_from = localDateTime(),
                v.system_created_at = localDateTime(),
                v.source_ids = [$source],
                v.confidence = 0.9,
                v.risk_score = 0.0
            SET v.last_ais_position_lat = $lat,
                v.last_ais_position_lon = $lon,
                v.last_ais_timestamp = $ts,
                v.last_ais_speed_kts = $speed,
                v.last_ais_heading = $heading,
                v.navigational_status = $nav_status,
                v.system_updated_at = localDateTime()
            """,
            {
                "mmsi": mmsi,
                "imo": msg.get("imo") or None,
                "name": msg.get("vessel_name") or None,
                "lat": msg.get("lat"),
                "lon": msg.get("lon"),
                "ts": msg.get("timestamp"),
                "speed": msg.get("speed_kts", 0.0),
                "heading": msg.get("heading", 511),
                "nav_status": msg.get("nav_status", 15),
                "source": msg.get("source", "unknown"),
            },
        )

    async def insert_ais_position_postgis(self, msg: dict):
        """Insert AIS position into PostGIS for spatial analysis."""
        if not self.pg_pool:
            return
        async with self.pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO ais_positions (mmsi, imo, valid_time, position, speed_kts,
                    heading, course, nav_status, vessel_name, vessel_type, source)
                VALUES ($1, $2, $3, ST_SetSRID(ST_MakePoint($4, $5), 4326),
                    $6, $7, $8, $9, $10, $11, $12)
                """,
                msg.get("mmsi"),
                msg.get("imo") or None,
                datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00")),
                msg.get("lon", 0),
                msg.get("lat", 0),
                msg.get("speed_kts", 0),
                msg.get("heading", 511),
                msg.get("course", 0),
                msg.get("nav_status", 15),
                msg.get("vessel_name"),
                msg.get("vessel_type"),
                msg.get("source", "unknown"),
            )

    def upsert_sanctions_entity(self, msg: dict):
        """Upsert a sanctions entity (vessel, person, or org) into Memgraph."""
        entity_type = msg.get("entity_type", "")

        if entity_type == "vessel":
            self._upsert_sanctioned_vessel(msg)
        elif entity_type == "person":
            self._upsert_sanctioned_person(msg)
        elif entity_type == "organization":
            self._upsert_sanctioned_org(msg)

    def _upsert_sanctioned_vessel(self, msg: dict):
        imo = msg.get("imo")
        mmsi = msg.get("mmsi")
        if not imo and not mmsi:
            return

        if imo:
            self.memgraph.execute(
                """
                MERGE (v:Vessel {imo: $imo})
                ON CREATE SET
                    v.mmsi = $mmsi,
                    v.name = $name,
                    v.system_created_at = localDateTime(),
                    v.valid_from = localDateTime()
                SET v.sanctions_status = 'SANCTIONED',
                    v.ofac_programs = $programs,
                    v.source_ids = $sources,
                    v.system_updated_at = localDateTime()
                """,
                {
                    "imo": imo,
                    "mmsi": mmsi,
                    "name": msg.get("name"),
                    "programs": msg.get("ofac_programs", []),
                    "sources": [msg.get("source", "unknown")],
                },
            )
        elif mmsi:
            self.memgraph.execute(
                """
                MERGE (v:Vessel {mmsi: $mmsi})
                ON CREATE SET
                    v.name = $name,
                    v.system_created_at = localDateTime(),
                    v.valid_from = localDateTime()
                SET v.sanctions_status = 'SANCTIONED',
                    v.ofac_programs = $programs,
                    v.source_ids = $sources,
                    v.system_updated_at = localDateTime()
                """,
                {
                    "mmsi": mmsi,
                    "name": msg.get("name"),
                    "programs": msg.get("ofac_programs", []),
                    "sources": [msg.get("source", "unknown")],
                },
            )

    def _upsert_sanctioned_person(self, msg: dict):
        self.memgraph.execute(
            """
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
            """,
            {
                "id": msg["entity_id"],
                "name": msg.get("name_full") or msg.get("name_last", ""),
                "aliases": msg.get("aliases", []),
                "dob": msg.get("dob"),
                "nationality": msg.get("nationality"),
                "programs": msg.get("ofac_programs", []),
                "sources": [msg.get("source", "unknown")],
            },
        )

    def _upsert_sanctioned_org(self, msg: dict):
        self.memgraph.execute(
            """
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
            """,
            {
                "id": msg["entity_id"],
                "name": msg.get("name"),
                "aliases": msg.get("aliases", []),
                "programs": msg.get("ofac_programs", []),
                "sources": [msg.get("source", "unknown")],
            },
        )

    def upsert_ais_gap(self, msg: dict):
        """Insert AIS gap event and link to vessel."""
        self.memgraph.execute(
            """
            MERGE (g:AIS_Gap_Event {event_id: $event_id})
            ON CREATE SET
                g.vessel_mmsi = $mmsi,
                g.gap_start_time = $start,
                g.gap_end_time = $end,
                g.gap_duration_hours = $duration,
                g.last_position_lat = $last_lat,
                g.last_position_lon = $last_lon,
                g.resume_position_lat = $resume_lat,
                g.resume_position_lon = $resume_lon,
                g.displacement_km = $displacement,
                g.probable_cause = $cause,
                g.risk_flag = $risk,
                g.source_ids = [$source],
                g.confidence = $confidence,
                g.system_created_at = localDateTime(),
                g.valid_from = $start
            WITH g
            MATCH (v:Vessel {mmsi: $mmsi})
            MERGE (v)-[:HAS_AIS_GAP]->(g)
            """,
            {
                "event_id": msg["event_id"],
                "mmsi": msg["vessel_mmsi"],
                "start": msg["gap_start_time"],
                "end": msg["gap_end_time"],
                "duration": msg["gap_duration_hours"],
                "last_lat": msg.get("last_position_lat", 0),
                "last_lon": msg.get("last_position_lon", 0),
                "resume_lat": msg.get("resume_position_lat", 0),
                "resume_lon": msg.get("resume_position_lon", 0),
                "displacement": msg.get("displacement_km", 0),
                "cause": msg.get("probable_cause", "UNKNOWN"),
                "risk": msg.get("risk_flag", False),
                "source": msg.get("source", "unknown"),
                "confidence": msg.get("confidence", 0.7),
            },
        )

    def upsert_uas_detection(self, msg: dict):
        """Insert UAS detection event into Memgraph."""
        self.memgraph.execute(
            """
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
            """,
            {
                "event_id": msg["event_id"],
                "ts": msg.get("detection_timestamp"),
                "lat": msg.get("detection_lat", 0),
                "lon": msg.get("detection_lon", 0),
                "alt": msg.get("detection_alt_m", 0),
                "sensor_type": msg.get("sensor_type", "UNKNOWN"),
                "method": msg.get("detection_method", "UNKNOWN"),
                "confidence": msg.get("detection_confidence", 0.5),
                "classification": msg.get("uas_classification", "UNKNOWN"),
                "source": msg.get("source", "unknown"),
            },
        )

    # ── GFW v3 handlers ─────────────────────────────────────────────
    def upsert_gfw_vessel(self, msg: dict):
        """Upsert a Vessel node from a GFW vessel-identity record."""
        vid = msg.get("vessel_id")
        mmsi = msg.get("mmsi")
        imo = msg.get("imo")
        if not vid and not mmsi and not imo:
            return
        # Prefer MMSI as the merge key when available, fall back to vessel_id
        merge_key, merge_val = ("mmsi", mmsi) if mmsi else ("gfw_vessel_id", vid)
        self.memgraph.execute(
            f"""
            MERGE (v:Vessel {{{merge_key}: $val}})
            ON CREATE SET
                v.system_created_at = localDateTime(),
                v.source_ids = ['global_fishing_watch'],
                v.confidence = 0.9
            SET v.gfw_vessel_id = $vid,
                v.imo = coalesce($imo, v.imo),
                v.callsign = coalesce($callsign, v.callsign),
                v.name = coalesce($name, v.name),
                v.flag = coalesce($flag, v.flag),
                v.vessel_type = coalesce($vtype, v.vessel_type),
                v.owner_name = coalesce($owner_name, v.owner_name),
                v.owner_flag = coalesce($owner_flag, v.owner_flag),
                v.first_seen = coalesce($first_seen, v.first_seen),
                v.last_seen = coalesce($last_seen, v.last_seen),
                v.system_updated_at = localDateTime()
            """,
            {
                "val": merge_val,
                "vid": vid,
                "imo": imo,
                "callsign": msg.get("callsign"),
                "name": msg.get("name"),
                "flag": msg.get("flag"),
                "vtype": msg.get("vessel_type"),
                "owner_name": msg.get("owner_name"),
                "owner_flag": msg.get("owner_flag"),
                "first_seen": msg.get("first_seen"),
                "last_seen": msg.get("last_seen"),
            },
        )

    def upsert_gfw_event(self, msg: dict, label: str, rel_type: str):
        """Generic GFW event upsert: creates an event node linked to its vessel."""
        eid = msg.get("event_id")
        if not eid:
            return
        vid = msg.get("vessel_id")
        mmsi = msg.get("vessel_mmsi")
        # Build the event node + relationship to the vessel
        merge_key, merge_val = ("mmsi", mmsi) if mmsi else ("gfw_vessel_id", vid)
        if not merge_val:
            return
        self.memgraph.execute(
            f"""
            MERGE (e:{label} {{event_id: $eid}})
            ON CREATE SET
                e.source = 'global_fishing_watch',
                e.start_time = $start_time,
                e.end_time = $end_time,
                e.lat = $lat,
                e.lon = $lon,
                e.encounter_type = $enc_type,
                e.median_distance_km = $dist_km,
                e.median_speed_knots = $speed,
                e.port_id = $port_id,
                e.port_name = $port_name,
                e.port_country = $port_country,
                e.gap_hours = $gap_hours,
                e.system_created_at = localDateTime()
            WITH e
            MERGE (v:Vessel {{{merge_key}: $merge_val}})
            ON CREATE SET v.system_created_at = localDateTime(),
                          v.source_ids = ['global_fishing_watch']
            MERGE (v)-[:{rel_type}]->(e)
            """,
            {
                "eid": eid,
                "start_time": msg.get("start_time"),
                "end_time": msg.get("end_time"),
                "lat": msg.get("lat"),
                "lon": msg.get("lon"),
                "enc_type": msg.get("encounter_type"),
                "dist_km": msg.get("median_distance_km"),
                "speed": msg.get("median_speed_knots"),
                "port_id": msg.get("port_id"),
                "port_name": msg.get("port_name"),
                "port_country": msg.get("port_country"),
                "gap_hours": msg.get("gap_hours"),
                "merge_val": merge_val,
            },
        )
        # If it's an encounter, also link the encountered vessel
        ev_id = msg.get("encountered_vessel_id")
        ev_mmsi = msg.get("encountered_vessel_mmsi")
        if label == "Encounter" and (ev_id or ev_mmsi):
            ev_key, ev_val = ("mmsi", ev_mmsi) if ev_mmsi else ("gfw_vessel_id", ev_id)
            self.memgraph.execute(
                f"""
                MATCH (e:Encounter {{event_id: $eid}})
                MERGE (v2:Vessel {{{ev_key}: $val}})
                ON CREATE SET v2.system_created_at = localDateTime(),
                              v2.source_ids = ['global_fishing_watch']
                MERGE (v2)-[:ENCOUNTERED]->(e)
                """,
                {"eid": eid, "val": ev_val},
            )

    def upsert_gfw_insight(self, msg: dict):
        """Attach IUU/risk insight properties to a vessel."""
        vid = msg.get("vessel_id")
        if not vid:
            return
        self.memgraph.execute(
            """
            MERGE (v:Vessel {gfw_vessel_id: $vid})
            ON CREATE SET v.system_created_at = localDateTime(),
                          v.source_ids = ['global_fishing_watch']
            SET v.iuu_listed = $iuu,
                v.gfw_fishing_hours = $fhrs,
                v.gfw_gap_hours = $ghrs,
                v.gfw_coverage_pct = $cov,
                v.system_updated_at = localDateTime()
            """,
            {
                "vid": vid,
                "iuu": bool(msg.get("iuu_listed")),
                "fhrs": msg.get("fishing_hours"),
                "ghrs": msg.get("gap_hours"),
                "cov": msg.get("coverage_pct"),
            },
        )

    def process_message(self, topic: str, msg: dict):
        """Route a Kafka message to the appropriate handler."""
        try:
            if topic == "mda.ais.positions.raw":
                self.upsert_vessel_position(msg)
            elif topic == "mda.sanctions.updates":
                self.upsert_sanctions_entity(msg)
            elif topic == "mda.ais.gaps.detected":
                self.upsert_ais_gap(msg)
            elif topic == "mda.uas.detections.raw":
                self.upsert_uas_detection(msg)
            elif topic == "mda.ais.encounters.detected":
                pass  # legacy raw HTTP ingester — superseded by GFW topics below
            elif topic == "mda.events.gdelt.raw":
                pass  # TODO: implement GDELT event processing
            elif topic == "mda.interdictions.new":
                pass  # TODO: implement interdiction upsert
            # ── GFW v3 ──────────────────────────────────────────────
            elif topic == "mda.gfw.encounters":
                self.upsert_gfw_event(msg, "Encounter", "INVOLVED_IN")
            elif topic == "mda.gfw.loitering":
                self.upsert_gfw_event(msg, "LoiteringEvent", "LOITERED_AT")
            elif topic == "mda.gfw.port_visits":
                self.upsert_gfw_event(msg, "PortVisit", "VISITED")
            elif topic == "mda.gfw.fishing":
                self.upsert_gfw_event(msg, "FishingEvent", "FISHED_AT")
            elif topic == "mda.gfw.gaps":
                self.upsert_gfw_event(msg, "AISGap", "WENT_DARK_AT")
            elif topic == "mda.gfw.vessels":
                self.upsert_gfw_vessel(msg)
            elif topic == "mda.gfw.insights":
                self.upsert_gfw_insight(msg)
            elif topic == "mda.gfw.sar_detections":
                pass  # SAR detections come without vessel IDs; geographic only
        except Exception as e:
            logger.error("Error processing message from %s: %s", topic, e, exc_info=True)


def run():
    """Main consumer loop."""
    processor = GraphProcessor()

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="mda-graph-processor",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        max_poll_records=500,
    )

    logger.info("Graph processor started, consuming topics: %s", TOPICS)

    for msg in consumer:
        processor.process_message(msg.topic, msg.value)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    run()
