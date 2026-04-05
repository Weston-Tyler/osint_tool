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
                pass  # TODO: implement encounter upsert
            elif topic == "mda.events.gdelt.raw":
                pass  # TODO: implement GDELT event processing
            elif topic == "mda.interdictions.new":
                pass  # TODO: implement interdiction upsert
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
