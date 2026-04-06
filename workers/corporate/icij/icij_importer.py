"""
ICIJ Offshore Leaks data importer for MDA Corporate Ownership Graph.

Parses the ICIJ Offshore Leaks CSV dumps (entities, officers, relationships),
maps jurisdictions to ISO-3166 alpha-2 codes, and loads everything into
Memgraph as Company (shell) / Person nodes with ICIJ_CONNECTED_TO and
SAME_AS edges.

Source: https://offshoreleaks.icij.org/pages/database
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

from confluent_kafka import Producer
from mgclient import connect as mg_connect

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = "mda.corporate.icij.raw"
MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "memgraph")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))

# ---------------------------------------------------------------------------
# Jurisdiction mapping (ICIJ labels -> ISO-3166 alpha-2)
# ---------------------------------------------------------------------------

JURISDICTION_MAP: Dict[str, str] = {
    "BVI": "VG",
    "British Virgin Islands": "VG",
    "Cayman": "KY",
    "Cayman Islands": "KY",
    "Panama": "PA",
    "Seychelles": "SC",
    "Bahamas": "BS",
    "Belize": "BZ",
    "Marshall Islands": "MH",
    "Samoa": "WS",
    "Isle of Man": "IM",
    "Jersey": "JE",
    "Malta": "MT",
    "Cyprus": "CY",
    "Singapore": "SG",
    "Hong Kong": "HK",
    "Mauritius": "MU",
    # Pass-through for already-coded values
    "VG": "VG",
    "KY": "KY",
    "PA": "PA",
    "SC": "SC",
    "BS": "BS",
    "BZ": "BZ",
    "MH": "MH",
    "WS": "WS",
    "IM": "IM",
    "JE": "JE",
    "MT": "MT",
    "CY": "CY",
    "SG": "SG",
    "HK": "HK",
    "MU": "MU",
}


def normalize_jurisdiction(raw: str) -> str:
    """Map an ICIJ jurisdiction label to ISO-3166 alpha-2 code."""
    if not raw:
        return ""
    stripped = raw.strip()
    return JURISDICTION_MAP.get(stripped, stripped)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ICIJEntity:
    node_id: str
    name: str
    jurisdiction: str = ""
    jurisdiction_description: str = ""
    country_codes: str = ""
    incorporation_date: str = ""
    inactivation_date: str = ""
    struck_off_date: str = ""
    status: str = ""
    service_provider: str = ""
    source_id: str = ""
    address: str = ""
    internal_id: str = ""
    valid_until: str = ""
    note: str = ""


@dataclass
class ICIJOfficer:
    node_id: str
    name: str
    country_codes: str = ""
    source_id: str = ""
    valid_until: str = ""
    note: str = ""


@dataclass
class ICIJRelationship:
    node_id_start: str
    node_id_end: str
    rel_type: str
    link: str = ""
    source_id: str = ""
    valid_until: str = ""
    start_date: str = ""
    end_date: str = ""


# ---------------------------------------------------------------------------
# CSV Parser
# ---------------------------------------------------------------------------


class ICIJCSVParser:
    """Parses ICIJ Offshore Leaks CSV dump files."""

    def __init__(self, encoding: str = "utf-8-sig"):
        self.encoding = encoding

    def parse_entities(self, path: str) -> Generator[ICIJEntity, None, None]:
        """Parse nodes-entities.csv into ICIJEntity objects."""
        logger.info("Parsing ICIJ entities from %s", path)
        count = 0
        with open(path, "r", encoding=self.encoding, errors="replace") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                entity = ICIJEntity(
                    node_id=row.get("node_id", "").strip(),
                    name=row.get("name", "").strip(),
                    jurisdiction=normalize_jurisdiction(
                        row.get("jurisdiction", "")
                    ),
                    jurisdiction_description=row.get(
                        "jurisdiction_description", ""
                    ).strip(),
                    country_codes=row.get("country_codes", "").strip(),
                    incorporation_date=row.get("incorporation_date", "").strip(),
                    inactivation_date=row.get("inactivation_date", "").strip(),
                    struck_off_date=row.get("struck_off_date", "").strip(),
                    status=row.get("status", "").strip(),
                    service_provider=row.get("service_provider", "").strip(),
                    source_id=row.get("sourceID", "").strip(),
                    address=row.get("address", "").strip(),
                    internal_id=row.get("internal_id", "").strip(),
                    valid_until=row.get("valid_until", "").strip(),
                    note=row.get("note", "").strip(),
                )
                if entity.node_id:
                    count += 1
                    yield entity
        logger.info("Parsed %d ICIJ entities", count)

    def parse_officers(self, path: str) -> Generator[ICIJOfficer, None, None]:
        """Parse nodes-officers.csv into ICIJOfficer objects."""
        logger.info("Parsing ICIJ officers from %s", path)
        count = 0
        with open(path, "r", encoding=self.encoding, errors="replace") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                officer = ICIJOfficer(
                    node_id=row.get("node_id", "").strip(),
                    name=row.get("name", "").strip(),
                    country_codes=row.get("country_codes", "").strip(),
                    source_id=row.get("sourceID", "").strip(),
                    valid_until=row.get("valid_until", "").strip(),
                    note=row.get("note", "").strip(),
                )
                if officer.node_id:
                    count += 1
                    yield officer
        logger.info("Parsed %d ICIJ officers", count)

    def parse_relationships(
        self, path: str
    ) -> Generator[ICIJRelationship, None, None]:
        """Parse relationships.csv into ICIJRelationship objects."""
        logger.info("Parsing ICIJ relationships from %s", path)
        count = 0
        with open(path, "r", encoding=self.encoding, errors="replace") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rel = ICIJRelationship(
                    node_id_start=row.get("node_id_start", "").strip(),
                    node_id_end=row.get("node_id_end", "").strip(),
                    rel_type=row.get("rel_type", "").strip(),
                    link=row.get("link", "").strip(),
                    source_id=row.get("sourceID", "").strip(),
                    valid_until=row.get("valid_until", "").strip(),
                    start_date=row.get("start_date", "").strip(),
                    end_date=row.get("end_date", "").strip(),
                )
                if rel.node_id_start and rel.node_id_end:
                    count += 1
                    yield rel
        logger.info("Parsed %d ICIJ relationships", count)


# ---------------------------------------------------------------------------
# Kafka Producer
# ---------------------------------------------------------------------------


class ICIJKafkaProducer:
    """Sends ICIJ records to Kafka."""

    def __init__(self, broker: str = KAFKA_BROKER, topic: str = KAFKA_TOPIC):
        self.topic = topic
        self._producer = Producer({"bootstrap.servers": broker})
        self._count = 0

    def _delivery_cb(self, err, msg):
        if err:
            logger.error("Kafka delivery failed: %s", err)

    def send_entity(self, entity: ICIJEntity) -> None:
        payload = json.dumps(
            {"type": "entity", "node_id": entity.node_id, "name": entity.name,
             "jurisdiction": entity.jurisdiction, "status": entity.status,
             "source_id": entity.source_id},
            default=str,
        ).encode()
        self._producer.produce(
            self.topic, key=entity.node_id.encode(), value=payload,
            callback=self._delivery_cb,
        )
        self._count += 1
        if self._count % 10_000 == 0:
            self._producer.poll(0)

    def send_officer(self, officer: ICIJOfficer) -> None:
        payload = json.dumps(
            {"type": "officer", "node_id": officer.node_id, "name": officer.name,
             "country_codes": officer.country_codes, "source_id": officer.source_id},
            default=str,
        ).encode()
        self._producer.produce(
            self.topic, key=officer.node_id.encode(), value=payload,
            callback=self._delivery_cb,
        )
        self._count += 1
        if self._count % 10_000 == 0:
            self._producer.poll(0)

    def send_relationship(self, rel: ICIJRelationship) -> None:
        payload = json.dumps(
            {"type": "relationship", "start": rel.node_id_start,
             "end": rel.node_id_end, "rel_type": rel.rel_type,
             "source_id": rel.source_id},
            default=str,
        ).encode()
        key = f"{rel.node_id_start}:{rel.node_id_end}".encode()
        self._producer.produce(
            self.topic, key=key, value=payload, callback=self._delivery_cb,
        )
        self._count += 1
        if self._count % 10_000 == 0:
            self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush(timeout=30)
        logger.info("Kafka flush complete, sent %d messages", self._count)


# ---------------------------------------------------------------------------
# Memgraph Loader
# ---------------------------------------------------------------------------

# Relationship types that indicate same-entity links
SAME_AS_LINK_TYPES: Set[str] = {"same_id_as", "same_name_as", "same name and registration date as"}
DEFAULT_SHELL_RISK_SCORE = 8.0


class ICIJMemgraphLoader:
    """Batch-loads ICIJ data into Memgraph with MERGE semantics."""

    def __init__(
        self,
        host: str = MEMGRAPH_HOST,
        port: int = MEMGRAPH_PORT,
        batch_size: int = BATCH_SIZE,
    ):
        self.host = host
        self.port = port
        self.batch_size = batch_size
        self._conn = None

    def _connect(self):
        if self._conn is None:
            self._conn = mg_connect(host=self.host, port=self.port)
        return self._conn

    def _execute(self, query: str, params: Optional[Dict] = None) -> None:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(query, params or {})
        conn.commit()

    def _execute_batch(self, query: str, batch: List[Dict]) -> None:
        if not batch:
            return
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(query, {"batch": batch})
        conn.commit()

    def ensure_indexes(self) -> None:
        """Create constraints and indexes for ICIJ nodes."""
        index_cmds = [
            "CREATE INDEX ON :Company(icij_node_id)",
            "CREATE INDEX ON :Person(icij_node_id)",
            "CREATE INDEX ON :Company(name)",
            "CREATE INDEX ON :Person(name)",
        ]
        for cmd in index_cmds:
            try:
                self._execute(cmd)
            except Exception as exc:
                logger.debug("Index may already exist: %s", exc)

    def merge_entities(self, entities: List[ICIJEntity]) -> int:
        """MERGE ICIJ entities as Company nodes with is_shell_flag=true."""
        query = """
        UNWIND $batch AS e
        MERGE (c:Company {icij_node_id: e.node_id})
        SET c.name = e.name,
            c.jurisdiction = e.jurisdiction,
            c.jurisdiction_description = e.jurisdiction_description,
            c.country_codes = e.country_codes,
            c.incorporation_date = e.incorporation_date,
            c.status = e.status,
            c.service_provider = e.service_provider,
            c.address = e.address,
            c.source = 'icij_offshore_leaks',
            c.is_shell_flag = true,
            c.risk_score = $risk_score,
            c.updated_at = localDateTime()
        """
        total = 0
        batch: List[Dict] = []
        for entity in entities:
            batch.append({
                "node_id": entity.node_id,
                "name": entity.name,
                "jurisdiction": entity.jurisdiction,
                "jurisdiction_description": entity.jurisdiction_description,
                "country_codes": entity.country_codes,
                "incorporation_date": entity.incorporation_date,
                "status": entity.status,
                "service_provider": entity.service_provider,
                "address": entity.address,
            })
            if len(batch) >= self.batch_size:
                self._execute(query, {"batch": batch, "risk_score": DEFAULT_SHELL_RISK_SCORE})
                total += len(batch)
                batch.clear()
        if batch:
            self._execute(query, {"batch": batch, "risk_score": DEFAULT_SHELL_RISK_SCORE})
            total += len(batch)
        logger.info("Merged %d ICIJ entities as Company nodes", total)
        return total

    def merge_officers(self, officers: List[ICIJOfficer]) -> int:
        """MERGE ICIJ officers as Person nodes."""
        query = """
        UNWIND $batch AS o
        MERGE (p:Person {icij_node_id: o.node_id})
        SET p.name = o.name,
            p.country_codes = o.country_codes,
            p.source = 'icij_offshore_leaks',
            p.updated_at = localDateTime()
        """
        total = 0
        batch: List[Dict] = []
        for officer in officers:
            batch.append({
                "node_id": officer.node_id,
                "name": officer.name,
                "country_codes": officer.country_codes,
            })
            if len(batch) >= self.batch_size:
                self._execute(query, {"batch": batch})
                total += len(batch)
                batch.clear()
        if batch:
            self._execute(query, {"batch": batch})
            total += len(batch)
        logger.info("Merged %d ICIJ officers as Person nodes", total)
        return total

    def merge_relationships(self, relationships: List[ICIJRelationship]) -> int:
        """MERGE ICIJ relationships as edges.

        same_id_as / same_name_as relationships become SAME_AS edges.
        All other relationship types become ICIJ_CONNECTED_TO edges.
        """
        same_as_query = """
        UNWIND $batch AS r
        MATCH (a {icij_node_id: r.start_id})
        MATCH (b {icij_node_id: r.end_id})
        MERGE (a)-[:SAME_AS {link_type: r.rel_type, source: 'icij_offshore_leaks'}]->(b)
        """
        connected_query = """
        UNWIND $batch AS r
        MATCH (a {icij_node_id: r.start_id})
        MATCH (b {icij_node_id: r.end_id})
        MERGE (a)-[:ICIJ_CONNECTED_TO {
            link_type: r.rel_type,
            link: r.link,
            source: 'icij_offshore_leaks',
            start_date: r.start_date,
            end_date: r.end_date
        }]->(b)
        """
        same_batch: List[Dict] = []
        conn_batch: List[Dict] = []
        total = 0

        for rel in relationships:
            rec = {
                "start_id": rel.node_id_start,
                "end_id": rel.node_id_end,
                "rel_type": rel.rel_type,
                "link": rel.link,
                "start_date": rel.start_date,
                "end_date": rel.end_date,
            }
            if rel.rel_type.lower().replace(" ", "_") in SAME_AS_LINK_TYPES or \
               rel.rel_type.lower() in SAME_AS_LINK_TYPES:
                same_batch.append(rec)
            else:
                conn_batch.append(rec)

            if len(same_batch) >= self.batch_size:
                self._execute(same_as_query, {"batch": same_batch})
                total += len(same_batch)
                same_batch.clear()
            if len(conn_batch) >= self.batch_size:
                self._execute(connected_query, {"batch": conn_batch})
                total += len(conn_batch)
                conn_batch.clear()

        if same_batch:
            self._execute(same_as_query, {"batch": same_batch})
            total += len(same_batch)
        if conn_batch:
            self._execute(connected_query, {"batch": conn_batch})
            total += len(conn_batch)

        logger.info("Merged %d ICIJ relationships", total)
        return total

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def ingest_icij(
    entities_path: str,
    officers_path: str,
    relationships_path: str,
    write_kafka: bool = True,
    write_graph: bool = True,
) -> Dict[str, int]:
    """Full ICIJ ingest pipeline: parse CSVs, send to Kafka, load to Memgraph."""
    parser = ICIJCSVParser()
    kafka = ICIJKafkaProducer() if write_kafka else None
    graph = ICIJMemgraphLoader() if write_graph else None

    if graph:
        graph.ensure_indexes()

    counts: Dict[str, int] = {"entities": 0, "officers": 0, "relationships": 0}

    # --- Entities ---
    entity_buffer: List[ICIJEntity] = []
    for entity in parser.parse_entities(entities_path):
        if kafka:
            kafka.send_entity(entity)
        entity_buffer.append(entity)
        if len(entity_buffer) >= BATCH_SIZE:
            if graph:
                graph.merge_entities(entity_buffer)
            counts["entities"] += len(entity_buffer)
            entity_buffer.clear()
    if entity_buffer and graph:
        graph.merge_entities(entity_buffer)
    counts["entities"] += len(entity_buffer)

    # --- Officers ---
    officer_buffer: List[ICIJOfficer] = []
    for officer in parser.parse_officers(officers_path):
        if kafka:
            kafka.send_officer(officer)
        officer_buffer.append(officer)
        if len(officer_buffer) >= BATCH_SIZE:
            if graph:
                graph.merge_officers(officer_buffer)
            counts["officers"] += len(officer_buffer)
            officer_buffer.clear()
    if officer_buffer and graph:
        graph.merge_officers(officer_buffer)
    counts["officers"] += len(officer_buffer)

    # --- Relationships ---
    rel_buffer: List[ICIJRelationship] = []
    for rel in parser.parse_relationships(relationships_path):
        if kafka:
            kafka.send_relationship(rel)
        rel_buffer.append(rel)
        if len(rel_buffer) >= BATCH_SIZE:
            if graph:
                graph.merge_relationships(rel_buffer)
            counts["relationships"] += len(rel_buffer)
            rel_buffer.clear()
    if rel_buffer and graph:
        graph.merge_relationships(rel_buffer)
    counts["relationships"] += len(rel_buffer)

    if kafka:
        kafka.flush()
    if graph:
        graph.close()

    logger.info("ICIJ ingest complete: %s", counts)
    return counts


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    ap = argparse.ArgumentParser(description="ICIJ Offshore Leaks Importer")
    ap.add_argument("--entities", required=True, help="Path to nodes-entities.csv")
    ap.add_argument("--officers", required=True, help="Path to nodes-officers.csv")
    ap.add_argument("--relationships", required=True, help="Path to relationships.csv")
    ap.add_argument("--no-kafka", action="store_true")
    ap.add_argument("--no-graph", action="store_true")

    args = ap.parse_args()
    result = ingest_icij(
        entities_path=args.entities,
        officers_path=args.officers,
        relationships_path=args.relationships,
        write_kafka=not args.no_kafka,
        write_graph=not args.no_graph,
    )
    print(json.dumps(result, indent=2))
