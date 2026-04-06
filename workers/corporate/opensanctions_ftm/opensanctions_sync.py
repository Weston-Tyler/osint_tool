"""
OpenSanctions Follow The Money (FtM) data sync for MDA Corporate Ownership Graph.

Parses .ftm.json JSONL exports from OpenSanctions, filters relevant entity
schemas (Company, Organization, Person, Ownership, Directorship, Sanction),
and loads them into Memgraph via batch MERGE operations.

Source: https://www.opensanctions.org/datasets/
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

from followthemoney import model
from followthemoney.proxy import EntityProxy
from confluent_kafka import Producer
from mgclient import connect as mg_connect

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = "mda.corporate.opensanctions.raw"
MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "memgraph")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))

# Schemas we care about for the corporate ownership graph
RELEVANT_SCHEMAS: Set[str] = {
    "Company", "Organization", "Person",
    "Ownership", "Directorship", "Sanction",
}

# Priority datasets to ingest first (highest-value sources)
PRIORITY_DATASETS: List[str] = [
    "default",
    "eu_fsf",
    "us_ofac_sdn",
    "us_ofac_cons",
    "un_sc_sanctions",
    "gb_hmt_sanctions",
    "eu_cor_members",
    "ca_dfatd_sema_sanctions",
    "au_dfat_sanctions",
    "ch_seco_sanctions",
    "jp_mof_sanctions",
]


# ---------------------------------------------------------------------------
# FtM Entity Parser
# ---------------------------------------------------------------------------


class FtMEntityParser:
    """Parses .ftm.json JSONL files using the followthemoney library."""

    def __init__(self, schemas: Optional[Set[str]] = None):
        self.schemas = schemas or RELEVANT_SCHEMAS

    def parse_file(
        self, path: str
    ) -> Generator[EntityProxy, None, None]:
        """Parse a .ftm.json JSONL file, yielding EntityProxy objects.

        Only yields entities whose schema name is in self.schemas.
        """
        logger.info("Parsing FtM JSONL from %s", path)
        count = 0
        skipped = 0
        with open(path, "r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    entity = EntityProxy.from_dict(model, data)
                except Exception as exc:
                    logger.warning("Skipping malformed line %d: %s", line_no, exc)
                    skipped += 1
                    continue

                if entity.schema.name not in self.schemas:
                    skipped += 1
                    continue

                count += 1
                yield entity

        logger.info(
            "Parsed %d relevant FtM entities (%d skipped) from %s",
            count, skipped, path,
        )

    def parse_multiple(
        self, paths: List[str]
    ) -> Generator[EntityProxy, None, None]:
        """Parse multiple .ftm.json files in order."""
        for path in paths:
            yield from self.parse_file(path)


# ---------------------------------------------------------------------------
# Property extraction helpers
# ---------------------------------------------------------------------------


def _first(entity: EntityProxy, prop: str) -> str:
    """Get first value of a property or empty string."""
    vals = entity.get(prop)
    return vals[0] if vals else ""


def _all(entity: EntityProxy, prop: str) -> List[str]:
    """Get all values of a property."""
    return list(entity.get(prop))


def extract_company_props(entity: EntityProxy) -> Dict[str, Any]:
    """Extract Company/Organization properties for Memgraph."""
    return {
        "ftm_id": entity.id,
        "schema": entity.schema.name,
        "name": _first(entity, "name"),
        "aliases": _all(entity, "alias"),
        "jurisdiction": _first(entity, "jurisdiction"),
        "registration_number": _first(entity, "registrationNumber"),
        "incorporation_date": _first(entity, "incorporationDate"),
        "lei_code": _first(entity, "leiCode"),
        "opencorporates_url": _first(entity, "opencorporatesUrl"),
        "status": _first(entity, "status"),
        "country": _first(entity, "country"),
        "address": _first(entity, "address"),
    }


def extract_person_props(entity: EntityProxy) -> Dict[str, Any]:
    """Extract Person properties for Memgraph."""
    return {
        "ftm_id": entity.id,
        "schema": entity.schema.name,
        "name": _first(entity, "name"),
        "aliases": _all(entity, "alias"),
        "birth_date": _first(entity, "birthDate"),
        "nationality": _first(entity, "nationality"),
        "country": _first(entity, "country"),
        "id_number": _first(entity, "idNumber"),
        "address": _first(entity, "address"),
    }


def extract_ownership_props(entity: EntityProxy) -> Dict[str, Any]:
    """Extract Ownership relationship properties."""
    return {
        "ftm_id": entity.id,
        "owner": _first(entity, "owner"),
        "asset": _first(entity, "asset"),
        "percentage": _first(entity, "percentage"),
        "start_date": _first(entity, "startDate"),
        "end_date": _first(entity, "endDate"),
        "role": _first(entity, "role"),
    }


def extract_directorship_props(entity: EntityProxy) -> Dict[str, Any]:
    """Extract Directorship relationship properties."""
    return {
        "ftm_id": entity.id,
        "director": _first(entity, "director"),
        "organization": _first(entity, "organization"),
        "role": _first(entity, "role"),
        "start_date": _first(entity, "startDate"),
        "end_date": _first(entity, "endDate"),
    }


def extract_sanction_props(entity: EntityProxy) -> Dict[str, Any]:
    """Extract Sanction properties."""
    return {
        "ftm_id": entity.id,
        "entity": _first(entity, "entity"),
        "authority": _first(entity, "authority"),
        "program": _first(entity, "program"),
        "reason": _first(entity, "reason"),
        "listed_date": _first(entity, "listingDate") or _first(entity, "startDate"),
        "start_date": _first(entity, "startDate"),
        "end_date": _first(entity, "endDate"),
    }


# ---------------------------------------------------------------------------
# Memgraph Loader
# ---------------------------------------------------------------------------


class FtMMemgraphLoader:
    """Batch-loads FtM entities into Memgraph with MERGE semantics."""

    MERGE_COMPANY = """
    UNWIND $batch AS e
    MERGE (c:Company {ftm_id: e.ftm_id})
    SET c.name = e.name,
        c.aliases = e.aliases,
        c.jurisdiction = e.jurisdiction,
        c.registration_number = e.registration_number,
        c.incorporation_date = e.incorporation_date,
        c.lei_code = e.lei_code,
        c.opencorporates_url = e.opencorporates_url,
        c.status = e.status,
        c.country = e.country,
        c.address = e.address,
        c.source = 'opensanctions',
        c.schema = e.schema,
        c.updated_at = localDateTime()
    """

    MERGE_PERSON = """
    UNWIND $batch AS e
    MERGE (p:Person {ftm_id: e.ftm_id})
    SET p.name = e.name,
        p.aliases = e.aliases,
        p.birth_date = e.birth_date,
        p.nationality = e.nationality,
        p.country = e.country,
        p.id_number = e.id_number,
        p.address = e.address,
        p.source = 'opensanctions',
        p.schema = e.schema,
        p.updated_at = localDateTime()
    """

    MERGE_OWNERSHIP = """
    UNWIND $batch AS e
    MERGE (rel:Ownership {ftm_id: e.ftm_id})
    SET rel.percentage = e.percentage,
        rel.start_date = e.start_date,
        rel.end_date = e.end_date,
        rel.role = e.role,
        rel.source = 'opensanctions'
    WITH rel, e
    WHERE e.owner IS NOT NULL AND e.owner <> ''
    MATCH (owner {ftm_id: e.owner})
    MERGE (owner)-[:OWNS {ftm_id: e.ftm_id, percentage: e.percentage}]->(rel)
    WITH rel, e
    WHERE e.asset IS NOT NULL AND e.asset <> ''
    MATCH (asset {ftm_id: e.asset})
    MERGE (rel)-[:ASSET_OF {ftm_id: e.ftm_id}]->(asset)
    """

    MERGE_DIRECTORSHIP = """
    UNWIND $batch AS e
    MATCH (d {ftm_id: e.director})
    MATCH (o {ftm_id: e.organization})
    MERGE (d)-[r:DIRECTOR_OF {ftm_id: e.ftm_id}]->(o)
    SET r.role = e.role,
        r.start_date = e.start_date,
        r.end_date = e.end_date,
        r.source = 'opensanctions'
    """

    MERGE_SANCTION = """
    UNWIND $batch AS e
    MERGE (s:Sanction {ftm_id: e.ftm_id})
    SET s.authority = e.authority,
        s.program = e.program,
        s.reason = e.reason,
        s.listed_date = e.listed_date,
        s.start_date = e.start_date,
        s.end_date = e.end_date,
        s.source = 'opensanctions',
        s.updated_at = localDateTime()
    WITH s, e
    WHERE e.entity IS NOT NULL AND e.entity <> ''
    MATCH (target {ftm_id: e.entity})
    MERGE (target)-[:SANCTIONED_BY]->(s)
    """

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

    def ensure_indexes(self) -> None:
        """Create indexes for FtM nodes."""
        for cmd in [
            "CREATE INDEX ON :Company(ftm_id)",
            "CREATE INDEX ON :Person(ftm_id)",
            "CREATE INDEX ON :Sanction(ftm_id)",
            "CREATE INDEX ON :Ownership(ftm_id)",
        ]:
            try:
                self._execute(cmd)
            except Exception as exc:
                logger.debug("Index may already exist: %s", exc)

    def _flush_batch(self, query: str, batch: List[Dict]) -> int:
        if not batch:
            return 0
        self._execute(query, {"batch": batch})
        return len(batch)

    def load_entities(self, entities: Generator[EntityProxy, None, None]) -> Dict[str, int]:
        """Route FtM entities by schema and batch MERGE into Memgraph."""
        self.ensure_indexes()

        buffers: Dict[str, List[Dict]] = {
            "Company": [], "Organization": [], "Person": [],
            "Ownership": [], "Directorship": [], "Sanction": [],
        }
        counts: Dict[str, int] = {k: 0 for k in buffers}
        extractors = {
            "Company": extract_company_props,
            "Organization": extract_company_props,
            "Person": extract_person_props,
            "Ownership": extract_ownership_props,
            "Directorship": extract_directorship_props,
            "Sanction": extract_sanction_props,
        }
        queries = {
            "Company": self.MERGE_COMPANY,
            "Organization": self.MERGE_COMPANY,
            "Person": self.MERGE_PERSON,
            "Ownership": self.MERGE_OWNERSHIP,
            "Directorship": self.MERGE_DIRECTORSHIP,
            "Sanction": self.MERGE_SANCTION,
        }

        for entity in entities:
            schema = entity.schema.name
            if schema not in extractors:
                continue
            props = extractors[schema](entity)
            buffers[schema].append(props)

            if len(buffers[schema]) >= self.batch_size:
                counts[schema] += self._flush_batch(
                    queries[schema], buffers[schema]
                )
                buffers[schema].clear()

        # Flush remaining
        for schema, buf in buffers.items():
            if buf:
                counts[schema] += self._flush_batch(queries[schema], buf)

        logger.info("FtM Memgraph load complete: %s", counts)
        return counts

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


# ---------------------------------------------------------------------------
# Kafka Producer
# ---------------------------------------------------------------------------


class FtMKafkaProducer:
    """Sends FtM entities to Kafka topic."""

    def __init__(self, broker: str = KAFKA_BROKER, topic: str = KAFKA_TOPIC):
        self.topic = topic
        self._producer = Producer({"bootstrap.servers": broker})
        self._count = 0

    def _delivery_cb(self, err, msg):
        if err:
            logger.error("Kafka delivery failed: %s", err)

    def send(self, entity: EntityProxy) -> None:
        payload = json.dumps(
            {"id": entity.id, "schema": entity.schema.name,
             "properties": dict(entity.properties)},
            default=str,
        ).encode()
        self._producer.produce(
            self.topic, key=entity.id.encode(), value=payload,
            callback=self._delivery_cb,
        )
        self._count += 1
        if self._count % 10_000 == 0:
            self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush(timeout=30)
        logger.info("Kafka flush complete, sent %d messages", self._count)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def sync_opensanctions(
    paths: List[str],
    write_kafka: bool = True,
    write_graph: bool = True,
    datasets: Optional[List[str]] = None,
) -> Dict[str, int]:
    """Full OpenSanctions FtM sync pipeline.

    Args:
        paths: List of .ftm.json file paths to process.
        write_kafka: Whether to produce to Kafka.
        write_graph: Whether to write to Memgraph.
        datasets: If set, only process files matching these dataset names.

    Returns:
        Dict of schema -> count of entities loaded.
    """
    if datasets:
        logger.info("Filtering for priority datasets: %s", datasets)

    parser = FtMEntityParser()
    kafka = FtMKafkaProducer() if write_kafka else None
    graph = FtMMemgraphLoader() if write_graph else None

    def _entity_stream() -> Generator[EntityProxy, None, None]:
        for entity in parser.parse_multiple(paths):
            if kafka:
                kafka.send(entity)
            yield entity

    counts: Dict[str, int] = {}
    if graph:
        counts = graph.load_entities(_entity_stream())
        graph.close()
    else:
        for entity in _entity_stream():
            schema = entity.schema.name
            counts[schema] = counts.get(schema, 0) + 1

    if kafka:
        kafka.flush()

    logger.info("OpenSanctions sync complete: %s", counts)
    return counts


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    ap = argparse.ArgumentParser(description="OpenSanctions FtM Sync")
    ap.add_argument("files", nargs="+", help="Paths to .ftm.json JSONL files")
    ap.add_argument("--no-kafka", action="store_true")
    ap.add_argument("--no-graph", action="store_true")
    ap.add_argument(
        "--priority-only", action="store_true",
        help="Only process priority datasets",
    )

    args = ap.parse_args()
    datasets = PRIORITY_DATASETS if args.priority_only else None
    result = sync_opensanctions(
        paths=args.files,
        write_kafka=not args.no_kafka,
        write_graph=not args.no_graph,
        datasets=datasets,
    )
    print(json.dumps(result, indent=2))
