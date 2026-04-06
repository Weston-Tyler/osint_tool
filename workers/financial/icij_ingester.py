"""ICIJ Offshore Leaks ingestion (Panama/Paradise/Pandora Papers).

Loads the ICIJ Offshore Leaks database CSV dumps, normalises entities,
officers, and intermediaries, builds shell-company ownership graphs, and
publishes to Kafka for MDA cross-referencing.

Data source: https://offshoreleaks.icij.org/pages/database
Note: data is static — this is a one-time (or periodic re-load) ingester,
not a continuous polling service.
"""

import csv
import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import networkx as nx
import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.financial.icij")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# Default data directory for CSV files
DEFAULT_DATA_DIR = Path(os.getenv("ICIJ_DATA_DIR", "/data/icij"))

# ICIJ CSV download URLs (bulk download archive)
ICIJ_DOWNLOAD_URLS: dict[str, str] = {
    "entities": "https://offshoreleaks.icij.org/pages/database",
    # Users must manually download CSVs from the ICIJ site; the URLs below
    # are placeholders for the expected filenames.
}

EXPECTED_FILES = {
    "entities": "nodes-entities.csv",
    "officers": "nodes-officers.csv",
    "intermediaries": "nodes-intermediaries.csv",
    "addresses": "nodes-addresses.csv",
    "relationships": "relationships.csv",
}


class ICIJIngester:
    """One-time ingester for ICIJ Offshore Leaks database dumps."""

    def __init__(self, kafka_bootstrap: str | None = None):
        self.kafka_bootstrap = kafka_bootstrap or KAFKA_BOOTSTRAP
        self._producer: KafkaProducer | None = None

        # In-memory stores populated during load
        self.entities: list[dict[str, Any]] = []
        self.officers: list[dict[str, Any]] = []
        self.intermediaries: list[dict[str, Any]] = []
        self.addresses: list[dict[str, Any]] = []
        self.relationships: list[dict[str, Any]] = []

        # Ownership graph
        self.graph: nx.DiGraph = nx.DiGraph()

    # ------------------------------------------------------------------
    # Kafka helpers
    # ------------------------------------------------------------------

    def _get_producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
        return self._producer

    def _close_producer(self) -> None:
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            self._producer = None

    # ------------------------------------------------------------------
    # CSV loading
    # ------------------------------------------------------------------

    @staticmethod
    def _read_csv(filepath: Path) -> list[dict[str, str]]:
        """Read a CSV file and return a list of row dicts."""
        rows: list[dict[str, str]] = []
        with open(filepath, newline="", encoding="utf-8", errors="replace") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rows.append(row)
        logger.info("Loaded %d rows from %s", len(rows), filepath.name)
        return rows

    def load_entities(self, filepath: str | Path) -> list[dict[str, str]]:
        """Parse the ICIJ entities CSV (shell companies, trusts, etc.)."""
        filepath = Path(filepath)
        self.entities = self._read_csv(filepath)
        return self.entities

    def load_officers(self, filepath: str | Path) -> list[dict[str, str]]:
        """Parse the ICIJ officers CSV (beneficial owners, directors)."""
        filepath = Path(filepath)
        self.officers = self._read_csv(filepath)
        return self.officers

    def load_intermediaries(self, filepath: str | Path) -> list[dict[str, str]]:
        """Parse intermediaries CSV (law firms, registered agents)."""
        filepath = Path(filepath)
        self.intermediaries = self._read_csv(filepath)
        return self.intermediaries

    def load_addresses(self, filepath: str | Path) -> list[dict[str, str]]:
        """Parse addresses CSV."""
        filepath = Path(filepath)
        self.addresses = self._read_csv(filepath)
        return self.addresses

    def load_relationships(self, filepath: str | Path) -> list[dict[str, str]]:
        """Parse entity-officer-intermediary relationship edges.

        Expected columns: ``node_id_start``, ``node_id_end``, ``rel_type``,
        ``link``, ``start_date``, ``end_date``.
        """
        filepath = Path(filepath)
        self.relationships = self._read_csv(filepath)
        return self.relationships

    # ------------------------------------------------------------------
    # Normalisation
    # ------------------------------------------------------------------

    @staticmethod
    def normalize_entity(raw: dict[str, str]) -> dict[str, Any]:
        """Map a raw ICIJ entity row to a Financial_Entity-compatible dict.

        Output fields align with the MDA ontology for offshore entities.
        """
        return {
            "entity_id": f"icij_entity_{raw.get('node_id', '')}",
            "entity_type": "offshore_entity",
            "name": (raw.get("name") or "").strip(),
            "original_name": (raw.get("original_name") or "").strip(),
            "jurisdiction": (raw.get("jurisdiction") or "").strip(),
            "jurisdiction_description": (raw.get("jurisdiction_description") or "").strip(),
            "country_codes": (raw.get("country_codes") or "").strip(),
            "countries": (raw.get("countries") or "").strip(),
            "incorporation_date": (raw.get("incorporation_date") or "").strip(),
            "inactivation_date": (raw.get("inactivation_date") or "").strip(),
            "struck_off_date": (raw.get("struck_off_date") or "").strip(),
            "status": (raw.get("status") or "").strip(),
            "service_provider": (raw.get("service_provider") or "").strip(),
            "source_id": (raw.get("sourceID") or "").strip(),
            "valid_until": (raw.get("valid_until") or "").strip(),
            "note": (raw.get("note") or "").strip(),
            "source": "icij_offshore_leaks",
            "ingest_time": datetime.utcnow().isoformat(),
        }

    @staticmethod
    def normalize_officer(raw: dict[str, str]) -> dict[str, Any]:
        """Map a raw ICIJ officer row to a Person-compatible dict."""
        return {
            "entity_id": f"icij_officer_{raw.get('node_id', '')}",
            "entity_type": "person",
            "name_full": (raw.get("name") or "").strip(),
            "country_codes": (raw.get("country_codes") or "").strip(),
            "countries": (raw.get("countries") or "").strip(),
            "source_id": (raw.get("sourceID") or "").strip(),
            "valid_until": (raw.get("valid_until") or "").strip(),
            "note": (raw.get("note") or "").strip(),
            "source": "icij_offshore_leaks",
            "ingest_time": datetime.utcnow().isoformat(),
        }

    # ------------------------------------------------------------------
    # Ownership graph construction
    # ------------------------------------------------------------------

    def build_ownership_graph(self) -> nx.DiGraph:
        """Construct directed ownership/control graph from loaded data.

        Node types:
          - ``officer``  — beneficial owner / director
          - ``intermediary`` — registered agent / law firm
          - ``entity`` — shell company / trust

        Edge types (from ``rel_type`` column):
          - ``registered_agent``
          - ``shareholder``
          - ``beneficiary``
          - ``nominee``
          - ``director``
          - ``similar``
          - other raw values preserved as-is

        Returns
        -------
        nx.DiGraph
            The ownership graph is also stored as ``self.graph``.
        """
        G = nx.DiGraph()

        # Add entity nodes
        for raw in self.entities:
            nid = raw.get("node_id", "")
            G.add_node(
                nid,
                node_type="entity",
                name=(raw.get("name") or "").strip(),
                jurisdiction=(raw.get("jurisdiction") or "").strip(),
                source_id=(raw.get("sourceID") or "").strip(),
            )

        # Add officer nodes
        for raw in self.officers:
            nid = raw.get("node_id", "")
            G.add_node(
                nid,
                node_type="officer",
                name=(raw.get("name") or "").strip(),
                countries=(raw.get("countries") or "").strip(),
            )

        # Add intermediary nodes
        for raw in self.intermediaries:
            nid = raw.get("node_id", "")
            G.add_node(
                nid,
                node_type="intermediary",
                name=(raw.get("name") or "").strip(),
                countries=(raw.get("countries") or "").strip(),
            )

        # Add relationship edges
        for rel in self.relationships:
            src = rel.get("node_id_start", "")
            dst = rel.get("node_id_end", "")
            rel_type = (rel.get("rel_type") or rel.get("link") or "unknown").strip().lower()

            # Normalise common rel_type variants
            rel_type_map = {
                "registered agent of": "registered_agent",
                "shareholder of": "shareholder",
                "beneficial owner of": "beneficiary",
                "beneficiary of": "beneficiary",
                "nominee shareholder of": "nominee",
                "nominee director of": "nominee",
                "director of": "director",
                "similar name and target address as": "similar",
            }
            normalised = rel_type_map.get(rel_type, rel_type.replace(" ", "_"))

            G.add_edge(
                src,
                dst,
                rel_type=normalised,
                start_date=rel.get("start_date", ""),
                end_date=rel.get("end_date", ""),
            )

        self.graph = G
        logger.info(
            "Built ownership graph: %d nodes, %d edges",
            G.number_of_nodes(),
            G.number_of_edges(),
        )
        return G

    # ------------------------------------------------------------------
    # Cross-referencing
    # ------------------------------------------------------------------

    def find_mda_connections(
        self,
        entity_names: list[str] | None = None,
        person_names: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Cross-reference ICIJ data against known sanctioned names.

        Performs case-insensitive substring matching of *entity_names* and
        *person_names* (e.g. from OFAC SDN / OpenSanctions) against the
        loaded ICIJ entities and officers.

        Parameters
        ----------
        entity_names : list[str]
            Organisation / vessel names to search for.
        person_names : list[str]
            Individual names to search for.

        Returns
        -------
        list[dict]
            Matching ICIJ records with match metadata.
        """
        matches: list[dict[str, Any]] = []

        search_names: list[tuple[str, str]] = []
        for name in (entity_names or []):
            search_names.append((name.lower(), "entity_name"))
        for name in (person_names or []):
            search_names.append((name.lower(), "person_name"))

        if not search_names:
            return matches

        # Search entities
        for raw in self.entities:
            icij_name = (raw.get("name") or "").lower()
            if not icij_name:
                continue
            for search_term, match_type in search_names:
                if search_term in icij_name or icij_name in search_term:
                    matches.append(
                        {
                            "match_type": match_type,
                            "search_term": search_term,
                            "icij_record": self.normalize_entity(raw),
                            "icij_node_type": "entity",
                        }
                    )

        # Search officers
        for raw in self.officers:
            icij_name = (raw.get("name") or "").lower()
            if not icij_name:
                continue
            for search_term, match_type in search_names:
                if search_term in icij_name or icij_name in search_term:
                    matches.append(
                        {
                            "match_type": match_type,
                            "search_term": search_term,
                            "icij_record": self.normalize_officer(raw),
                            "icij_node_type": "officer",
                        }
                    )

        logger.info(
            "Found %d ICIJ matches for %d search terms",
            len(matches),
            len(search_names),
        )
        return matches

    # ------------------------------------------------------------------
    # Full ingestion
    # ------------------------------------------------------------------

    def ingest_all(self, data_dir: str | Path | None = None) -> dict[str, int]:
        """Load all ICIJ CSV files and publish to Kafka.

        Topics:
          - ``mda.icij.entities`` — normalised entity records
          - ``mda.icij.relationships`` — edge records

        Parameters
        ----------
        data_dir : str or Path, optional
            Directory containing the ICIJ CSV files.  Defaults to
            ``/data/icij`` or the ``ICIJ_DATA_DIR`` env var.

        Returns
        -------
        dict[str, int]
            Count of messages published per topic.
        """
        data_dir = Path(data_dir) if data_dir else DEFAULT_DATA_DIR
        if not data_dir.is_dir():
            raise FileNotFoundError(f"ICIJ data directory not found: {data_dir}")

        # Load CSVs
        entities_file = data_dir / EXPECTED_FILES["entities"]
        officers_file = data_dir / EXPECTED_FILES["officers"]
        intermediaries_file = data_dir / EXPECTED_FILES["intermediaries"]
        addresses_file = data_dir / EXPECTED_FILES["addresses"]
        rels_file = data_dir / EXPECTED_FILES["relationships"]

        if entities_file.exists():
            self.load_entities(entities_file)
        if officers_file.exists():
            self.load_officers(officers_file)
        if intermediaries_file.exists():
            self.load_intermediaries(intermediaries_file)
        if addresses_file.exists():
            self.load_addresses(addresses_file)
        if rels_file.exists():
            self.load_relationships(rels_file)

        # Build graph
        self.build_ownership_graph()

        # Publish
        producer = self._get_producer()
        counts: dict[str, int] = {"mda.icij.entities": 0, "mda.icij.relationships": 0}

        # Publish entities
        for raw in self.entities:
            try:
                normalised = self.normalize_entity(raw)
                producer.send("mda.icij.entities", normalised)
                counts["mda.icij.entities"] += 1
            except Exception as exc:
                logger.warning("Failed to publish entity: %s", exc)

        # Publish officers as entities too
        for raw in self.officers:
            try:
                normalised = self.normalize_officer(raw)
                producer.send("mda.icij.entities", normalised)
                counts["mda.icij.entities"] += 1
            except Exception as exc:
                logger.warning("Failed to publish officer: %s", exc)

        # Publish relationships
        for rel in self.relationships:
            try:
                rel_type = (rel.get("rel_type") or rel.get("link") or "unknown").strip()
                msg = {
                    "source_node": rel.get("node_id_start", ""),
                    "target_node": rel.get("node_id_end", ""),
                    "rel_type": rel_type,
                    "start_date": rel.get("start_date", ""),
                    "end_date": rel.get("end_date", ""),
                    "source": "icij_offshore_leaks",
                    "ingest_time": datetime.utcnow().isoformat(),
                }
                producer.send("mda.icij.relationships", msg)
                counts["mda.icij.relationships"] += 1
            except Exception as exc:
                logger.warning("Failed to publish relationship: %s", exc)

        self._close_producer()
        logger.info("ICIJ ingestion complete: %s", counts)
        return counts


# ------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    ingester = ICIJIngester()
    ingester.ingest_all()
