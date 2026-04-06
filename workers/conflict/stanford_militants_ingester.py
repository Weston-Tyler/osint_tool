"""Stanford Mapping Militant Organizations (MMP) ingestion worker.

Parses group profile data from Stanford's MMP dataset (JSON or CSV),
normalizes to Organization-compatible dicts, builds alliance/rivalry
networks for Memgraph, and publishes to Kafka.

Source: https://cisac.fsi.stanford.edu/mappingmilitants
"""

import csv
import json
import logging
import os
from datetime import datetime, timezone

from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.stanford_militants")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = "mda.conflict.militant_groups"

# Expected JSON structure for group profiles:
# [
#   {
#     "name": "Group Name",
#     "aliases": ["Alias 1", "Alias 2"],
#     "ideology": "Jihadist",
#     "territory": "Syria, Iraq",
#     "allies": ["Allied Group 1"],
#     "rivals": ["Rival Group 1"],
#     "founding_year": 1999,
#     "peak_strength": "10000-15000",
#     "active": true,
#     "description": "...",
#     "headquarters": "...",
#     "leaders": ["Leader 1"],
#     "attacks": [...]
#   }
# ]

# CSV column candidates for when data is in tabular form
CSV_COLUMN_MAP = {
    "name": ["Name", "name", "Group Name", "Organization"],
    "aliases": ["Aliases", "aliases", "Also known as", "AKA", "Other names"],
    "ideology": ["Ideology", "ideology", "Type", "Classification"],
    "territory": [
        "Territory", "territory", "Area of operation",
        "Operating area", "Region",
    ],
    "allies": ["Allies", "allies", "Allied groups", "Alliances"],
    "rivals": ["Rivals", "rivals", "Rival groups", "Enemies", "Opponents"],
    "founding_year": [
        "Founding year", "founding_year", "Founded", "Year founded",
        "Year established",
    ],
    "peak_strength": [
        "Peak strength", "peak_strength", "Estimated strength",
        "Size", "Manpower",
    ],
    "active": ["Active", "active", "Status", "Currently active"],
    "description": ["Description", "description", "Summary", "Overview"],
    "headquarters": ["Headquarters", "headquarters", "HQ", "Base"],
    "leaders": ["Leaders", "leaders", "Leadership", "Key leaders"],
}


def _resolve_column(header: list[str], candidates: list[str]) -> str | None:
    """Find the first matching column name from a list of candidates."""
    header_lower = {h.lower().strip(): h for h in header}
    for candidate in candidates:
        if candidate in header:
            return candidate
        if candidate.lower().strip() in header_lower:
            return header_lower[candidate.lower().strip()]
    return None


def _parse_list_field(value) -> list[str]:
    """Parse a field that may be a list, semicolon-separated string, or comma-separated string."""
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if not value or not str(value).strip():
        return []
    s = str(value).strip()
    # Try semicolon first (less likely to appear inside names)
    if ";" in s:
        return [item.strip() for item in s.split(";") if item.strip()]
    # Fall back to comma
    return [item.strip() for item in s.split(",") if item.strip()]


def _parse_bool(value) -> bool:
    """Parse a boolean-ish value."""
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    return s in ("true", "yes", "1", "active", "y")


def _safe_int(value) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(float(str(value).strip().replace(",", "")))
    except (ValueError, TypeError):
        return None


class StanfordMilitantsIngester:
    """Parses Stanford MMP militant group data and publishes to Kafka."""

    def __init__(self, kafka_bootstrap: str = KAFKA_BOOTSTRAP):
        self.kafka_bootstrap = kafka_bootstrap
        self._producer = None
        self._groups: list[dict] = []

    @property
    def producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
        return self._producer

    def close(self):
        """Flush and close the Kafka producer."""
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            self._producer = None

    def load_group_profiles(self, filepath: str) -> list[dict]:
        """Load group profiles from JSON or CSV.

        Auto-detects format based on file extension.

        Args:
            filepath: Path to group profiles file (.json or .csv).

        Returns:
            List of raw group profile dicts.
        """
        if filepath.lower().endswith(".json"):
            return self._load_json(filepath)
        elif filepath.lower().endswith(".csv"):
            return self._load_csv(filepath)
        else:
            # Attempt JSON first, fall back to CSV
            try:
                return self._load_json(filepath)
            except (json.JSONDecodeError, UnicodeDecodeError):
                return self._load_csv(filepath)

    def _load_json(self, filepath: str) -> list[dict]:
        """Load group profiles from a JSON file."""
        with open(filepath, encoding="utf-8") as fh:
            data = json.load(fh)

        # Handle both list-at-root and wrapped formats
        if isinstance(data, list):
            groups = data
        elif isinstance(data, dict):
            # Try common wrapper keys
            for key in ("groups", "organizations", "data", "profiles"):
                if key in data and isinstance(data[key], list):
                    groups = data[key]
                    break
            else:
                logger.error("JSON file does not contain a recognizable group list")
                return []
        else:
            logger.error("Unexpected JSON root type: %s", type(data).__name__)
            return []

        self._groups = groups
        logger.info("Loaded %d group profiles from %s (JSON)", len(groups), filepath)
        return groups

    def _load_csv(self, filepath: str) -> list[dict]:
        """Load group profiles from a CSV file."""
        groups: list[dict] = []

        with open(filepath, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            header = reader.fieldnames or []

            resolved: dict[str, str | None] = {}
            for canonical, candidates in CSV_COLUMN_MAP.items():
                resolved[canonical] = _resolve_column(header, candidates)

            for raw_row in reader:
                row: dict = {}
                for canonical, col_name in resolved.items():
                    if col_name is not None:
                        row[canonical] = raw_row.get(col_name, "").strip()
                    else:
                        row[canonical] = None
                groups.append(row)

        self._groups = groups
        logger.info("Loaded %d group profiles from %s (CSV)", len(groups), filepath)
        return groups

    @staticmethod
    def normalize_group(raw: dict) -> dict:
        """Normalize a raw group profile to an Organization-compatible dict.

        Args:
            raw: Raw group profile dict.

        Returns:
            Normalized organization dict suitable for Memgraph/Kafka.
        """
        name = str(raw.get("name", "")).strip()
        aliases = _parse_list_field(raw.get("aliases", []))
        ideology = str(raw.get("ideology", "") or "").strip()
        territory = str(raw.get("territory", "") or "").strip()
        allies = _parse_list_field(raw.get("allies", []))
        rivals = _parse_list_field(raw.get("rivals", []))
        founding_year = _safe_int(raw.get("founding_year"))
        peak_strength = str(raw.get("peak_strength", "") or "").strip()
        active = _parse_bool(raw.get("active", True))

        # Build deterministic ID
        slug = name.lower().replace(" ", "_").replace("'", "").replace("-", "_")[:50]
        entity_id = f"mmp_{slug}"

        return {
            "entity_id": entity_id,
            "entity_type": "militant_organization",
            "source": "stanford_mmp",
            "name": name,
            "aliases": aliases,
            "ideology": ideology,
            "territory": territory,
            "territory_list": _parse_list_field(territory) if territory else [],
            "allies": allies,
            "rivals": rivals,
            "founding_year": founding_year,
            "peak_strength": peak_strength,
            "active": active,
            "description": str(raw.get("description", "") or "").strip(),
            "headquarters": str(raw.get("headquarters", "") or "").strip(),
            "leaders": _parse_list_field(raw.get("leaders", [])),
            "confidence": 0.85,  # Academic source
            "ingest_time": datetime.now(timezone.utc).isoformat(),
        }

    def build_alliance_network(self) -> list[dict]:
        """Extract alliance and rivalry edges from loaded group profiles.

        Builds a list of relationship edges suitable for insertion into
        Memgraph or any graph database.

        Returns:
            List of edge dicts with: source, target, relationship_type, source_db.
        """
        if not self._groups:
            logger.warning("No groups loaded; call load_group_profiles first")
            return []

        edges: list[dict] = []
        seen: set[tuple[str, str, str]] = set()

        for raw in self._groups:
            group_name = str(raw.get("name", "")).strip()
            if not group_name:
                continue

            # Alliance edges
            allies = _parse_list_field(raw.get("allies", []))
            for ally in allies:
                edge_key = (
                    min(group_name, ally),
                    max(group_name, ally),
                    "ALLIED_WITH",
                )
                if edge_key not in seen:
                    seen.add(edge_key)
                    edges.append({
                        "source": group_name,
                        "target": ally,
                        "relationship_type": "ALLIED_WITH",
                        "source_db": "stanford_mmp",
                        "bidirectional": True,
                    })

            # Rivalry edges
            rivals = _parse_list_field(raw.get("rivals", []))
            for rival in rivals:
                edge_key = (
                    min(group_name, rival),
                    max(group_name, rival),
                    "RIVAL_OF",
                )
                if edge_key not in seen:
                    seen.add(edge_key)
                    edges.append({
                        "source": group_name,
                        "target": rival,
                        "relationship_type": "RIVAL_OF",
                        "source_db": "stanford_mmp",
                        "bidirectional": True,
                    })

        logger.info(
            "Built alliance network: %d edges (%d alliances, %d rivalries)",
            len(edges),
            sum(1 for e in edges if e["relationship_type"] == "ALLIED_WITH"),
            sum(1 for e in edges if e["relationship_type"] == "RIVAL_OF"),
        )
        return edges

    def ingest(self, filepath: str) -> int:
        """Load group profiles, normalize, build network, and publish to Kafka.

        Publishes both group profile records and network edge records
        to the Kafka topic.

        Args:
            filepath: Path to group profiles file (.json or .csv).

        Returns:
            Total number of records published (groups + edges).
        """
        raw_groups = self.load_group_profiles(filepath)
        count = 0

        # Publish normalized group profiles
        for raw in raw_groups:
            group = self.normalize_group(raw)
            self.producer.send(
                KAFKA_TOPIC,
                value={**group, "record_type": "group_profile"},
            )
            count += 1

        # Build and publish alliance/rivalry network edges
        edges = self.build_alliance_network()
        for edge in edges:
            self.producer.send(
                KAFKA_TOPIC,
                value={
                    **edge,
                    "record_type": "network_edge",
                    "source_system": "stanford_mmp",
                    "ingest_time": datetime.now(timezone.utc).isoformat(),
                },
            )
            count += 1

        self.producer.flush()
        logger.info(
            "Published %d Stanford MMP records (%d groups, %d edges) from %s",
            count, len(raw_groups), len(edges), filepath,
        )
        return count


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if len(sys.argv) < 2:
        print("Usage: python stanford_militants_ingester.py <filepath>")
        print("  Accepts .json or .csv files")
        sys.exit(1)

    ingester = StanfordMilitantsIngester()
    try:
        published = ingester.ingest(sys.argv[1])
        print(f"Published {published} records")
    finally:
        ingester.close()
