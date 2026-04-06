"""EU and UN sanctions list aggregation — complement to OFAC/OpenSanctions.

Downloads and parses the EU consolidated sanctions XML and the UN Security
Council consolidated sanctions XML, normalises entries, deduplicates against
existing OFAC/OpenSanctions data, and publishes to Kafka.

Sources:
  - EU: https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content
  - UN: https://scsanctions.un.org/resources/xml/en/consolidated.xml
"""

import hashlib
import json
import logging
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.financial.sanctions_aggregator")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

EU_SANCTIONS_URL = (
    "https://webgate.ec.europa.eu/fsd/fsf/public/files/"
    "xmlFullSanctionsList_1_1/content"
)
UN_SANCTIONS_URL = (
    "https://scsanctions.un.org/resources/xml/en/consolidated.xml"
)


class SanctionsAggregator:
    """Daily ingestion worker for EU and UN sanctions lists."""

    def __init__(self, kafka_bootstrap: str | None = None):
        self.kafka_bootstrap = kafka_bootstrap or KAFKA_BOOTSTRAP
        self._producer: KafkaProducer | None = None

        # Dedup set: sha256 hashes of (source, name_lower) tuples already seen
        self._known_hashes: set[str] = set()

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
    # EU sanctions
    # ------------------------------------------------------------------

    def fetch_eu_sanctions(self) -> list[dict[str, Any]]:
        """Download and parse the EU consolidated sanctions XML.

        Returns a list of normalised entity dicts.
        """
        logger.info("Downloading EU sanctions XML from %s", EU_SANCTIONS_URL)
        resp = requests.get(EU_SANCTIONS_URL, timeout=120)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)

        # The EU XML uses a default namespace; detect it.
        ns = ""
        if root.tag.startswith("{"):
            ns = root.tag.split("}")[0] + "}"

        entities: list[dict[str, Any]] = []

        # Each <sanctionEntity> element contains one listing
        for entity_el in root.iter(f"{ns}sanctionEntity"):
            try:
                normalised = self.normalize_eu_entry(entity_el, ns)
                if normalised:
                    entities.append(normalised)
            except Exception:
                logger.debug("Failed to parse EU entity element", exc_info=True)

        logger.info("Parsed %d EU sanctions entries", len(entities))
        return entities

    def normalize_eu_entry(
        self,
        entry: ET.Element,
        ns: str = "",
    ) -> dict[str, Any] | None:
        """Map an EU XML ``<sanctionEntity>`` to a normalised sanctions dict.

        Fields: entity_id, name, aliases, entity_type, program, listing_date,
        country, sanctions_source.
        """
        logical_id = entry.attrib.get("logicalId", "")
        eu_ref = entry.attrib.get("euReferenceNumber", logical_id)

        # Subject type: person / entity
        subject_type_el = entry.find(f"{ns}subjectType")
        raw_type = ""
        if subject_type_el is not None:
            code_el = subject_type_el.find(f"{ns}code")
            raw_type = (code_el.text if code_el is not None else "").strip().lower()

        entity_type = "person" if raw_type == "person" else "organization"

        # Names
        names: list[str] = []
        for name_alias in entry.iter(f"{ns}nameAlias"):
            whole = name_alias.attrib.get("wholeName", "").strip()
            if whole:
                names.append(whole)
            else:
                first = name_alias.attrib.get("firstName", "").strip()
                last = name_alias.attrib.get("lastName", "").strip()
                middle = name_alias.attrib.get("middleName", "").strip()
                parts = [p for p in [first, middle, last] if p]
                if parts:
                    names.append(" ".join(parts))

        if not names:
            return None

        primary_name = names[0]
        aliases = names[1:]

        # Programme / regulation
        programmes: list[str] = []
        for reg in entry.iter(f"{ns}regulation"):
            prog = reg.attrib.get("programme", "").strip()
            if prog and prog not in programmes:
                programmes.append(prog)

        # Listing date
        listing_date = ""
        for reg in entry.iter(f"{ns}regulation"):
            entry_date = reg.attrib.get("entryIntoForceDate", "").strip()
            if entry_date:
                listing_date = entry_date
                break

        # Country (from citizenship or address)
        country = ""
        for citizen in entry.iter(f"{ns}citizenship"):
            country_el = citizen.find(f"{ns}country")
            if country_el is not None:
                country = country_el.attrib.get("countryIso2Code", "").strip()
                break
        if not country:
            for addr in entry.iter(f"{ns}address"):
                country_el = addr.find(f"{ns}country")
                if country_el is not None:
                    country = country_el.attrib.get("countryIso2Code", "").strip()
                    break

        return {
            "entity_id": f"eu_sanctions_{eu_ref}",
            "name": primary_name,
            "aliases": aliases,
            "entity_type": entity_type,
            "program": programmes,
            "listing_date": listing_date,
            "country": country,
            "sanctions_source": "eu",
            "sanctions_status": "SANCTIONED",
            "eu_reference": eu_ref,
            "source": "eu_consolidated_sanctions",
            "ingest_time": datetime.utcnow().isoformat(),
        }

    # ------------------------------------------------------------------
    # UN sanctions
    # ------------------------------------------------------------------

    def fetch_un_sanctions(self) -> list[dict[str, Any]]:
        """Download and parse the UN SC consolidated sanctions XML.

        Returns a list of normalised entity dicts.
        """
        logger.info("Downloading UN sanctions XML from %s", UN_SANCTIONS_URL)
        resp = requests.get(UN_SANCTIONS_URL, timeout=120)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        entities: list[dict[str, Any]] = []

        # UN XML structure: <INDIVIDUALS><INDIVIDUAL>... and <ENTITIES><ENTITY>...
        for individual in root.iter("INDIVIDUAL"):
            try:
                normalised = self._parse_un_individual(individual)
                if normalised:
                    entities.append(normalised)
            except Exception:
                logger.debug("Failed to parse UN individual", exc_info=True)

        for entity_el in root.iter("ENTITY"):
            try:
                normalised = self._parse_un_entity(entity_el)
                if normalised:
                    entities.append(normalised)
            except Exception:
                logger.debug("Failed to parse UN entity", exc_info=True)

        logger.info("Parsed %d UN sanctions entries", len(entities))
        return entities

    def _parse_un_individual(self, el: ET.Element) -> dict[str, Any] | None:
        """Parse a UN ``<INDIVIDUAL>`` element."""
        dataid = self._text(el, "DATAID")

        first = self._text(el, "FIRST_NAME")
        second = self._text(el, "SECOND_NAME")
        third = self._text(el, "THIRD_NAME")
        fourth = self._text(el, "FOURTH_NAME")
        name_parts = [p for p in [first, second, third, fourth] if p]
        primary_name = " ".join(name_parts)

        if not primary_name:
            return None

        aliases = self._collect_un_aliases(el)
        listing_date = self._text(el, "LISTED_ON")
        nationality = self._text(el, "NATIONALITY/VALUE")

        un_list_type = self._text(el, "UN_LIST_TYPE")
        reference = self._text(el, "REFERENCE_NUMBER")

        return {
            "entity_id": f"un_sanctions_{dataid}",
            "name": primary_name,
            "aliases": aliases,
            "entity_type": "person",
            "program": [un_list_type] if un_list_type else [],
            "listing_date": listing_date,
            "country": nationality,
            "sanctions_source": "un",
            "sanctions_status": "SANCTIONED",
            "un_reference": reference,
            "source": "un_sc_consolidated",
            "ingest_time": datetime.utcnow().isoformat(),
        }

    def _parse_un_entity(self, el: ET.Element) -> dict[str, Any] | None:
        """Parse a UN ``<ENTITY>`` element."""
        dataid = self._text(el, "DATAID")
        primary_name = self._text(el, "FIRST_NAME")
        if not primary_name:
            return None

        aliases = self._collect_un_aliases(el)
        listing_date = self._text(el, "LISTED_ON")
        un_list_type = self._text(el, "UN_LIST_TYPE")
        reference = self._text(el, "REFERENCE_NUMBER")

        # Country from address
        country = ""
        for addr in el.iter("ADDRESS"):
            c = self._text(addr, "COUNTRY")
            if c:
                country = c
                break

        return {
            "entity_id": f"un_sanctions_{dataid}",
            "name": primary_name,
            "aliases": aliases,
            "entity_type": "organization",
            "program": [un_list_type] if un_list_type else [],
            "listing_date": listing_date,
            "country": country,
            "sanctions_source": "un",
            "sanctions_status": "SANCTIONED",
            "un_reference": reference,
            "source": "un_sc_consolidated",
            "ingest_time": datetime.utcnow().isoformat(),
        }

    @staticmethod
    def _text(el: ET.Element, path: str) -> str:
        """Extract text from a child element by tag path, or empty string."""
        child = el.find(path)
        return (child.text or "").strip() if child is not None else ""

    @staticmethod
    def _collect_un_aliases(el: ET.Element) -> list[str]:
        """Collect alias names from UN ``<ALIAS>`` elements."""
        aliases: list[str] = []
        for alias_el in el.iter("ALIAS"):
            name = alias_el.findtext("ALIAS_NAME", "").strip()
            if name:
                aliases.append(name)
        return aliases

    def normalize_un_entry(self, entry: ET.Element) -> dict[str, Any] | None:
        """Normalise a UN XML element (dispatches to individual or entity)."""
        if entry.tag == "INDIVIDUAL":
            return self._parse_un_individual(entry)
        elif entry.tag == "ENTITY":
            return self._parse_un_entity(entry)
        return None

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def deduplicate_against_ofac(
        self,
        entities: list[dict[str, Any]],
        ofac_names: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Remove entries that already exist in OFAC / OpenSanctions.

        Parameters
        ----------
        entities : list[dict]
            Normalised EU or UN sanctions entries.
        ofac_names : set[str], optional
            Lowercased names from OFAC SDN / OpenSanctions.  If not provided,
            deduplication is skipped (all entries pass through).

        Returns
        -------
        list[dict]
            Entries not found in the OFAC name set.
        """
        if ofac_names is None:
            logger.info("No OFAC name set provided — skipping deduplication")
            return entities

        unique: list[dict[str, Any]] = []
        dupes = 0
        for ent in entities:
            name_lower = ent.get("name", "").lower().strip()
            if not name_lower:
                continue

            key = hashlib.sha256(name_lower.encode()).hexdigest()

            if name_lower in ofac_names or key in self._known_hashes:
                dupes += 1
                continue

            self._known_hashes.add(key)
            unique.append(ent)

        logger.info(
            "Deduplication: %d input, %d duplicates removed, %d unique",
            len(entities),
            dupes,
            len(unique),
        )
        return unique

    # ------------------------------------------------------------------
    # Full ingestion
    # ------------------------------------------------------------------

    def ingest_all(
        self,
        ofac_names: set[str] | None = None,
    ) -> dict[str, int]:
        """Fetch EU + UN sanctions, deduplicate, and publish to Kafka.

        Topics:
          - ``mda.sanctions.eu``
          - ``mda.sanctions.un``

        Parameters
        ----------
        ofac_names : set[str], optional
            Set of lowercased names from OFAC/OpenSanctions for dedup.

        Returns
        -------
        dict[str, int]
            Count of messages published per topic.
        """
        producer = self._get_producer()
        counts: dict[str, int] = {"mda.sanctions.eu": 0, "mda.sanctions.un": 0}

        # EU sanctions
        try:
            eu_entities = self.fetch_eu_sanctions()
            eu_entities = self.deduplicate_against_ofac(eu_entities, ofac_names)
            for ent in eu_entities:
                producer.send("mda.sanctions.eu", ent)
                counts["mda.sanctions.eu"] += 1
        except Exception:
            logger.exception("Failed to ingest EU sanctions")

        # UN sanctions
        try:
            un_entities = self.fetch_un_sanctions()
            un_entities = self.deduplicate_against_ofac(un_entities, ofac_names)
            for ent in un_entities:
                producer.send("mda.sanctions.un", ent)
                counts["mda.sanctions.un"] += 1
        except Exception:
            logger.exception("Failed to ingest UN sanctions")

        self._close_producer()
        logger.info("Sanctions aggregation complete: %s", counts)
        return counts

    # ------------------------------------------------------------------
    # Polling loop
    # ------------------------------------------------------------------

    def run_polling_loop(
        self,
        interval_days: float = 1,
        ofac_names: set[str] | None = None,
    ) -> None:
        """Continuously refresh EU + UN sanctions on a daily cadence.

        Parameters
        ----------
        interval_days : float
            Hours between refresh cycles (default 1 day).
        ofac_names : set[str], optional
            OFAC/OpenSanctions name set for deduplication.
        """
        logger.info(
            "Starting sanctions aggregator polling loop — interval %.1f d",
            interval_days,
        )

        while True:
            cycle_start = time.time()
            try:
                self.ingest_all(ofac_names=ofac_names)
            except Exception:
                logger.exception("Error in sanctions aggregation cycle")

            elapsed = time.time() - cycle_start
            sleep_secs = max(0, interval_days * 86400 - elapsed)
            logger.info(
                "Sanctions cycle finished in %.1f s — sleeping %.0f s",
                elapsed,
                sleep_secs,
            )
            time.sleep(sleep_secs)


# ------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    aggregator = SanctionsAggregator()
    aggregator.run_polling_loop()
