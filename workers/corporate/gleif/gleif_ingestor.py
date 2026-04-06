"""GLEIF Golden Copy File ingestion for MDA Corporate Ownership Graph.

Downloads and parses the GLEIF Level 1 (LEI-CDF) and Level 2 (RR-CDF)
Golden Copy files, loading ~3.27M LEI records and their parent-child
relationships into the Memgraph corporate ownership graph.

Sources:
  - Level 1 LEI-CDF:  https://leidata.gleif.org/api/v1/concatenated-files/lei2/get/.../zip
  - Level 2 RR-CDF:   https://leidata.gleif.org/api/v1/concatenated-files/rr/get/.../zip
  - Golden Copy API:   https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest

Data format: XML (ISO 17442), streamed with lxml iterparse for memory efficiency.
"""

import asyncio
import hashlib
import json
import logging
import os
import tempfile
import zipfile
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional
from xml.etree.ElementTree import Element

import aiohttp
from lxml import etree
from kafka import KafkaProducer
from gqlalchemy import Memgraph

logger = logging.getLogger("mda.worker.corporate.gleif")

# ── Configuration ───────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))
DATA_DIR = Path(os.getenv("GLEIF_DATA_DIR", "/data/gleif"))

GLEIF_GOLDEN_COPY_API = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest"
GLEIF_LEI_CDF_URL = os.getenv(
    "GLEIF_LEI_CDF_URL",
    "https://leidata.gleif.org/api/v1/concatenated-files/lei2/get/30447/zip",
)
GLEIF_RR_CDF_URL = os.getenv(
    "GLEIF_RR_CDF_URL",
    "https://leidata.gleif.org/api/v1/concatenated-files/rr/get/30450/zip",
)

BATCH_SIZE = 500
DOWNLOAD_CHUNK_SIZE = 1024 * 1024  # 1 MB chunks

# GLEIF LEI-CDF XML namespace
LEI_NS = "http://www.gleif.org/data/schema/leidata/2016"
RR_NS = "http://www.gleif.org/data/schema/rr/2016"


# ── Data Classes ────────────────────────────────────────────


@dataclass
class LEIRecord:
    """Represents a single LEI registration record (Level 1 LEI-CDF)."""

    lei: str
    legal_name: str
    legal_name_language: str = ""
    other_names: list[str] = field(default_factory=list)
    legal_address_line1: str = ""
    legal_address_line2: str = ""
    legal_address_city: str = ""
    legal_address_region: str = ""
    legal_address_country: str = ""
    legal_address_postal_code: str = ""
    hq_address_line1: str = ""
    hq_address_line2: str = ""
    hq_address_city: str = ""
    hq_address_region: str = ""
    hq_address_country: str = ""
    hq_address_postal_code: str = ""
    jurisdiction: str = ""
    legal_form_code: str = ""
    legal_form_other: str = ""
    entity_status: str = ""
    entity_category: str = ""
    registration_authority_id: str = ""
    registration_authority_entity_id: str = ""
    initial_registration_date: str = ""
    last_update_date: str = ""
    next_renewal_date: str = ""
    registration_status: str = ""
    managing_lou: str = ""
    corroboration_level: str = ""
    validation_sources: str = ""


@dataclass
class RelationshipRecord:
    """Represents a Level 2 relationship record (parent-child)."""

    start_lei: str
    end_lei: str
    relationship_type: str  # IS_DIRECTLY_CONSOLIDATED_BY or IS_ULTIMATELY_CONSOLIDATED_BY
    relationship_status: str = ""
    start_date: str = ""
    end_date: str = ""
    qualification: str = ""
    quantifier: str = ""
    managing_lou: str = ""
    validation_sources: str = ""
    initial_registration_date: str = ""
    last_update_date: str = ""


# ── Downloader ──────────────────────────────────────────────


class GLEIFGoldenCopyDownloader:
    """Async downloader for GLEIF Golden Copy ZIP files with SHA256 verification."""

    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

    async def download_with_checksum(
        self,
        url: str,
        filename: str,
        expected_sha256: Optional[str] = None,
    ) -> Path:
        """Download a file with streaming and optional SHA256 checksum verification.

        Args:
            url: URL to download from.
            filename: Local filename to save as.
            expected_sha256: If provided, verify file integrity after download.

        Returns:
            Path to the downloaded file.

        Raises:
            ValueError: If checksum does not match.
            aiohttp.ClientError: If download fails.
        """
        output_path = self.data_dir / filename
        sha256_hash = hashlib.sha256()
        total_bytes = 0

        logger.info("Downloading GLEIF file: %s -> %s", url, output_path)

        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=3600)) as response:
                response.raise_for_status()
                content_length = response.headers.get("Content-Length")
                if content_length:
                    logger.info("Expected size: %.1f MB", int(content_length) / (1024 * 1024))

                with open(output_path, "wb") as f:
                    async for chunk in response.content.iter_chunked(DOWNLOAD_CHUNK_SIZE):
                        f.write(chunk)
                        sha256_hash.update(chunk)
                        total_bytes += len(chunk)

        actual_sha256 = sha256_hash.hexdigest()
        logger.info(
            "Download complete: %s (%.1f MB, SHA256: %s)",
            filename,
            total_bytes / (1024 * 1024),
            actual_sha256,
        )

        if expected_sha256 and actual_sha256 != expected_sha256:
            output_path.unlink(missing_ok=True)
            raise ValueError(
                f"SHA256 mismatch for {filename}: "
                f"expected {expected_sha256}, got {actual_sha256}"
            )

        return output_path

    async def fetch_latest_publish_info(self) -> dict:
        """Fetch the latest Golden Copy publish metadata from GLEIF API.

        Returns:
            Dict with 'lei2' and 'rr' download URLs and checksums.
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(GLEIF_GOLDEN_COPY_API) as response:
                response.raise_for_status()
                data = await response.json()

        publish_info = {}
        for item in data.get("data", []):
            file_type = item.get("type", "")
            publish_info[file_type] = {
                "url": item.get("attributes", {}).get("download_url", ""),
                "sha256": item.get("attributes", {}).get("sha256", ""),
                "publish_date": item.get("attributes", {}).get("publish_date", ""),
                "record_count": item.get("attributes", {}).get("record_count", 0),
            }

        return publish_info

    async def download_golden_copy(self) -> tuple[Path, Path]:
        """Download both Level 1 (LEI-CDF) and Level 2 (RR-CDF) Golden Copy files.

        Returns:
            Tuple of (lei_xml_path, rr_xml_path) after extraction.
        """
        try:
            publish_info = await self.fetch_latest_publish_info()
            lei_info = publish_info.get("lei2", {})
            rr_info = publish_info.get("rr", {})
            lei_url = lei_info.get("url", GLEIF_LEI_CDF_URL)
            lei_sha = lei_info.get("sha256")
            rr_url = rr_info.get("url", GLEIF_RR_CDF_URL)
            rr_sha = rr_info.get("sha256")
        except Exception:
            logger.warning("Could not fetch publish info, using default URLs")
            lei_url = GLEIF_LEI_CDF_URL
            lei_sha = None
            rr_url = GLEIF_RR_CDF_URL
            rr_sha = None

        lei_zip_path, rr_zip_path = await asyncio.gather(
            self.download_with_checksum(lei_url, "lei-cdf-golden-copy.zip", lei_sha),
            self.download_with_checksum(rr_url, "rr-cdf-golden-copy.zip", rr_sha),
        )

        lei_xml_path = self._extract_zip(lei_zip_path, "lei")
        rr_xml_path = self._extract_zip(rr_zip_path, "rr")

        return lei_xml_path, rr_xml_path

    def _extract_zip(self, zip_path: Path, prefix: str) -> Path:
        """Extract the first XML file from a ZIP archive.

        Args:
            zip_path: Path to the ZIP file.
            prefix: Prefix for naming extracted XML.

        Returns:
            Path to the extracted XML file.
        """
        extract_dir = self.data_dir / f"{prefix}_extracted"
        extract_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, "r") as zf:
            xml_files = [n for n in zf.namelist() if n.endswith(".xml")]
            if not xml_files:
                raise FileNotFoundError(f"No XML files found in {zip_path}")

            target = xml_files[0]
            zf.extract(target, extract_dir)
            extracted_path = extract_dir / target

        logger.info("Extracted: %s -> %s", zip_path.name, extracted_path)
        return extracted_path


# ── Level 1 LEI XML Parser ─────────────────────────────────


class LEIXMLParser:
    """Memory-efficient streaming parser for GLEIF Level 1 LEI-CDF XML.

    Uses lxml iterparse to handle the full ~3.27M record Golden Copy
    without loading the entire DOM into memory.
    """

    # Tag names with namespace prefix
    LEI_RECORD_TAG = f"{{{LEI_NS}}}LEIRecord"
    ENTITY_TAG = f"{{{LEI_NS}}}Entity"
    REGISTRATION_TAG = f"{{{LEI_NS}}}Registration"

    def _text(self, element: Optional[Element], tag: str, ns: str = LEI_NS) -> str:
        """Safely extract text from a child element."""
        if element is None:
            return ""
        child = element.find(f"{{{ns}}}{tag}")
        if child is not None and child.text:
            return child.text.strip()
        return ""

    def _parse_address(self, entity_el: Element, address_tag: str) -> dict:
        """Parse an address block from the entity element."""
        addr = entity_el.find(f"{{{LEI_NS}}}{address_tag}")
        if addr is None:
            return {}

        lines = []
        line1 = self._text(addr, "FirstAddressLine")
        if line1:
            lines.append(line1)
        additional = addr.find(f"{{{LEI_NS}}}AdditionalAddressLine")
        line2 = additional.text.strip() if additional is not None and additional.text else ""

        return {
            "line1": line1,
            "line2": line2,
            "city": self._text(addr, "City"),
            "region": self._text(addr, "Region"),
            "country": self._text(addr, "Country"),
            "postal_code": self._text(addr, "PostalCode"),
        }

    def stream_parse(self, xml_path: Path) -> Iterator[LEIRecord]:
        """Stream-parse the LEI-CDF XML file, yielding LEIRecord objects.

        Uses iterparse with element cleanup to maintain constant memory
        usage regardless of file size.

        Args:
            xml_path: Path to the Level 1 LEI-CDF XML file.

        Yields:
            LEIRecord for each <LEIRecord> element in the file.
        """
        count = 0
        context = etree.iterparse(
            str(xml_path),
            events=("end",),
            tag=self.LEI_RECORD_TAG,
            recover=True,
        )

        for event, elem in context:
            try:
                record = self._parse_lei_element(elem)
                if record:
                    yield record
                    count += 1
                    if count % 100_000 == 0:
                        logger.info("Parsed %d LEI records", count)
            except Exception as e:
                lei_el = elem.find(f"{{{LEI_NS}}}LEI")
                lei_val = lei_el.text if lei_el is not None and lei_el.text else "UNKNOWN"
                logger.warning("Failed to parse LEI record %s: %s", lei_val, e)
            finally:
                # Critical: clear element to free memory
                elem.clear()
                # Also clear preceding siblings to prevent memory leak
                while elem.getprevious() is not None:
                    parent = elem.getparent()
                    if parent is not None:
                        parent.remove(elem.getprevious())

        logger.info("LEI XML parsing complete: %d records", count)

    def _parse_lei_element(self, elem: Element) -> Optional[LEIRecord]:
        """Parse a single <LEIRecord> XML element into an LEIRecord dataclass."""
        lei = self._text(elem, "LEI")
        if not lei:
            return None

        entity = elem.find(f"{{{LEI_NS}}}Entity")
        registration = elem.find(f"{{{LEI_NS}}}Registration")

        if entity is None:
            return None

        # Legal name
        legal_name_el = entity.find(f"{{{LEI_NS}}}LegalName")
        legal_name = ""
        legal_name_lang = ""
        if legal_name_el is not None:
            legal_name = legal_name_el.text.strip() if legal_name_el.text else ""
            legal_name_lang = legal_name_el.get("{http://www.w3.org/XML/1998/namespace}lang", "")

        # Other names (trading names, transliterations)
        other_names = []
        other_names_el = entity.find(f"{{{LEI_NS}}}OtherEntityNames")
        if other_names_el is not None:
            for name_el in other_names_el.findall(f"{{{LEI_NS}}}OtherEntityName"):
                if name_el.text:
                    other_names.append(name_el.text.strip())

        # Transliterated names
        transliterated_el = entity.find(f"{{{LEI_NS}}}TransliteratedOtherEntityNames")
        if transliterated_el is not None:
            for name_el in transliterated_el.findall(f"{{{LEI_NS}}}TransliteratedOtherEntityName"):
                if name_el.text:
                    other_names.append(name_el.text.strip())

        # Addresses
        legal_addr = self._parse_address(entity, "LegalAddress")
        hq_addr = self._parse_address(entity, "HeadquartersAddress")

        # Jurisdiction and legal form
        jurisdiction = self._text(entity, "LegalJurisdiction")
        legal_form_el = entity.find(f"{{{LEI_NS}}}LegalForm")
        legal_form_code = ""
        legal_form_other = ""
        if legal_form_el is not None:
            legal_form_code = self._text(legal_form_el, "EntityLegalFormCode")
            legal_form_other = self._text(legal_form_el, "OtherLegalForm")

        # Entity status and category
        entity_status = self._text(entity, "EntityStatus")
        entity_category = self._text(entity, "EntityCategory")

        # Registration authority
        reg_auth = entity.find(f"{{{LEI_NS}}}RegistrationAuthority")
        reg_auth_id = ""
        reg_auth_entity_id = ""
        if reg_auth is not None:
            reg_auth_id = self._text(reg_auth, "RegistrationAuthorityID")
            reg_auth_entity_id = self._text(reg_auth, "RegistrationAuthorityEntityID")

        # Registration section
        initial_reg_date = ""
        last_update = ""
        next_renewal = ""
        reg_status = ""
        managing_lou = ""
        corroboration_level = ""
        validation_sources = ""

        if registration is not None:
            initial_reg_date = self._text(registration, "InitialRegistrationDate")
            last_update = self._text(registration, "LastUpdateDate")
            next_renewal = self._text(registration, "NextRenewalDate")
            reg_status = self._text(registration, "RegistrationStatus")
            managing_lou = self._text(registration, "ManagingLOU")
            corroboration_level = self._text(registration, "CorroborationLevel")

            val_sources = registration.find(f"{{{LEI_NS}}}ValidationSources")
            if val_sources is not None and val_sources.text:
                validation_sources = val_sources.text.strip()

        return LEIRecord(
            lei=lei,
            legal_name=legal_name,
            legal_name_language=legal_name_lang,
            other_names=other_names,
            legal_address_line1=legal_addr.get("line1", ""),
            legal_address_line2=legal_addr.get("line2", ""),
            legal_address_city=legal_addr.get("city", ""),
            legal_address_region=legal_addr.get("region", ""),
            legal_address_country=legal_addr.get("country", ""),
            legal_address_postal_code=legal_addr.get("postal_code", ""),
            hq_address_line1=hq_addr.get("line1", ""),
            hq_address_line2=hq_addr.get("line2", ""),
            hq_address_city=hq_addr.get("city", ""),
            hq_address_region=hq_addr.get("region", ""),
            hq_address_country=hq_addr.get("country", ""),
            hq_address_postal_code=hq_addr.get("postal_code", ""),
            jurisdiction=jurisdiction,
            legal_form_code=legal_form_code,
            legal_form_other=legal_form_other,
            entity_status=entity_status,
            entity_category=entity_category,
            registration_authority_id=reg_auth_id,
            registration_authority_entity_id=reg_auth_entity_id,
            initial_registration_date=initial_reg_date,
            last_update_date=last_update,
            next_renewal_date=next_renewal,
            registration_status=reg_status,
            managing_lou=managing_lou,
            corroboration_level=corroboration_level,
            validation_sources=validation_sources,
        )


# ── Level 2 Relationship Record Parser ─────────────────────


class GLEIFLevel2Parser:
    """Memory-efficient streaming parser for GLEIF Level 2 RR-CDF XML.

    Parses direct and ultimate parent relationship records from the
    Relationship Record Concatenated Data File.
    """

    RR_RECORD_TAG = f"{{{RR_NS}}}RelationshipRecord"

    def _text(self, element: Optional[Element], tag: str, ns: str = RR_NS) -> str:
        """Safely extract text from a child element."""
        if element is None:
            return ""
        child = element.find(f"{{{ns}}}{tag}")
        if child is not None and child.text:
            return child.text.strip()
        return ""

    def stream_parse(self, xml_path: Path) -> Iterator[RelationshipRecord]:
        """Stream-parse the Level 2 RR-CDF XML file.

        Args:
            xml_path: Path to the Level 2 RR-CDF XML file.

        Yields:
            RelationshipRecord for each <RelationshipRecord> element.
        """
        count = 0
        context = etree.iterparse(
            str(xml_path),
            events=("end",),
            tag=self.RR_RECORD_TAG,
            recover=True,
        )

        for event, elem in context:
            try:
                record = self._parse_rr_element(elem)
                if record:
                    yield record
                    count += 1
                    if count % 50_000 == 0:
                        logger.info("Parsed %d relationship records", count)
            except Exception as e:
                logger.warning("Failed to parse RR record: %s", e)
            finally:
                elem.clear()
                while elem.getprevious() is not None:
                    parent = elem.getparent()
                    if parent is not None:
                        parent.remove(elem.getprevious())

        logger.info("RR XML parsing complete: %d records", count)

    def _parse_rr_element(self, elem: Element) -> Optional[RelationshipRecord]:
        """Parse a single <RelationshipRecord> XML element."""
        relationship = elem.find(f"{{{RR_NS}}}Relationship")
        if relationship is None:
            return None

        # Start node (child entity)
        start_node = relationship.find(f"{{{RR_NS}}}StartNode")
        start_lei = ""
        if start_node is not None:
            start_lei = self._text(start_node, "NodeID")

        # End node (parent entity)
        end_node = relationship.find(f"{{{RR_NS}}}EndNode")
        end_lei = ""
        if end_node is not None:
            end_lei = self._text(end_node, "NodeID")

        if not start_lei or not end_lei:
            return None

        # Relationship type
        rel_type = self._text(relationship, "RelationshipType")

        # Relationship status
        rel_status = self._text(relationship, "RelationshipStatus")

        # Periods
        start_date = ""
        end_date = ""
        periods = relationship.find(f"{{{RR_NS}}}RelationshipPeriods")
        if periods is not None:
            period = periods.find(f"{{{RR_NS}}}RelationshipPeriod")
            if period is not None:
                start_el = period.find(f"{{{RR_NS}}}StartDate")
                if start_el is not None and start_el.text:
                    start_date = start_el.text.strip()
                end_el = period.find(f"{{{RR_NS}}}EndDate")
                if end_el is not None and end_el.text:
                    end_date = end_el.text.strip()

        # Qualifiers
        qualification = ""
        quantifier = ""
        qualifiers = relationship.find(f"{{{RR_NS}}}RelationshipQualifiers")
        if qualifiers is not None:
            qual = qualifiers.find(f"{{{RR_NS}}}RelationshipQualifier")
            if qual is not None:
                qualification = self._text(qual, "QualifierDimension")
                quantifier = self._text(qual, "QualifierCategory")

        # Registration
        registration = elem.find(f"{{{RR_NS}}}Registration")
        managing_lou = ""
        validation_sources = ""
        initial_reg_date = ""
        last_update = ""

        if registration is not None:
            managing_lou = self._text(registration, "ManagingLOU")
            initial_reg_date = self._text(registration, "InitialRegistrationDate")
            last_update = self._text(registration, "LastUpdateDate")
            val_el = registration.find(f"{{{RR_NS}}}ValidationSources")
            if val_el is not None and val_el.text:
                validation_sources = val_el.text.strip()

        return RelationshipRecord(
            start_lei=start_lei,
            end_lei=end_lei,
            relationship_type=rel_type,
            relationship_status=rel_status,
            start_date=start_date,
            end_date=end_date,
            qualification=qualification,
            quantifier=quantifier,
            managing_lou=managing_lou,
            validation_sources=validation_sources,
            initial_registration_date=initial_reg_date,
            last_update_date=last_update,
        )


# ── Memgraph Loader ────────────────────────────────────────


class GLEIFMemgraphLoader:
    """Batch loader for GLEIF data into Memgraph corporate ownership graph.

    Uses MERGE operations to upsert Company nodes, Address nodes,
    and relationship edges. Processes in batches of BATCH_SIZE for
    optimal throughput.
    """

    def __init__(
        self,
        host: str = MEMGRAPH_HOST,
        port: int = MEMGRAPH_PORT,
        batch_size: int = BATCH_SIZE,
    ):
        self.memgraph = Memgraph(host=host, port=port)
        self.batch_size = batch_size
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )

    def load_lei_records(self, records: Iterator[LEIRecord]) -> int:
        """Load LEI records into Memgraph as Company nodes with Address relationships.

        Args:
            records: Iterator of LEIRecord objects.

        Returns:
            Total number of records loaded.
        """
        batch: list[LEIRecord] = []
        total_loaded = 0

        for record in records:
            batch.append(record)
            if len(batch) >= self.batch_size:
                self._flush_lei_batch(batch)
                total_loaded += len(batch)
                if total_loaded % 10_000 == 0:
                    logger.info("Loaded %d LEI records into Memgraph", total_loaded)
                batch = []

        # Flush remaining
        if batch:
            self._flush_lei_batch(batch)
            total_loaded += len(batch)

        logger.info("LEI loading complete: %d records", total_loaded)
        return total_loaded

    def _flush_lei_batch(self, batch: list[LEIRecord]) -> None:
        """MERGE a batch of LEI records into Memgraph."""
        for record in batch:
            try:
                # MERGE Company node
                query = """
                MERGE (c:Company {lei: $lei})
                SET c.legal_name = $legal_name,
                    c.legal_name_language = $legal_name_language,
                    c.other_names = $other_names,
                    c.jurisdiction = $jurisdiction,
                    c.legal_form_code = $legal_form_code,
                    c.legal_form_other = $legal_form_other,
                    c.entity_status = $entity_status,
                    c.entity_category = $entity_category,
                    c.registration_authority_id = $registration_authority_id,
                    c.registration_authority_entity_id = $registration_authority_entity_id,
                    c.initial_registration_date = $initial_registration_date,
                    c.last_update_date = $last_update_date,
                    c.next_renewal_date = $next_renewal_date,
                    c.registration_status = $registration_status,
                    c.lei_status = $registration_status,
                    c.managing_lou = $managing_lou,
                    c.corroboration_level = $corroboration_level,
                    c.validation_sources = $validation_sources,
                    c.country_iso = $legal_address_country,
                    c.status = $entity_status,
                    c.source = 'gleif',
                    c.entity_id = 'gleif_' + $lei,
                    c.updated_at = localDateTime()
                """
                self.memgraph.execute(query, asdict(record))

                # MERGE Legal Address node
                if record.legal_address_country:
                    addr_id = f"addr_legal_{record.lei}"
                    addr_query = """
                    MERGE (a:Address {address_id: $address_id})
                    SET a.address_type = 'LEGAL',
                        a.line1 = $line1,
                        a.line2 = $line2,
                        a.city = $city,
                        a.region = $region,
                        a.country_iso = $country,
                        a.postal_code = $postal_code,
                        a.source = 'gleif'
                    WITH a
                    MATCH (c:Company {lei: $lei})
                    MERGE (c)-[:HAS_LEGAL_ADDRESS]->(a)
                    """
                    self.memgraph.execute(addr_query, {
                        "address_id": addr_id,
                        "line1": record.legal_address_line1,
                        "line2": record.legal_address_line2,
                        "city": record.legal_address_city,
                        "region": record.legal_address_region,
                        "country": record.legal_address_country,
                        "postal_code": record.legal_address_postal_code,
                        "lei": record.lei,
                    })

                # MERGE HQ Address node (if different from legal)
                if record.hq_address_country:
                    hq_addr_id = f"addr_hq_{record.lei}"
                    hq_query = """
                    MERGE (a:Address {address_id: $address_id})
                    SET a.address_type = 'HEADQUARTERS',
                        a.line1 = $line1,
                        a.line2 = $line2,
                        a.city = $city,
                        a.region = $region,
                        a.country_iso = $country,
                        a.postal_code = $postal_code,
                        a.source = 'gleif'
                    WITH a
                    MATCH (c:Company {lei: $lei})
                    MERGE (c)-[:HAS_HQ_ADDRESS]->(a)
                    """
                    self.memgraph.execute(hq_query, {
                        "address_id": hq_addr_id,
                        "line1": record.hq_address_line1,
                        "line2": record.hq_address_line2,
                        "city": record.hq_address_city,
                        "region": record.hq_address_region,
                        "country": record.hq_address_country,
                        "postal_code": record.hq_address_postal_code,
                        "lei": record.lei,
                    })

                # Emit to Kafka
                self.kafka_producer.send(
                    "mda.corporate.gleif.lei_records",
                    {
                        "lei": record.lei,
                        "legal_name": record.legal_name,
                        "jurisdiction": record.jurisdiction,
                        "entity_status": record.entity_status,
                        "country_iso": record.legal_address_country,
                        "source": "gleif",
                        "ingest_time": datetime.utcnow().isoformat(),
                    },
                )
            except Exception as e:
                logger.error("Failed to load LEI %s: %s", record.lei, e)
                self.kafka_producer.send(
                    "mda.dlq",
                    {
                        "source": "gleif_lei",
                        "lei": record.lei,
                        "error": str(e),
                        "timestamp": datetime.utcnow().isoformat(),
                    },
                )

    def load_relationships(self, records: Iterator[RelationshipRecord]) -> int:
        """Load Level 2 relationship records into Memgraph.

        Creates OWNS_DIRECTLY and OWNS_ULTIMATELY edges between Company nodes.

        Args:
            records: Iterator of RelationshipRecord objects.

        Returns:
            Total number of relationships loaded.
        """
        batch: list[RelationshipRecord] = []
        total_loaded = 0

        for record in records:
            batch.append(record)
            if len(batch) >= self.batch_size:
                self._flush_relationship_batch(batch)
                total_loaded += len(batch)
                if total_loaded % 10_000 == 0:
                    logger.info("Loaded %d relationship records", total_loaded)
                batch = []

        if batch:
            self._flush_relationship_batch(batch)
            total_loaded += len(batch)

        logger.info("Relationship loading complete: %d records", total_loaded)
        return total_loaded

    def _flush_relationship_batch(self, batch: list[RelationshipRecord]) -> None:
        """MERGE a batch of relationship records into Memgraph."""
        for record in batch:
            try:
                # Map GLEIF relationship types to edge labels
                if record.relationship_type == "IS_DIRECTLY_CONSOLIDATED_BY":
                    edge_label = "OWNS_DIRECTLY"
                elif record.relationship_type == "IS_ULTIMATELY_CONSOLIDATED_BY":
                    edge_label = "OWNS_ULTIMATELY"
                else:
                    edge_label = "RELATED_TO"

                # Ensure both Company nodes exist (MERGE on LEI)
                # The parent (end_lei) OWNS the child (start_lei)
                query = f"""
                MERGE (child:Company {{lei: $start_lei}})
                MERGE (parent:Company {{lei: $end_lei}})
                MERGE (parent)-[r:{edge_label}]->(child)
                SET r.relationship_type = $relationship_type,
                    r.relationship_status = $relationship_status,
                    r.start_date = $start_date,
                    r.end_date = $end_date,
                    r.qualification = $qualification,
                    r.quantifier = $quantifier,
                    r.managing_lou = $managing_lou,
                    r.validation_sources = $validation_sources,
                    r.source = 'gleif_rr',
                    r.updated_at = localDateTime()
                """
                self.memgraph.execute(query, {
                    "start_lei": record.start_lei,
                    "end_lei": record.end_lei,
                    "relationship_type": record.relationship_type,
                    "relationship_status": record.relationship_status,
                    "start_date": record.start_date,
                    "end_date": record.end_date,
                    "qualification": record.qualification,
                    "quantifier": record.quantifier,
                    "managing_lou": record.managing_lou,
                    "validation_sources": record.validation_sources,
                })

                # Emit to Kafka
                self.kafka_producer.send(
                    "mda.corporate.gleif.relationships",
                    {
                        "child_lei": record.start_lei,
                        "parent_lei": record.end_lei,
                        "relationship_type": record.relationship_type,
                        "relationship_status": record.relationship_status,
                        "source": "gleif_rr",
                        "ingest_time": datetime.utcnow().isoformat(),
                    },
                )
            except Exception as e:
                logger.error(
                    "Failed to load relationship %s -> %s: %s",
                    record.start_lei,
                    record.end_lei,
                    e,
                )
                self.kafka_producer.send(
                    "mda.dlq",
                    {
                        "source": "gleif_rr",
                        "start_lei": record.start_lei,
                        "end_lei": record.end_lei,
                        "error": str(e),
                        "timestamp": datetime.utcnow().isoformat(),
                    },
                )

    def close(self) -> None:
        """Flush Kafka producer and clean up."""
        self.kafka_producer.flush()
        self.kafka_producer.close()


# ── Main Entry Point ────────────────────────────────────────


async def run_gleif_ingestion() -> None:
    """Main async entry point for GLEIF Golden Copy ingestion.

    1. Downloads Level 1 and Level 2 Golden Copy ZIP files.
    2. Extracts and stream-parses XML.
    3. Batch-loads into Memgraph.
    4. Emits events to Kafka.
    """
    logger.info("Starting GLEIF Golden Copy ingestion")

    # Download
    downloader = GLEIFGoldenCopyDownloader()
    lei_xml_path, rr_xml_path = await downloader.download_golden_copy()

    # Parse and load Level 1 (LEI records)
    logger.info("Parsing Level 1 LEI-CDF: %s", lei_xml_path)
    lei_parser = LEIXMLParser()
    loader = GLEIFMemgraphLoader()

    try:
        lei_records = lei_parser.stream_parse(lei_xml_path)
        lei_count = loader.load_lei_records(lei_records)
        logger.info("Level 1 complete: %d LEI records loaded", lei_count)

        # Parse and load Level 2 (Relationship records)
        logger.info("Parsing Level 2 RR-CDF: %s", rr_xml_path)
        rr_parser = GLEIFLevel2Parser()
        rr_records = rr_parser.stream_parse(rr_xml_path)
        rr_count = loader.load_relationships(rr_records)
        logger.info("Level 2 complete: %d relationship records loaded", rr_count)

        # Emit completion event
        loader.kafka_producer.send(
            "mda.corporate.gleif.ingestion_complete",
            {
                "lei_records_loaded": lei_count,
                "relationships_loaded": rr_count,
                "timestamp": datetime.utcnow().isoformat(),
                "source": "gleif_golden_copy",
            },
        )
    finally:
        loader.close()

    logger.info("GLEIF ingestion complete")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    asyncio.run(run_gleif_ingestion())
