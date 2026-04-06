"""
UK Companies House data ingester.

Parses BasicCompanyData CSVs, PSC JSON snapshots, and queries the
Companies House REST API.  Produces Kafka messages and writes
Company / Ownership nodes into Memgraph.
"""

from __future__ import annotations

import base64
import csv
import io
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Tuple

import requests
from confluent_kafka import Producer
from mgclient import connect as mg_connect

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = "mda.corporate.companies_house.raw"
MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "memgraph")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))
CH_API_KEY = os.getenv("CH_API_KEY", "")
CH_API_BASE = "https://api.company-information.service.gov.uk"
RATE_LIMIT_PER_MIN = 600

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

STATUS_MAP: Dict[str, str] = {
    "Active": "active",
    "Active - Proposal to Strike off": "active_pso",
    "Dissolved": "dissolved",
    "Liquidation": "liquidation",
    "Administration": "administration",
    "Voluntary Arrangement": "voluntary_arrangement",
    "Converted/Closed": "converted_closed",
    "Insolvency Proceedings": "insolvency",
    "Registered": "active",
    "Removed": "dissolved",
    "": "unknown",
}

PSC_OWNERSHIP_BANDS: Dict[str, Tuple[float, float, float]] = {
    "ownership-of-shares-25-to-50-percent": (25.0, 50.0, 37.5),
    "ownership-of-shares-50-to-75-percent": (50.0, 75.0, 62.5),
    "ownership-of-shares-75-to-100-percent": (75.0, 100.0, 87.5),
    "ownership-of-shares-25-to-50-percent-as-trust": (25.0, 50.0, 37.5),
    "ownership-of-shares-50-to-75-percent-as-trust": (50.0, 75.0, 62.5),
    "ownership-of-shares-75-to-100-percent-as-trust": (75.0, 100.0, 87.5),
    "ownership-of-shares-25-to-50-percent-as-firm": (25.0, 50.0, 37.5),
    "ownership-of-shares-50-to-75-percent-as-firm": (50.0, 75.0, 62.5),
    "ownership-of-shares-75-to-100-percent-as-firm": (75.0, 100.0, 87.5),
}

PSC_KIND_MAP: Dict[str, str] = {
    "individual-person-with-significant-control": "person",
    "corporate-entity-person-with-significant-control": "company",
    "legal-person-person-with-significant-control": "organization",
    "super-secure-person-with-significant-control": "person",
    "individual-beneficial-owner": "person",
    "corporate-entity-beneficial-owner": "company",
    "legal-person-beneficial-owner": "organization",
}


def _parse_date_ddmmyyyy(val: str) -> Optional[str]:
    """Parse DD/MM/YYYY to ISO-8601 date string."""
    val = val.strip()
    if not val:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(val, fmt).date().isoformat()
        except ValueError:
            continue
    logger.warning("Unparseable date: %s", val)
    return None


def _extract_sic(raw: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract SIC code and description from '12345 - Description' format."""
    raw = raw.strip()
    if not raw:
        return None, None
    m = re.match(r"^(\d{4,5})\s*-\s*(.+)$", raw)
    if m:
        return m.group(1), m.group(2).strip()
    if raw.isdigit():
        return raw, None
    return None, raw


def _build_address(row: Dict[str, str], prefix: str = "RegAddress.") -> Dict[str, str]:
    """Build a normalised address dict from multiple CSV columns."""
    fields = [
        "CareOf", "POBox", "AddressLine1", "AddressLine2",
        "PostTown", "County", "Country", "PostCode",
    ]
    addr: Dict[str, str] = {}
    for f in fields:
        val = row.get(f"{prefix}{f}", "").strip()
        if val:
            addr[f.lower()] = val
    parts = [addr.get(k, "") for k in ("addressline1", "addressline2", "posttown", "county", "postcode", "country")]
    addr["full"] = ", ".join(p for p in parts if p)
    return addr


# ---------------------------------------------------------------------------
# CSV Parser – BasicCompanyData
# ---------------------------------------------------------------------------


@dataclass
class CompanyRecord:
    company_number: str
    name: str
    status: str
    company_type: str
    incorporation_date: Optional[str]
    dissolution_date: Optional[str]
    sic_codes: List[Tuple[Optional[str], Optional[str]]]
    registered_address: Dict[str, str]
    country_of_origin: str
    raw: Dict[str, str] = field(default_factory=dict)


class CompaniesHouseCSVParser:
    """Parse BasicCompanyData-*.csv bulk files."""

    SIC_COLUMNS = [
        "SICCode.SicText_1", "SICCode.SicText_2",
        "SICCode.SicText_3", "SICCode.SicText_4",
    ]

    def __init__(self, path: str, encoding: str = "utf-8-sig") -> None:
        self.path = path
        self.encoding = encoding

    def parse(self) -> Generator[CompanyRecord, None, None]:
        with open(self.path, "r", encoding=self.encoding, errors="replace") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                yield self._to_record(row)

    def _to_record(self, row: Dict[str, str]) -> CompanyRecord:
        raw_status = row.get("CompanyStatus", "").strip()
        status = STATUS_MAP.get(raw_status, raw_status.lower().replace(" ", "_"))

        sic_codes = []
        for col in self.SIC_COLUMNS:
            code, desc = _extract_sic(row.get(col, ""))
            if code or desc:
                sic_codes.append((code, desc))

        return CompanyRecord(
            company_number=row.get("CompanyNumber", "").strip(),
            name=row.get("CompanyName", "").strip(),
            status=status,
            company_type=row.get("CompanyCategory", "").strip(),
            incorporation_date=_parse_date_ddmmyyyy(row.get("IncorporationDate", "")),
            dissolution_date=_parse_date_ddmmyyyy(row.get("DissolutionDate", "")),
            sic_codes=sic_codes,
            registered_address=_build_address(row),
            country_of_origin=row.get("CountryOfOrigin", "").strip(),
            raw=dict(row),
        )


# ---------------------------------------------------------------------------
# PSC JSON Snapshot Parser
# ---------------------------------------------------------------------------

@dataclass
class PSCRecord:
    company_number: str
    psc_kind: str
    psc_type: str  # person / company / organization
    name: str
    nationality: Optional[str]
    country_of_residence: Optional[str]
    natures_of_control: List[str]
    ownership_lower: Optional[float]
    ownership_upper: Optional[float]
    ownership_midpoint: Optional[float]
    notified_on: Optional[str]
    ceased_on: Optional[str]
    identification: Optional[Dict[str, str]]
    raw: Dict[str, Any] = field(default_factory=dict)


class PSCParser:
    """Parse PSC bulk JSONL snapshot files."""

    def __init__(self, path: str) -> None:
        self.path = path

    def parse(self) -> Generator[PSCRecord, None, None]:
        with open(self.path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed JSONL line")
                    continue
                rec = self._to_record(obj)
                if rec:
                    yield rec

    def _to_record(self, obj: Dict[str, Any]) -> Optional[PSCRecord]:
        data = obj.get("data", obj)
        kind = data.get("kind", "")
        psc_type = PSC_KIND_MAP.get(kind, "unknown")

        natures = data.get("natures_of_control", [])
        lower, upper, midpoint = self._ownership_from_natures(natures)

        name_parts = data.get("name_elements", {})
        name = data.get("name", "")
        if not name and name_parts:
            name = " ".join(
                name_parts.get(k, "") for k in ("title", "forename", "other_forenames", "surname")
            ).strip()

        company_number = obj.get("company_number", data.get("company_number", ""))
        if not company_number:
            return None

        return PSCRecord(
            company_number=company_number,
            psc_kind=kind,
            psc_type=psc_type,
            name=name,
            nationality=data.get("nationality", None),
            country_of_residence=data.get("country_of_residence", None),
            natures_of_control=natures,
            ownership_lower=lower,
            ownership_upper=upper,
            ownership_midpoint=midpoint,
            notified_on=data.get("notified_on", None),
            ceased_on=data.get("ceased_on", None),
            identification=data.get("identification", None),
            raw=data,
        )

    @staticmethod
    def _ownership_from_natures(
        natures: List[str],
    ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Map natures_of_control to ownership percentage band."""
        for nature in natures:
            for pattern, (lo, hi, mid) in PSC_OWNERSHIP_BANDS.items():
                if pattern in nature:
                    return lo, hi, mid
        return None, None, None


# ---------------------------------------------------------------------------
# Companies House REST API Client
# ---------------------------------------------------------------------------

class CompaniesHouseAPIClient:
    """Thin wrapper around the Companies House REST API with rate-limiting."""

    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key = api_key or CH_API_KEY
        self.base_url = CH_API_BASE
        self._session = requests.Session()
        # HTTP Basic: key as username, empty password
        self._session.auth = (self.api_key, "")
        self._session.headers.update({"Accept": "application/json"})
        self._request_times: List[float] = []

    def _rate_limit(self) -> None:
        """Enforce 600 requests / minute sliding window."""
        now = time.time()
        window_start = now - 60.0
        self._request_times = [t for t in self._request_times if t > window_start]
        if len(self._request_times) >= RATE_LIMIT_PER_MIN:
            sleep_for = self._request_times[0] - window_start + 0.05
            logger.debug("Rate limit: sleeping %.2fs", sleep_for)
            time.sleep(sleep_for)
        self._request_times.append(time.time())

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self._rate_limit()
        url = f"{self.base_url}{path}"
        resp = self._session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_company(self, company_number: str) -> Dict[str, Any]:
        return self._get(f"/company/{company_number}")

    def get_officers(
        self, company_number: str, items_per_page: int = 100, start_index: int = 0,
    ) -> Dict[str, Any]:
        return self._get(
            f"/company/{company_number}/officers",
            params={"items_per_page": items_per_page, "start_index": start_index},
        )

    def get_pscs(
        self, company_number: str, items_per_page: int = 100, start_index: int = 0,
    ) -> Dict[str, Any]:
        return self._get(
            f"/company/{company_number}/persons-with-significant-control",
            params={"items_per_page": items_per_page, "start_index": start_index},
        )

    def search_companies(self, query: str, items_per_page: int = 20) -> Dict[str, Any]:
        return self._get("/search/companies", params={"q": query, "items_per_page": items_per_page})


# ---------------------------------------------------------------------------
# Kafka Producer
# ---------------------------------------------------------------------------

class CHKafkaProducer:
    """Publish raw company records to Kafka."""

    def __init__(self, broker: Optional[str] = None, topic: Optional[str] = None) -> None:
        self.topic = topic or KAFKA_TOPIC
        conf = {"bootstrap.servers": broker or KAFKA_BROKER, "linger.ms": 50, "batch.size": 65536}
        self._producer = Producer(conf)

    def _delivery_cb(self, err, msg):
        if err:
            logger.error("Kafka delivery failed: %s", err)

    def send_company(self, record: CompanyRecord) -> None:
        payload = json.dumps({
            "company_number": record.company_number,
            "name": record.name,
            "status": record.status,
            "company_type": record.company_type,
            "incorporation_date": record.incorporation_date,
            "dissolution_date": record.dissolution_date,
            "sic_codes": record.sic_codes,
            "registered_address": record.registered_address,
            "country_of_origin": record.country_of_origin,
            "source": "companies_house",
        })
        self._producer.produce(
            self.topic, key=record.company_number.encode(), value=payload.encode(),
            callback=self._delivery_cb,
        )
        self._producer.poll(0)

    def send_psc(self, record: PSCRecord) -> None:
        payload = json.dumps({
            "company_number": record.company_number,
            "psc_kind": record.psc_kind,
            "psc_type": record.psc_type,
            "name": record.name,
            "ownership_midpoint": record.ownership_midpoint,
            "natures_of_control": record.natures_of_control,
            "notified_on": record.notified_on,
            "ceased_on": record.ceased_on,
            "source": "companies_house_psc",
        })
        self._producer.produce(
            self.topic, key=record.company_number.encode(), value=payload.encode(),
            callback=self._delivery_cb,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> None:
        self._producer.flush(timeout)


# ---------------------------------------------------------------------------
# Memgraph Writer
# ---------------------------------------------------------------------------

class CHMemgraphWriter:
    """MERGE Company and Ownership nodes/edges into Memgraph."""

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None) -> None:
        self.host = host or MEMGRAPH_HOST
        self.port = port or MEMGRAPH_PORT
        self._conn = None

    def _ensure_conn(self):
        if self._conn is None:
            self._conn = mg_connect(host=self.host, port=self.port)
        return self._conn

    def _execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> None:
        conn = self._ensure_conn()
        cursor = conn.cursor()
        cursor.execute(query, params or {})
        conn.commit()

    def ensure_indexes(self) -> None:
        for stmt in (
            "CREATE INDEX ON :Company(company_number);",
            "CREATE INDEX ON :Person(name);",
            "CREATE INDEX ON :Company(name);",
        ):
            try:
                self._execute(stmt)
            except Exception:
                pass  # index may already exist

    def merge_company(self, rec: CompanyRecord) -> None:
        query = """
        MERGE (c:Company {company_number: $company_number})
        SET c.name = $name,
            c.status = $status,
            c.company_type = $company_type,
            c.incorporation_date = $incorporation_date,
            c.dissolution_date = $dissolution_date,
            c.jurisdiction = 'GB',
            c.address_full = $address_full,
            c.country_of_origin = $country_of_origin,
            c.source = 'companies_house',
            c.updated_at = timestamp()
        """
        sic_str = ";".join(f"{c or ''}:{d or ''}" for c, d in rec.sic_codes)
        self._execute(query, {
            "company_number": rec.company_number,
            "name": rec.name,
            "status": rec.status,
            "company_type": rec.company_type,
            "incorporation_date": rec.incorporation_date,
            "dissolution_date": rec.dissolution_date,
            "address_full": rec.registered_address.get("full", ""),
            "country_of_origin": rec.country_of_origin,
        })
        if sic_str:
            self._execute(
                "MATCH (c:Company {company_number: $cn}) SET c.sic_codes = $sic",
                {"cn": rec.company_number, "sic": sic_str},
            )

    def merge_ownership(self, rec: PSCRecord) -> None:
        if rec.psc_type == "person":
            owner_query = """
            MERGE (p:Person {name: $name})
            SET p.nationality = $nationality,
                p.country_of_residence = $country_of_residence,
                p.source = 'companies_house_psc'
            """
            self._execute(owner_query, {
                "name": rec.name,
                "nationality": rec.nationality,
                "country_of_residence": rec.country_of_residence,
            })
            edge_query = """
            MATCH (p:Person {name: $name})
            MATCH (c:Company {company_number: $company_number})
            MERGE (p)-[r:OWNS]->(c)
            SET r.ownership_lower = $lo,
                r.ownership_upper = $hi,
                r.ownership_midpoint = $mid,
                r.notified_on = $notified,
                r.ceased_on = $ceased,
                r.source = 'companies_house_psc'
            """
        else:
            owner_query = """
            MERGE (o:Company {name: $name})
            SET o.source = 'companies_house_psc'
            """
            self._execute(owner_query, {"name": rec.name})
            edge_query = """
            MATCH (o:Company {name: $name})
            MATCH (c:Company {company_number: $company_number})
            WHERE o <> c
            MERGE (o)-[r:OWNS]->(c)
            SET r.ownership_lower = $lo,
                r.ownership_upper = $hi,
                r.ownership_midpoint = $mid,
                r.notified_on = $notified,
                r.ceased_on = $ceased,
                r.source = 'companies_house_psc'
            """
        self._execute(edge_query, {
            "name": rec.name,
            "company_number": rec.company_number,
            "lo": rec.ownership_lower,
            "hi": rec.ownership_upper,
            "mid": rec.ownership_midpoint,
            "notified": rec.notified_on,
            "ceased": rec.ceased_on,
        })

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def ingest_csv(csv_path: str, write_kafka: bool = True, write_graph: bool = True) -> int:
    """Ingest a BasicCompanyData CSV file."""
    parser = CompaniesHouseCSVParser(csv_path)
    kafka = CHKafkaProducer() if write_kafka else None
    graph = CHMemgraphWriter() if write_graph else None
    if graph:
        graph.ensure_indexes()

    count = 0
    for rec in parser.parse():
        if kafka:
            kafka.send_company(rec)
        if graph:
            graph.merge_company(rec)
        count += 1
        if count % 10_000 == 0:
            logger.info("Processed %d companies", count)

    if kafka:
        kafka.flush()
    if graph:
        graph.close()
    logger.info("Finished CSV ingest: %d companies", count)
    return count


def ingest_psc(jsonl_path: str, write_kafka: bool = True, write_graph: bool = True) -> int:
    """Ingest a PSC bulk JSONL snapshot."""
    parser = PSCParser(jsonl_path)
    kafka = CHKafkaProducer() if write_kafka else None
    graph = CHMemgraphWriter() if write_graph else None

    count = 0
    for rec in parser.parse():
        if kafka:
            kafka.send_psc(rec)
        if graph:
            graph.merge_ownership(rec)
        count += 1
        if count % 10_000 == 0:
            logger.info("Processed %d PSC records", count)

    if kafka:
        kafka.flush()
    if graph:
        graph.close()
    logger.info("Finished PSC ingest: %d records", count)
    return count


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    ap = argparse.ArgumentParser(description="Companies House Ingester")
    sub = ap.add_subparsers(dest="cmd")

    csv_p = sub.add_parser("csv", help="Ingest BasicCompanyData CSV")
    csv_p.add_argument("path", help="Path to CSV file")
    csv_p.add_argument("--no-kafka", action="store_true")
    csv_p.add_argument("--no-graph", action="store_true")

    psc_p = sub.add_parser("psc", help="Ingest PSC JSONL snapshot")
    psc_p.add_argument("path", help="Path to JSONL file")
    psc_p.add_argument("--no-kafka", action="store_true")
    psc_p.add_argument("--no-graph", action="store_true")

    api_p = sub.add_parser("api", help="Query API for a single company")
    api_p.add_argument("company_number")

    args = ap.parse_args()
    if args.cmd == "csv":
        ingest_csv(args.path, write_kafka=not args.no_kafka, write_graph=not args.no_graph)
    elif args.cmd == "psc":
        ingest_psc(args.path, write_kafka=not args.no_kafka, write_graph=not args.no_graph)
    elif args.cmd == "api":
        client = CompaniesHouseAPIClient()
        print(json.dumps(client.get_company(args.company_number), indent=2))
    else:
        ap.print_help()
