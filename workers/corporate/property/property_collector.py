"""Property ownership integration for MDA Corporate Ownership Graph.

Collects property records from US county assessor open-data portals
(Socrata) and UK Land Registry bulk downloads, normalises them, and
links property nodes to corporate owners in Memgraph.

Data sources:
    - US: Socrata JSON APIs for 10 priority counties
    - UK: OCOD (Overseas Companies Ownership Data) and UKCOD (UK Companies
      Ownership Data) CSV bulk files from HM Land Registry
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd
from neo4j import GraphDatabase

logger = logging.getLogger("mda.worker.corporate.property")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MEMGRAPH_URI = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
MEMGRAPH_USER = os.getenv("MEMGRAPH_USER", "")
MEMGRAPH_PASS = os.getenv("MEMGRAPH_PASS", "")

SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
UK_OCOD_PATH = os.getenv("UK_OCOD_PATH", "/data/corporate/uk_land_registry/OCOD.csv")
UK_UKCOD_PATH = os.getenv("UK_UKCOD_PATH", "/data/corporate/uk_land_registry/UKCOD.csv")

REQUEST_TIMEOUT = int(os.getenv("PROPERTY_REQUEST_TIMEOUT", "60"))
BATCH_SIZE = int(os.getenv("PROPERTY_BATCH_SIZE", "1000"))
SOCRATA_LIMIT = int(os.getenv("SOCRATA_PAGE_LIMIT", "5000"))

# ---------------------------------------------------------------------------
# US Priority Counties
# ---------------------------------------------------------------------------

@dataclass
class CountyConfig:
    """Configuration for a single county Socrata dataset."""

    name: str
    state: str
    base_url: str
    dataset_id: str
    field_map: dict[str, str] = field(default_factory=dict)

    @property
    def api_url(self) -> str:
        return f"{self.base_url}/resource/{self.dataset_id}.json"


PRIORITY_COUNTIES: dict[str, CountyConfig] = {
    "los_angeles": CountyConfig(
        name="Los Angeles County",
        state="CA",
        base_url="https://data.lacounty.gov",
        dataset_id="9trm-uz8i",
        field_map={
            "ain": "parcel_id",
            "situsaddress": "address",
            "ownername1": "owner_name",
            "usedescription": "property_type",
            "roll_year": "year",
            "roll_landvalue": "land_value",
            "roll_impvalue": "improvement_value",
            "roll_totlandimp": "total_value",
        },
    ),
    "miami_dade": CountyConfig(
        name="Miami-Dade County",
        state="FL",
        base_url="https://opendata.miamidade.gov",
        dataset_id="uf2r-5ity",
        field_map={
            "folio": "parcel_id",
            "siteaddress": "address",
            "owner1": "owner_name",
            "dor_uc": "property_type",
            "yr": "year",
            "asd_val": "total_value",
        },
    ),
    "cook": CountyConfig(
        name="Cook County",
        state="IL",
        base_url="https://datacatalog.cookcountyil.gov",
        dataset_id="tx2p-k2g9",
        field_map={
            "pin": "parcel_id",
            "property_address": "address",
            "taxpayer_name": "owner_name",
            "class": "property_type",
            "tax_year": "year",
            "assessed_value": "total_value",
        },
    ),
    "harris": CountyConfig(
        name="Harris County",
        state="TX",
        base_url="https://opendata.houstontx.gov",
        dataset_id="q3bw-w4kv",
        field_map={
            "account": "parcel_id",
            "site_address": "address",
            "owner_name": "owner_name",
            "state_class": "property_type",
            "tax_year": "year",
            "market_value": "total_value",
        },
    ),
    "nyc": CountyConfig(
        name="New York City",
        state="NY",
        base_url="https://data.cityofnewyork.us",
        dataset_id="bnx9-e6tj",
        field_map={
            "bbl": "parcel_id",
            "address": "address",
            "ownername": "owner_name",
            "bldgclass": "property_type",
            "year": "year",
            "fullval": "total_value",
        },
    ),
    "dallas": CountyConfig(
        name="Dallas County",
        state="TX",
        base_url="https://www.dallasopendata.com",
        dataset_id="5zt2-8cxy",
        field_map={
            "account_num": "parcel_id",
            "situs_address": "address",
            "owner_name": "owner_name",
            "property_use": "property_type",
            "tax_year": "year",
            "market_value": "total_value",
        },
    ),
    "san_diego": CountyConfig(
        name="San Diego County",
        state="CA",
        base_url="https://data.sandiegocounty.gov",
        dataset_id="mhfz-r89d",
        field_map={
            "apn": "parcel_id",
            "site_address": "address",
            "owner_name": "owner_name",
            "use_code": "property_type",
            "roll_year": "year",
            "total_value": "total_value",
        },
    ),
    "broward": CountyConfig(
        name="Broward County",
        state="FL",
        base_url="https://opendata.broward.org",
        dataset_id="nm7b-jt37",
        field_map={
            "folio": "parcel_id",
            "siteaddr": "address",
            "owner1": "owner_name",
            "dor_uc": "property_type",
            "year": "year",
            "jv": "total_value",
        },
    ),
    "maricopa": CountyConfig(
        name="Maricopa County",
        state="AZ",
        base_url="https://opendata.maricopa.gov",
        dataset_id="rvh2-4w9k",
        field_map={
            "parcel_number": "parcel_id",
            "situs_address": "address",
            "owner_name": "owner_name",
            "property_type": "property_type",
            "tax_year": "year",
            "full_cash_value": "total_value",
        },
    ),
    "king": CountyConfig(
        name="King County",
        state="WA",
        base_url="https://data.kingcounty.gov",
        dataset_id="xkwe-ki8f",
        field_map={
            "pin": "parcel_id",
            "situsaddress": "address",
            "taxpayername": "owner_name",
            "proptype": "property_type",
            "taxyear": "year",
            "apprtotval": "total_value",
        },
    ),
}

# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

_LLC_PATTERN = re.compile(
    r"\b(llc|l\.l\.c|inc|corp|ltd|limited|trust|lp|llp)\b\.?",
    re.IGNORECASE,
)
_WHITESPACE = re.compile(r"\s+")


def _normalize_owner_name(name: str | None) -> str:
    """Normalise a property-owner name for fuzzy matching."""
    if not name:
        return ""
    cleaned = name.upper().strip()
    cleaned = _LLC_PATTERN.sub("", cleaned)
    cleaned = _WHITESPACE.sub(" ", cleaned).strip()
    return cleaned


def _parse_value(val: Any) -> float | None:
    """Parse a dollar-value field to float."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# US County Assessor Collector (async)
# ---------------------------------------------------------------------------


class CountyAssessorCollector:
    """Async collector that queries Socrata JSON APIs for US county
    property-ownership records and matches LLC owners to corporate
    entities in Memgraph."""

    def __init__(
        self,
        counties: dict[str, CountyConfig] | None = None,
        app_token: str | None = None,
        memgraph_uri: str | None = None,
    ):
        self._counties = counties or PRIORITY_COUNTIES
        self._app_token = app_token or SOCRATA_APP_TOKEN
        self._mg_uri = memgraph_uri or MEMGRAPH_URI
        auth = (MEMGRAPH_USER, MEMGRAPH_PASS) if MEMGRAPH_USER else None
        self._driver = GraphDatabase.driver(self._mg_uri, auth=auth)

    def close(self) -> None:
        self._driver.close()

    async def collect_all(self) -> list[dict]:
        """Collect property records from all priority counties."""
        all_records: list[dict] = []
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        ) as session:
            tasks = [
                self._collect_county(session, key, cfg)
                for key, cfg in self._counties.items()
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for county_key, result in zip(self._counties.keys(), results):
                if isinstance(result, Exception):
                    logger.error("Failed to collect %s: %s", county_key, result)
                else:
                    all_records.extend(result)

        logger.info("Collected %d total property records", len(all_records))
        return all_records

    async def _collect_county(
        self,
        session: aiohttp.ClientSession,
        county_key: str,
        config: CountyConfig,
    ) -> list[dict]:
        """Collect and normalise records for a single county."""
        records: list[dict] = []
        offset = 0

        headers: dict[str, str] = {}
        if self._app_token:
            headers["X-App-Token"] = self._app_token

        while True:
            params = {
                "$limit": str(SOCRATA_LIMIT),
                "$offset": str(offset),
                "$order": ":id",
            }
            try:
                async with session.get(
                    config.api_url, params=params, headers=headers
                ) as resp:
                    if resp.status != 200:
                        logger.warning(
                            "HTTP %d from %s (offset %d)",
                            resp.status,
                            county_key,
                            offset,
                        )
                        break
                    page = await resp.json()
            except Exception:
                logger.exception("Request failed for %s at offset %d", county_key, offset)
                break

            if not page:
                break

            for raw in page:
                normalised = self._normalize_record(raw, config)
                if normalised:
                    records.append(normalised)

            if len(page) < SOCRATA_LIMIT:
                break
            offset += SOCRATA_LIMIT

        logger.info(
            "County %s (%s): collected %d records",
            config.name,
            config.state,
            len(records),
        )
        return records

    def _normalize_record(
        self, raw: dict, config: CountyConfig
    ) -> dict | None:
        """Map source fields to canonical schema."""
        mapped: dict[str, Any] = {
            "county": config.name,
            "state": config.state,
            "country": "US",
            "source": f"socrata:{config.dataset_id}",
            "collected_at": datetime.now(timezone.utc).isoformat(),
        }
        for src_field, dst_field in config.field_map.items():
            mapped[dst_field] = raw.get(src_field)

        # Parse value
        mapped["total_value"] = _parse_value(mapped.get("total_value"))
        mapped["land_value"] = _parse_value(mapped.get("land_value"))
        mapped["improvement_value"] = _parse_value(mapped.get("improvement_value"))

        # Require at minimum a parcel id and owner name
        if not mapped.get("parcel_id") or not mapped.get("owner_name"):
            return None

        mapped["owner_name_norm"] = _normalize_owner_name(mapped.get("owner_name"))
        return mapped

    def match_owners_to_graph(
        self, records: list[dict], batch_size: int = BATCH_SIZE
    ) -> int:
        """Match LLC/corporate property owners to Company nodes in Memgraph.

        Creates Property nodes and OWNS_PROPERTY edges where a match is
        found.
        """
        merge_query = """
        UNWIND $records AS rec
        MERGE (p:Property {parcel_id: rec.parcel_id, county: rec.county, state: rec.state})
        SET p.address = rec.address,
            p.property_type = rec.property_type,
            p.total_value = rec.total_value,
            p.land_value = rec.land_value,
            p.improvement_value = rec.improvement_value,
            p.source = rec.source,
            p.collected_at = rec.collected_at
        WITH p, rec
        OPTIONAL MATCH (c:Company)
        WHERE c.name_norm = rec.owner_name_norm
           OR c.name = rec.owner_name
        WITH p, c, rec
        WHERE c IS NOT NULL
        MERGE (c)-[:OWNS_PROPERTY]->(p)
        RETURN count(p) AS matched
        """
        total_matched = 0
        with self._driver.session() as session:
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                result = session.run(merge_query, records=batch).single()
                matched = result["matched"] if result else 0
                total_matched += matched

        logger.info(
            "Matched %d property records to corporate entities", total_matched
        )
        return total_matched


# ---------------------------------------------------------------------------
# UK Land Registry Parsers
# ---------------------------------------------------------------------------


class UKLandRegistryParser:
    """Parse UK Land Registry bulk CSV files: OCOD (overseas companies
    owning UK property) and UKCOD (UK companies owning property).

    Both files support up to 4 co-proprietors per record, each with
    name, company registration number, and country of incorporation.
    """

    # Column indices for OCOD CSV (0-based)
    # Title Number, Tenure, Property Address, District, County, Region, Postcode,
    # Multiple Address Indicator, Price Paid, Proprietor Name (1),
    # Company Registration No (1), Proprietorship Category (1),
    # Country Incorporated (1), Proprietor (1) Address (1,2,3),
    # ... then Proprietor Name (2) ... up to Proprietor Name (4)

    OCOD_PROPRIETOR_OFFSETS = [
        {"name_col": 9, "reg_col": 10, "category_col": 11, "country_col": 12, "addr_start": 13},
        {"name_col": 16, "reg_col": 17, "category_col": 18, "country_col": 19, "addr_start": 20},
        {"name_col": 23, "reg_col": 24, "category_col": 25, "country_col": 26, "addr_start": 27},
        {"name_col": 30, "reg_col": 31, "category_col": 32, "country_col": 33, "addr_start": 34},
    ]

    UKCOD_PROPRIETOR_OFFSETS = [
        {"name_col": 9, "reg_col": 10, "category_col": 11, "addr_start": 12},
        {"name_col": 15, "reg_col": 16, "category_col": 17, "addr_start": 18},
        {"name_col": 21, "reg_col": 22, "category_col": 23, "addr_start": 24},
        {"name_col": 27, "reg_col": 28, "category_col": 29, "addr_start": 30},
    ]

    def parse_ocod(self, csv_path: str | None = None) -> pd.DataFrame:
        """Parse the OCOD (Overseas Companies Ownership Data) CSV.

        Returns a DataFrame with one row per proprietor-property pair.
        """
        path = Path(csv_path or UK_OCOD_PATH)
        if not path.exists():
            logger.warning("OCOD file not found: %s", path)
            return pd.DataFrame()

        records: list[dict] = []
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader, None)  # skip header

            for row_num, row in enumerate(reader, start=2):
                if len(row) < 16:
                    continue

                base = {
                    "title_number": row[0].strip(),
                    "tenure": row[1].strip(),
                    "property_address": row[2].strip(),
                    "district": row[3].strip(),
                    "county": row[4].strip(),
                    "region": row[5].strip(),
                    "postcode": row[6].strip(),
                    "multi_address": row[7].strip(),
                    "price_paid": _parse_value(row[8]),
                    "source": "OCOD",
                    "country": "GB",
                }

                for i, offsets in enumerate(self.OCOD_PROPRIETOR_OFFSETS):
                    name_col = offsets["name_col"]
                    if name_col >= len(row) or not row[name_col].strip():
                        continue

                    record = dict(base)
                    record["proprietor_number"] = i + 1
                    record["proprietor_name"] = row[name_col].strip()
                    record["company_reg_number"] = (
                        row[offsets["reg_col"]].strip()
                        if offsets["reg_col"] < len(row)
                        else ""
                    )
                    record["proprietorship_category"] = (
                        row[offsets["category_col"]].strip()
                        if offsets["category_col"] < len(row)
                        else ""
                    )
                    record["country_incorporated"] = (
                        row[offsets["country_col"]].strip()
                        if offsets["country_col"] < len(row)
                        else ""
                    )
                    # Collect up to 3 address lines
                    addr_parts = []
                    for j in range(3):
                        idx = offsets["addr_start"] + j
                        if idx < len(row) and row[idx].strip():
                            addr_parts.append(row[idx].strip())
                    record["proprietor_address"] = ", ".join(addr_parts)
                    records.append(record)

        df = pd.DataFrame(records)
        logger.info("Parsed %d proprietor-property rows from OCOD", len(df))
        return df

    def parse_ukcod(self, csv_path: str | None = None) -> pd.DataFrame:
        """Parse the UKCOD (UK Companies Ownership Data) CSV.

        Returns a DataFrame with one row per proprietor-property pair.
        """
        path = Path(csv_path or UK_UKCOD_PATH)
        if not path.exists():
            logger.warning("UKCOD file not found: %s", path)
            return pd.DataFrame()

        records: list[dict] = []
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader, None)

            for row_num, row in enumerate(reader, start=2):
                if len(row) < 15:
                    continue

                base = {
                    "title_number": row[0].strip(),
                    "tenure": row[1].strip(),
                    "property_address": row[2].strip(),
                    "district": row[3].strip(),
                    "county": row[4].strip(),
                    "region": row[5].strip(),
                    "postcode": row[6].strip(),
                    "multi_address": row[7].strip(),
                    "price_paid": _parse_value(row[8]),
                    "source": "UKCOD",
                    "country": "GB",
                }

                for i, offsets in enumerate(self.UKCOD_PROPRIETOR_OFFSETS):
                    name_col = offsets["name_col"]
                    if name_col >= len(row) or not row[name_col].strip():
                        continue

                    record = dict(base)
                    record["proprietor_number"] = i + 1
                    record["proprietor_name"] = row[name_col].strip()
                    record["company_reg_number"] = (
                        row[offsets["reg_col"]].strip()
                        if offsets["reg_col"] < len(row)
                        else ""
                    )
                    record["proprietorship_category"] = (
                        row[offsets["category_col"]].strip()
                        if offsets["category_col"] < len(row)
                        else ""
                    )
                    record["country_incorporated"] = "GB"
                    addr_parts = []
                    for j in range(3):
                        idx = offsets["addr_start"] + j
                        if idx < len(row) and row[idx].strip():
                            addr_parts.append(row[idx].strip())
                    record["proprietor_address"] = ", ".join(addr_parts)
                    records.append(record)

        df = pd.DataFrame(records)
        logger.info("Parsed %d proprietor-property rows from UKCOD", len(df))
        return df


# ---------------------------------------------------------------------------
# UK Land Registry Loader
# ---------------------------------------------------------------------------


class UKLandRegistryLoader:
    """Batch-load UK Land Registry data into Memgraph, creating Property
    nodes and linking them to Company nodes by registration number or
    name match."""

    MERGE_PROPERTY_QUERY = """
    UNWIND $records AS rec
    MERGE (p:Property {title_number: rec.title_number})
    SET p.tenure = rec.tenure,
        p.address = rec.property_address,
        p.district = rec.district,
        p.county = rec.county,
        p.region = rec.region,
        p.postcode = rec.postcode,
        p.price_paid = rec.price_paid,
        p.country = rec.country,
        p.source = rec.source,
        p.loaded_at = rec.loaded_at
    """

    LINK_BY_REG_NUMBER_QUERY = """
    UNWIND $records AS rec
    MATCH (p:Property {title_number: rec.title_number})
    MATCH (c:Company)
    WHERE c.registration_number = rec.company_reg_number
      AND rec.company_reg_number <> ''
    MERGE (c)-[r:OWNS_PROPERTY]->(p)
    SET r.proprietor_number = rec.proprietor_number,
        r.proprietorship_category = rec.proprietorship_category,
        r.linked_at = rec.loaded_at
    RETURN count(r) AS linked_by_reg
    """

    LINK_BY_NAME_QUERY = """
    UNWIND $records AS rec
    MATCH (p:Property {title_number: rec.title_number})
    WHERE NOT EXISTS((p)<-[:OWNS_PROPERTY]-(:Company))
    MATCH (c:Company)
    WHERE c.name = rec.proprietor_name
       OR c.name_norm = rec.proprietor_name_norm
    WITH p, c, rec
    LIMIT 1
    MERGE (c)-[r:OWNS_PROPERTY]->(p)
    SET r.proprietor_number = rec.proprietor_number,
        r.proprietorship_category = rec.proprietorship_category,
        r.match_method = 'name',
        r.linked_at = rec.loaded_at
    RETURN count(r) AS linked_by_name
    """

    def __init__(self, memgraph_uri: str | None = None):
        self._mg_uri = memgraph_uri or MEMGRAPH_URI
        auth = (MEMGRAPH_USER, MEMGRAPH_PASS) if MEMGRAPH_USER else None
        self._driver = GraphDatabase.driver(self._mg_uri, auth=auth)

    def close(self) -> None:
        self._driver.close()

    def load(
        self, df: pd.DataFrame, batch_size: int = BATCH_SIZE
    ) -> dict[str, int]:
        """Load a parsed UK Land Registry DataFrame into Memgraph.

        Returns a dict with counts of properties created and links
        established.
        """
        if df.empty:
            return {"properties": 0, "linked_by_reg": 0, "linked_by_name": 0}

        loaded_at = datetime.now(timezone.utc).isoformat()
        df = df.copy()
        df["loaded_at"] = loaded_at
        df["proprietor_name_norm"] = df["proprietor_name"].apply(
            _normalize_owner_name
        )

        # Fill NaNs for Cypher compatibility
        df = df.fillna("")

        records = df.to_dict(orient="records")
        total_linked_reg = 0
        total_linked_name = 0

        with self._driver.session() as session:
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]

                # Create / update Property nodes
                session.run(self.MERGE_PROPERTY_QUERY, records=batch)

                # Link by registration number
                result = session.run(
                    self.LINK_BY_REG_NUMBER_QUERY, records=batch
                ).single()
                total_linked_reg += result["linked_by_reg"] if result else 0

                # Link by name (fallback for unlinked properties)
                result = session.run(
                    self.LINK_BY_NAME_QUERY, records=batch
                ).single()
                total_linked_name += result["linked_by_name"] if result else 0

        stats = {
            "properties": len(records),
            "linked_by_reg": total_linked_reg,
            "linked_by_name": total_linked_name,
        }
        logger.info("UK Land Registry load stats: %s", stats)
        return stats


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


async def run_us_collection(
    counties: dict[str, CountyConfig] | None = None,
) -> dict[str, int]:
    """Run the full US property collection pipeline."""
    collector = CountyAssessorCollector(counties=counties)
    try:
        records = await collector.collect_all()
        matched = collector.match_owners_to_graph(records)
        return {"collected": len(records), "matched": matched}
    finally:
        collector.close()


def run_uk_collection(
    ocod_path: str | None = None,
    ukcod_path: str | None = None,
) -> dict[str, Any]:
    """Run the full UK Land Registry collection pipeline."""
    parser = UKLandRegistryParser()
    loader = UKLandRegistryLoader()

    stats: dict[str, Any] = {}
    try:
        # OCOD -- overseas companies
        ocod_df = parser.parse_ocod(csv_path=ocod_path)
        if not ocod_df.empty:
            stats["ocod"] = loader.load(ocod_df)

        # UKCOD -- UK companies
        ukcod_df = parser.parse_ukcod(csv_path=ukcod_path)
        if not ukcod_df.empty:
            stats["ukcod"] = loader.load(ukcod_df)
    finally:
        loader.close()

    logger.info("UK collection complete: %s", stats)
    return stats
