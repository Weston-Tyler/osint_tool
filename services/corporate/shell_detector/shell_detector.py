"""Shell company detection engine for MDA Corporate Ownership Graph.

Computes a composite shell-company score from 12 weighted indicators drawn
from company metadata, graph topology, and jurisdiction opacity.  The score
drives downstream triage in the TCO detection pipeline and feeds the
ownership API.

Indicators are calibrated against the ICIJ Offshore Leaks dataset and
FinCEN Files ground-truth labels.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from neo4j import GraphDatabase

logger = logging.getLogger("mda.corporate.shell_detector")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MEMGRAPH_URI = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
MEMGRAPH_USER = os.getenv("MEMGRAPH_USER", "")
MEMGRAPH_PASS = os.getenv("MEMGRAPH_PASS", "")

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

KNOWN_FORMATION_AGENTS: set[str] = {
    "mossack fonseca",
    "appleby",
    "trident trust",
    "formations house",
    "asiaciti trust",
    "portcullis trustnet",
    "offshore incorporations",
    "oi group",
    "commonwealth trust",
    "harneys",
    "conyers dill & pearman",
    "maples group",
    "walkers",
    "mourant",
    "carey olsen",
    "ocra worldwide",
    "amicorp",
    "fidelity corporate services",
    "sertus",
    "aleman cordero galindo & lee",
}

# ISO-2 codes of jurisdictions with high corporate-opacity risk
OPACITY_JURISDICTIONS: set[str] = {
    "VG",  # British Virgin Islands
    "KY",  # Cayman Islands
    "PA",  # Panama
    "SC",  # Seychelles
    "BZ",  # Belize
    "MH",  # Marshall Islands
    "WS",  # Samoa
    "BS",  # Bahamas
    "LI",  # Liechtenstein
    "MU",  # Mauritius
}

# Granular opacity scores (0 = fully transparent, 10 = fully opaque).
# Based on Tax Justice Network Financial Secrecy Index methodology.
JURISDICTION_OPACITY: dict[str, float] = {
    # Maximum opacity
    "VG": 9.5,
    "KY": 9.0,
    "PA": 8.5,
    "SC": 9.0,
    "BZ": 8.0,
    "MH": 9.0,
    "WS": 8.5,
    "BS": 8.0,
    "LI": 7.5,
    "MU": 7.0,
    # High opacity
    "JE": 6.5,   # Jersey
    "GG": 6.5,   # Guernsey
    "IM": 6.0,   # Isle of Man
    "BM": 6.5,   # Bermuda
    "AI": 7.0,   # Anguilla
    "TC": 7.5,   # Turks & Caicos
    "GI": 5.5,   # Gibraltar
    "CW": 7.0,   # Curacao
    "AN": 7.0,   # Netherlands Antilles (legacy)
    "LB": 7.0,   # Lebanon
    "AE": 6.0,   # UAE
    "HK": 5.0,   # Hong Kong
    "SG": 4.5,   # Singapore
    "LU": 5.5,   # Luxembourg
    "MC": 6.0,   # Monaco
    "AD": 6.0,   # Andorra
    "MT": 5.0,   # Malta
    "CY": 5.5,   # Cyprus
    "SM": 5.5,   # San Marino
    # Moderate opacity
    "NL": 4.0,   # Netherlands (holding structures)
    "IE": 3.5,   # Ireland
    "CH": 5.0,   # Switzerland
    "US": 3.0,   # USA (varies by state)
    "GB": 2.5,   # UK
    "DE": 2.0,   # Germany
    "FR": 2.0,   # France
    "CA": 2.0,   # Canada
    "AU": 2.0,   # Australia
    "NO": 1.0,   # Norway
    "SE": 1.0,   # Sweden
    "DK": 1.0,   # Denmark
    "FI": 1.0,   # Finland
}

# US states with elevated opacity (used for sub-national scoring)
US_OPACITY_STATES: dict[str, float] = {
    "DE": 5.0,   # Delaware
    "WY": 6.0,   # Wyoming
    "NV": 5.5,   # Nevada
    "SD": 5.5,   # South Dakota
    "NM": 5.0,   # New Mexico
}

# Pattern for generic/meaningless company names
_GENERIC_NAME_PATTERN = re.compile(
    r"^(global|international|pacific|atlantic|universal|premier|elite|"
    r"crown|apex|summit|pinnacle|zenith|paramount|sovereign|liberty|"
    r"heritage|legacy|cornerstone|vanguard|sterling|platinum|golden|"
    r"eagle|alpha|omega|horizon|venture|capital|asset|holding|group|"
    r"trust|enterprise|management|consulting|trading|investments?|"
    r"services?|solutions?|partners?|associates?|resources?|"
    r"properties|developments?)\b",
    re.IGNORECASE,
)

_PO_BOX_PATTERN = re.compile(
    r"\b(p\.?\s*o\.?\s*box|post\s*office\s*box|apartado)\b",
    re.IGNORECASE,
)

_SUFFIX_PATTERN = re.compile(
    r"\b(ltd|limited|llc|l\.l\.c|inc|incorporated|corp|corporation|"
    r"gmbh|ag|sa|s\.a|srl|s\.r\.l|bv|b\.v|nv|n\.v|plc|lp|llp|"
    r"se|pty|pvt|co|company)\.?\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ShellScore:
    """Composite shell-company risk score with per-indicator breakdown."""

    entity_id: str
    entity_name: str
    total_score: float = 0.0
    max_possible: float = 13.5
    indicators: dict[str, float] = field(default_factory=dict)
    evidence: list[str] = field(default_factory=list)
    jurisdiction: str = ""
    computed_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def is_shell(self) -> bool:
        """High-confidence shell: score >= 7 out of 13.5."""
        return self.total_score >= 7.0

    @property
    def is_possible_shell(self) -> bool:
        """Medium-confidence shell: score >= 4 out of 13.5."""
        return self.total_score >= 4.0

    @property
    def confidence_pct(self) -> float:
        """Score as a percentage of the theoretical maximum."""
        if self.max_possible == 0:
            return 0.0
        return round((self.total_score / self.max_possible) * 100, 1)


@dataclass
class OpacityScore:
    """Structural opacity score for an ownership chain."""

    entity_id: str
    base_jurisdiction_score: float = 0.0
    layer_penalty: float = 0.0
    nominee_penalty: float = 0.0
    circular_penalty: float = 0.0
    missing_ubo_penalty: float = 0.0
    total: float = 0.0
    details: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Cypher queries
# ---------------------------------------------------------------------------

_ENTITY_METADATA_QUERY = """
MATCH (c:Company {id: $entity_id})
OPTIONAL MATCH (c)-[:REGISTERED_AT]->(a:Address)
OPTIONAL MATCH (c)-[:HAS_OFFICER]->(o:Officer)
OPTIONAL MATCH (c)-[:HAS_SIC]->(s:SICCode)
OPTIONAL MATCH (c)-[:CONTROLLED_BY]->(ubo:Person)
OPTIONAL MATCH (c)<-[:FORMED]-(agent:FormationAgent)
RETURN c,
       a.full_address AS registered_address,
       a.postal_code AS postal_code,
       collect(DISTINCT o {.name, .role, .is_nominee}) AS officers,
       collect(DISTINCT s.code) AS sic_codes,
       ubo IS NOT NULL AS has_ubo,
       agent.name AS formation_agent_name
"""

_SHARED_ADDRESS_COUNT_QUERY = """
MATCH (c:Company {id: $entity_id})-[:REGISTERED_AT]->(a:Address)
MATCH (other:Company)-[:REGISTERED_AT]->(a)
WHERE other.id <> $entity_id
RETURN count(other) AS co_registered_count
"""

_ACCOUNTS_OVERDUE_QUERY = """
MATCH (c:Company {id: $entity_id})
RETURN c.accounts_overdue AS overdue,
       c.last_accounts_date AS last_accounts
"""

_OPACITY_FACTORS_QUERY = """
MATCH path = (target:Company {id: $entity_id})<-[:OWNS*1..10]-(parent)
WITH target, path,
     [n IN nodes(path) WHERE n:Company | n.jurisdiction] AS jurisdictions,
     length(path) AS depth,
     [n IN nodes(path) WHERE n:Company AND n.has_nominee_director = true] AS nominees
OPTIONAL MATCH (target)<-[:OWNS*1..10]-(ancestor)-[:OWNS*1..10]->(target)
WITH target, path, jurisdictions, depth, nominees,
     count(ancestor) > 0 AS has_circular
OPTIONAL MATCH (target)<-[:CONTROLLED_BY]->(ubo:Person)
RETURN jurisdictions,
       depth,
       size(nominees) AS nominee_count,
       has_circular,
       ubo IS NULL AS missing_ubo
ORDER BY depth DESC
LIMIT 1
"""


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------


def _get_driver():
    """Create a Neo4j/Memgraph bolt driver."""
    auth = (MEMGRAPH_USER, MEMGRAPH_PASS) if MEMGRAPH_USER else None
    return GraphDatabase.driver(MEMGRAPH_URI, auth=auth)


def _is_generic_name(name: str) -> bool:
    """Return True if the company name is suspiciously generic."""
    cleaned = _SUFFIX_PATTERN.sub("", name).strip()
    words = cleaned.split()
    if len(words) <= 2 and _GENERIC_NAME_PATTERN.search(cleaned):
        return True
    # All-uppercase single-word names of <= 4 chars (e.g. "AXON", "ZENO")
    if len(words) == 1 and cleaned.isupper() and len(cleaned) <= 4:
        return True
    return False


def _is_po_box(address: str | None) -> bool:
    """Return True if the address is a PO box."""
    if not address:
        return False
    return bool(_PO_BOX_PATTERN.search(address))


def _has_formation_agent(agent_name: str | None, address: str | None) -> bool:
    """Check whether the registered agent or address mentions a known
    formation agent."""
    if agent_name and agent_name.strip().lower() in KNOWN_FORMATION_AGENTS:
        return True
    if address:
        addr_lower = address.lower()
        for agent in KNOWN_FORMATION_AGENTS:
            if agent in addr_lower:
                return True
    return False


def _recently_incorporated(incorporation_date: str | None, threshold_days: int = 365) -> bool:
    """Return True if the company was incorporated within *threshold_days*."""
    if not incorporation_date:
        return False
    try:
        inc = datetime.fromisoformat(incorporation_date.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - inc
        return delta.days <= threshold_days
    except (ValueError, TypeError):
        return False


def compute_shell_score(
    entity_id: str,
    entity_data: dict[str, Any] | None = None,
    driver: Any | None = None,
) -> ShellScore:
    """Compute the 12-indicator shell company score.

    If *entity_data* is provided it is used directly; otherwise the function
    queries Memgraph for the entity metadata.

    Indicator weights (total max = 13.5):
        1.  Opacity jurisdiction                 2.0
        2.  Formation agent address              2.0
        3.  Missing UBO                          2.0
        4.  No employees                         1.0
        5.  Nominee directors                    1.0
        6.  Shared address (>10 co-registered)   1.0
        7.  Recently incorporated (<1 yr)        1.0
        8.  No SIC / activity code               1.0
        9.  Overdue accounts                     1.0
        10. Generic name                         0.5
        11. Registered agent is director         0.5
        12. PO box registered address            0.5
    """
    own_driver = False
    if driver is None and entity_data is None:
        driver = _get_driver()
        own_driver = True

    try:
        data = entity_data or _fetch_entity_data(driver, entity_id)
    finally:
        if own_driver and driver is not None:
            driver.close()

    score = ShellScore(
        entity_id=entity_id,
        entity_name=data.get("name", ""),
        jurisdiction=data.get("jurisdiction", ""),
    )

    # 1. Opacity jurisdiction (2 pts)
    jurisdiction = data.get("jurisdiction", "")
    if jurisdiction.upper() in OPACITY_JURISDICTIONS:
        score.indicators["opacity_jurisdiction"] = 2.0
        score.evidence.append(
            f"Incorporated in opacity jurisdiction {jurisdiction}"
        )

    # 2. Formation agent address (2 pts)
    if _has_formation_agent(
        data.get("formation_agent_name"),
        data.get("registered_address"),
    ):
        score.indicators["formation_agent"] = 2.0
        agent = data.get("formation_agent_name") or "detected in address"
        score.evidence.append(f"Known formation agent: {agent}")

    # 3. Missing UBO (2 pts)
    if not data.get("has_ubo", False):
        score.indicators["missing_ubo"] = 2.0
        score.evidence.append("No ultimate beneficial owner identified")

    # 4. No employees (1 pt)
    employee_count = data.get("employee_count")
    if employee_count is not None and int(employee_count) == 0:
        score.indicators["no_employees"] = 1.0
        score.evidence.append("Zero employees reported")
    elif employee_count is None:
        # Unknown employee count is also suspicious but less so
        score.indicators["no_employees"] = 0.5
        score.evidence.append("Employee count unknown")

    # 5. Nominee directors (1 pt)
    officers = data.get("officers", [])
    nominee_count = sum(
        1 for o in officers if o.get("is_nominee") or _is_known_nominee(o.get("name", ""))
    )
    if nominee_count > 0:
        score.indicators["nominee_directors"] = 1.0
        score.evidence.append(f"{nominee_count} nominee director(s) detected")

    # 6. Shared address with >10 companies (1 pt)
    co_registered = data.get("co_registered_count", 0)
    if co_registered > 10:
        score.indicators["shared_address"] = 1.0
        score.evidence.append(
            f"Registered address shared with {co_registered} other companies"
        )

    # 7. Recently incorporated (1 pt)
    if _recently_incorporated(data.get("incorporation_date")):
        score.indicators["recently_incorporated"] = 1.0
        score.evidence.append(
            f"Incorporated within last 12 months: {data.get('incorporation_date')}"
        )

    # 8. No SIC code (1 pt)
    sic_codes = data.get("sic_codes", [])
    if not sic_codes:
        score.indicators["no_sic"] = 1.0
        score.evidence.append("No SIC / activity classification code")

    # 9. Overdue accounts (1 pt)
    if data.get("accounts_overdue"):
        score.indicators["overdue_accounts"] = 1.0
        score.evidence.append("Company accounts are overdue")

    # 10. Generic name (0.5 pt)
    name = data.get("name", "")
    if name and _is_generic_name(name):
        score.indicators["generic_name"] = 0.5
        score.evidence.append(f"Generic / meaningless company name: {name}")

    # 11. Registered agent is also a director (0.5 pt)
    agent_name = (data.get("formation_agent_name") or "").lower().strip()
    if agent_name:
        for o in officers:
            if o.get("name", "").lower().strip() == agent_name:
                score.indicators["agent_is_director"] = 0.5
                score.evidence.append(
                    "Registered agent also serves as director"
                )
                break

    # 12. PO box address (0.5 pt)
    if _is_po_box(data.get("registered_address")):
        score.indicators["po_box"] = 0.5
        score.evidence.append("Registered address is a PO box")

    score.total_score = sum(score.indicators.values())
    return score


def _is_known_nominee(name: str) -> bool:
    """Heuristic check for common nominee-service provider names."""
    if not name:
        return False
    lowered = name.lower()
    nominee_keywords = [
        "nominee", "trustee", "fiduciary", "designee",
        "corporate directors", "sertus", "amicorp",
        "mossack", "appleby", "portcullis",
    ]
    return any(kw in lowered for kw in nominee_keywords)


def _fetch_entity_data(driver: Any, entity_id: str) -> dict[str, Any]:
    """Fetch all metadata required for shell scoring from Memgraph."""
    result: dict[str, Any] = {"id": entity_id}

    with driver.session() as session:
        # Main metadata
        row = session.run(_ENTITY_METADATA_QUERY, entity_id=entity_id).single()
        if row:
            c = row["c"]
            result["name"] = c.get("name", "")
            result["jurisdiction"] = c.get("jurisdiction", "")
            result["incorporation_date"] = c.get("incorporation_date")
            result["employee_count"] = c.get("employee_count")
            result["registered_address"] = row["registered_address"]
            result["postal_code"] = row["postal_code"]
            result["officers"] = row["officers"] or []
            result["sic_codes"] = row["sic_codes"] or []
            result["has_ubo"] = row["has_ubo"]
            result["formation_agent_name"] = row["formation_agent_name"]

        # Shared-address count
        sa_row = session.run(
            _SHARED_ADDRESS_COUNT_QUERY, entity_id=entity_id
        ).single()
        if sa_row:
            result["co_registered_count"] = sa_row["co_registered_count"]

        # Accounts overdue
        acc_row = session.run(
            _ACCOUNTS_OVERDUE_QUERY, entity_id=entity_id
        ).single()
        if acc_row:
            result["accounts_overdue"] = acc_row["overdue"]

    return result


# ---------------------------------------------------------------------------
# Opacity scoring
# ---------------------------------------------------------------------------


def compute_opacity_score(
    entity_id: str,
    driver: Any | None = None,
) -> OpacityScore:
    """Compute the structural opacity score for a corporate ownership chain.

    Components:
        - Base jurisdiction opacity (0-10 from JURISDICTION_OPACITY)
        - Layer penalty: +0.5 per ownership layer beyond 2
        - Nominee penalty: +1.0 per nominee entity in chain
        - Circular penalty: +3.0 if circular ownership detected
        - Missing UBO penalty: +2.0 if no UBO at chain end
    """
    own_driver = False
    if driver is None:
        driver = _get_driver()
        own_driver = True

    opacity = OpacityScore(entity_id=entity_id)

    try:
        with driver.session() as session:
            row = session.run(
                _OPACITY_FACTORS_QUERY, entity_id=entity_id
            ).single()

            if not row:
                logger.warning("No ownership chain found for %s", entity_id)
                return opacity

            jurisdictions = row["jurisdictions"] or []
            depth = row["depth"] or 0
            nominee_count = row["nominee_count"] or 0
            has_circular = row["has_circular"]
            missing_ubo = row["missing_ubo"]

            # Base jurisdiction: take the maximum opacity across the chain
            for j in jurisdictions:
                if j:
                    j_score = JURISDICTION_OPACITY.get(j.upper(), 1.0)
                    if j_score > opacity.base_jurisdiction_score:
                        opacity.base_jurisdiction_score = j_score
            opacity.details.append(
                f"Max jurisdiction opacity in chain: {opacity.base_jurisdiction_score}"
            )

            # Layer penalty: +0.5 per layer beyond 2
            if depth > 2:
                opacity.layer_penalty = (depth - 2) * 0.5
                opacity.details.append(
                    f"Ownership depth {depth} => +{opacity.layer_penalty} penalty"
                )

            # Nominee penalty
            if nominee_count > 0:
                opacity.nominee_penalty = nominee_count * 1.0
                opacity.details.append(
                    f"{nominee_count} nominee(s) => +{opacity.nominee_penalty} penalty"
                )

            # Circular ownership
            if has_circular:
                opacity.circular_penalty = 3.0
                opacity.details.append("Circular ownership detected => +3.0")

            # Missing UBO
            if missing_ubo:
                opacity.missing_ubo_penalty = 2.0
                opacity.details.append("No UBO at chain terminus => +2.0")

            opacity.total = (
                opacity.base_jurisdiction_score
                + opacity.layer_penalty
                + opacity.nominee_penalty
                + opacity.circular_penalty
                + opacity.missing_ubo_penalty
            )

    finally:
        if own_driver:
            driver.close()

    return opacity


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------


def batch_score_entities(
    entity_ids: list[str],
    driver: Any | None = None,
) -> list[ShellScore]:
    """Score multiple entities, reusing a single driver session."""
    own_driver = False
    if driver is None:
        driver = _get_driver()
        own_driver = True

    results: list[ShellScore] = []
    try:
        for eid in entity_ids:
            try:
                data = _fetch_entity_data(driver, eid)
                score = compute_shell_score(eid, entity_data=data, driver=driver)
                results.append(score)
            except Exception:
                logger.exception("Failed to score entity %s", eid)
    finally:
        if own_driver:
            driver.close()

    return results


def persist_shell_score(score: ShellScore, driver: Any | None = None) -> None:
    """Write the shell score back to the Company node in Memgraph."""
    own_driver = False
    if driver is None:
        driver = _get_driver()
        own_driver = True

    query = """
    MATCH (c:Company {id: $entity_id})
    SET c.shell_score = $total_score,
        c.is_shell = $is_shell,
        c.is_possible_shell = $is_possible_shell,
        c.shell_indicators = $indicators,
        c.shell_score_updated = $computed_at
    """
    try:
        with driver.session() as session:
            session.run(
                query,
                entity_id=score.entity_id,
                total_score=score.total_score,
                is_shell=score.is_shell,
                is_possible_shell=score.is_possible_shell,
                indicators=str(score.indicators),
                computed_at=score.computed_at,
            )
    finally:
        if own_driver:
            driver.close()
