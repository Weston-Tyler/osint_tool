"""Transnational Criminal Organization (TCO) corporate detection pipeline.

Runs ten rules against a corporate entity in Memgraph to produce a
composite risk score indicating potential TCO affiliation.  High-scoring
entities trigger Kafka alerts consumed by the analyst workbench and the
STIX export pipeline.

Each rule returns a (score, flags, evidence) tuple.  The pipeline
aggregates these into a TCOAlert and optionally persists the risk score
back to the graph.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from neo4j import GraphDatabase

logger = logging.getLogger("mda.corporate.tco_detector")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MEMGRAPH_URI = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
MEMGRAPH_USER = os.getenv("MEMGRAPH_USER", "")
MEMGRAPH_PASS = os.getenv("MEMGRAPH_PASS", "")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TCO_ALERT_TOPIC = os.getenv("TCO_ALERT_TOPIC", "mda.corporate.tco_alerts")
TCO_ALERT_THRESHOLD = int(os.getenv("TCO_ALERT_THRESHOLD", "40"))

# ---------------------------------------------------------------------------
# Reference sets
# ---------------------------------------------------------------------------

OPACITY_JURISDICTIONS: set[str] = {
    "VG", "KY", "PA", "SC", "BZ", "MH", "WS", "BS", "LI", "MU",
}

LOGISTICS_SECTORS: set[str] = {
    "freight", "logistics", "shipping", "warehousing", "customs broker",
    "forwarding", "transshipment", "cargo", "haulage", "courier",
    "cold chain", "bonded warehouse", "free trade zone",
}

TCO_SANCTION_PROGRAMS: set[str] = {
    "SDNT",      # Specially Designated Narcotics Traffickers
    "SDNTK",     # Narcotics Trafficking Kingpin
    "TCO",       # Transnational Criminal Organizations
    "CJNG",      # Cartel de Jalisco Nueva Generacion
    "SINALOA",   # Sinaloa Cartel list
    "OFAC_SDN",  # OFAC Specially Designated Nationals
    "EU_SANCTIONS",
    "UK_SANCTIONS",
}

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

RuleResult = tuple[float, list[str], list[str]]


@dataclass
class TCOAlert:
    """Alert generated when a corporate entity exceeds the TCO risk
    threshold."""

    entity_id: str
    entity_name: str
    total_score: float
    max_possible: float
    flags: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    rule_scores: dict[str, float] = field(default_factory=dict)
    triggered_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    jurisdiction: str = ""
    alert_level: str = ""

    def __post_init__(self):
        if not self.alert_level:
            if self.total_score >= 100:
                self.alert_level = "CRITICAL"
            elif self.total_score >= 70:
                self.alert_level = "HIGH"
            elif self.total_score >= 40:
                self.alert_level = "MEDIUM"
            else:
                self.alert_level = "LOW"


# ---------------------------------------------------------------------------
# Cypher queries
# ---------------------------------------------------------------------------

_ENTITY_BASE_QUERY = """
MATCH (c:Company {id: $entity_id})
OPTIONAL MATCH (c)-[:REGISTERED_AT]->(a:Address)
OPTIONAL MATCH (c)-[:HAS_OFFICER]->(o:Officer)
OPTIONAL MATCH (c)-[:HAS_SIC]->(s:SICCode)
OPTIONAL MATCH (c)-[:CONTROLLED_BY]->(ubo:Person)
RETURN c,
       a.full_address AS address,
       collect(DISTINCT o {.name, .role, .is_nominee, .id}) AS officers,
       collect(DISTINCT s.code) AS sic_codes,
       collect(DISTINCT ubo {.name, .id}) AS ubos
"""

_SANCTIONS_PROXIMITY_QUERY = """
MATCH (c:Company {id: $entity_id})
MATCH path = (c)-[:OWNS|CONTROLLED_BY|HAS_OFFICER|REGISTERED_AT*1..4]-(sanctioned)
WHERE sanctioned.sanctioned = true
   OR ANY(label IN labels(sanctioned) WHERE label = 'Sanctioned')
RETURN DISTINCT sanctioned.name AS sanctioned_name,
       sanctioned.sanction_program AS program,
       length(path) AS hops,
       [n IN nodes(path) | n.name] AS chain
ORDER BY hops ASC
LIMIT 10
"""

_VESSEL_AIS_QUERY = """
MATCH (c:Company {id: $entity_id})-[:OWNS|OPERATES]->(v:Vessel)
WHERE v.ais_gap_hours IS NOT NULL AND v.ais_gap_hours > 24
RETURN v.imo AS imo,
       v.name AS vessel_name,
       v.ais_gap_hours AS gap_hours,
       v.flag_state AS flag
"""

_SHARED_AGENT_CARTEL_QUERY = """
MATCH (c:Company {id: $entity_id})-[:FORMED_BY|HAS_AGENT]->(agent)
MATCH (cartel_entity)-[:FORMED_BY|HAS_AGENT]->(agent)
WHERE cartel_entity.tco_linked = true AND cartel_entity.id <> $entity_id
RETURN DISTINCT agent.name AS agent_name,
       cartel_entity.name AS cartel_entity_name,
       cartel_entity.sanction_program AS program
LIMIT 20
"""

_POST_SANCTION_QUERY = """
MATCH (c:Company {id: $entity_id})
WHERE c.incorporation_date IS NOT NULL
MATCH (c)-[:OWNS|CONTROLLED_BY|HAS_OFFICER*1..3]-(related)
WHERE related.sanctioned = true
  AND related.sanction_date IS NOT NULL
  AND c.incorporation_date > related.sanction_date
RETURN related.name AS sanctioned_name,
       related.sanction_date AS sanction_date,
       c.incorporation_date AS inc_date
LIMIT 5
"""

_SAFE_HOUSE_QUERY = """
MATCH (c:Company {id: $entity_id})-[:REGISTERED_AT]->(a:Address)
MATCH (sh:SafeHouse)-[:LOCATED_AT]->(a2:Address)
WHERE a.full_address = a2.full_address
   OR (a.lat IS NOT NULL AND a2.lat IS NOT NULL
       AND abs(a.lat - a2.lat) < 0.001
       AND abs(a.lon - a2.lon) < 0.001)
RETURN sh.name AS safe_house_name,
       a.full_address AS address,
       sh.source AS source
LIMIT 5
"""

_DIRECTOR_EVENTS_QUERY = """
MATCH (c:Company {id: $entity_id})-[:HAS_OFFICER]->(o:Officer)
MATCH (o)-[:MENTIONED_IN]->(e:Event)
WHERE e.source IN ['ACLED', 'GDELT']
  AND e.event_type IN ['violence', 'arrest', 'protest', 'seizure',
                        'drug_trafficking', 'money_laundering']
RETURN o.name AS officer_name,
       e.event_type AS event_type,
       e.date AS event_date,
       e.source AS source,
       e.description AS description
LIMIT 10
"""

_MASS_INCORPORATION_QUERY = """
MATCH (c:Company {id: $entity_id})-[:REGISTERED_AT]->(a:Address)
MATCH (other:Company)-[:REGISTERED_AT]->(a)
WHERE other.id <> $entity_id
WITH a, count(other) AS cnt,
     collect(other.incorporation_date) AS dates
WHERE cnt > 20
RETURN a.full_address AS address,
       cnt AS company_count,
       dates
LIMIT 3
"""

_NO_BUSINESS_QUERY = """
MATCH (c:Company {id: $entity_id})
WHERE (c.employee_count IS NULL OR c.employee_count = 0)
  AND NOT EXISTS((c)-[:HAS_SIC]->())
  AND (c.revenue IS NULL OR c.revenue = 0)
  AND (c.accounts_overdue = true OR c.last_accounts_date IS NULL)
RETURN c.name AS name, c.incorporation_date AS inc_date
"""

_MULTI_JURISDICTION_NOMINEES_QUERY = """
MATCH (c:Company {id: $entity_id})-[:HAS_OFFICER]->(o:Officer)
WHERE o.is_nominee = true
MATCH (o)<-[:HAS_OFFICER]-(other:Company)
WHERE other.id <> $entity_id
WITH o, collect(DISTINCT other.jurisdiction) AS jurisdictions,
     count(DISTINCT other) AS company_count
WHERE size(jurisdictions) >= 3
RETURN o.name AS nominee_name,
       jurisdictions,
       company_count
LIMIT 10
"""


# ---------------------------------------------------------------------------
# TCO Corporate Detector
# ---------------------------------------------------------------------------


class TCOCorporateDetector:
    """Pipeline that evaluates a corporate entity against ten TCO-risk
    rules and emits alerts for scores exceeding the threshold."""

    MAX_POSSIBLE_SCORE: float = 225.0  # sum of all rule maximums

    def __init__(
        self,
        memgraph_uri: str | None = None,
        kafka_bootstrap: str | None = None,
        alert_threshold: int | None = None,
    ):
        self._mg_uri = memgraph_uri or MEMGRAPH_URI
        auth = (MEMGRAPH_USER, MEMGRAPH_PASS) if MEMGRAPH_USER else None
        self._driver = GraphDatabase.driver(self._mg_uri, auth=auth)
        self._kafka_bootstrap = kafka_bootstrap or KAFKA_BOOTSTRAP
        self._alert_threshold = alert_threshold or TCO_ALERT_THRESHOLD
        self._producer = None

    # -- lifecycle -----------------------------------------------------------

    def close(self) -> None:
        self._driver.close()
        if self._producer:
            self._producer.close()

    def _get_producer(self):
        if self._producer is None:
            from kafka import KafkaProducer

            self._producer = KafkaProducer(
                bootstrap_servers=self._kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v, default=str).encode(),
                acks="all",
                retries=3,
            )
        return self._producer

    # -- public interface ----------------------------------------------------

    def run_all_rules(self, entity_id: str) -> TCOAlert:
        """Execute all ten TCO detection rules and return the composite
        alert."""
        with self._driver.session() as session:
            base = session.run(_ENTITY_BASE_QUERY, entity_id=entity_id).single()

        if not base:
            logger.warning("Entity %s not found in graph", entity_id)
            return TCOAlert(
                entity_id=entity_id,
                entity_name="UNKNOWN",
                total_score=0,
                max_possible=self.MAX_POSSIBLE_SCORE,
            )

        company = base["c"]
        entity_name = company.get("name", "")
        jurisdiction = company.get("jurisdiction", "")

        all_flags: list[str] = []
        all_evidence: list[str] = []
        rule_scores: dict[str, float] = {}
        total = 0.0

        rules = [
            ("opacity_sanctions_proximity", self._rule_opacity_sanctions_proximity),
            ("vessel_ais_gaps", self._rule_vessel_ais_gaps),
            ("shared_agent_cartel", self._rule_shared_agent_cartel),
            ("post_sanction_incorporation", self._rule_post_sanction_incorporation),
            ("safe_house_address", self._rule_safe_house_address),
            ("director_events", self._rule_director_events),
            ("logistics_sector", self._rule_logistics_sector),
            ("mass_incorporation", self._rule_mass_incorporation),
            ("no_business_indicators", self._rule_no_business_indicators),
            ("multi_jurisdiction_nominees", self._rule_multi_jurisdiction_nominees),
        ]

        for rule_name, rule_fn in rules:
            try:
                score, flags, evidence = rule_fn(entity_id, company, base)
                rule_scores[rule_name] = score
                total += score
                all_flags.extend(flags)
                all_evidence.extend(evidence)
            except Exception:
                logger.exception("Rule %s failed for %s", rule_name, entity_id)
                rule_scores[rule_name] = 0.0

        alert = TCOAlert(
            entity_id=entity_id,
            entity_name=entity_name,
            total_score=total,
            max_possible=self.MAX_POSSIBLE_SCORE,
            flags=all_flags,
            evidence=all_evidence,
            rule_scores=rule_scores,
            jurisdiction=jurisdiction,
        )

        if total >= self._alert_threshold:
            self._emit_alert(alert)
            self._update_risk_score(entity_id, total)

        return alert

    # -- rule implementations ------------------------------------------------

    def _rule_opacity_sanctions_proximity(
        self, entity_id: str, company: Any, base: Any
    ) -> RuleResult:
        """Rule 1: Opacity jurisdiction + sanctions proximity (max 30 pts).

        Awards up to 10 pts for jurisdiction opacity and up to 20 pts for
        proximity to sanctioned entities.
        """
        score = 0.0
        flags: list[str] = []
        evidence: list[str] = []

        jurisdiction = (company.get("jurisdiction") or "").upper()
        if jurisdiction in OPACITY_JURISDICTIONS:
            score += 10.0
            flags.append("OPACITY_JURISDICTION")
            evidence.append(f"Incorporated in opacity jurisdiction: {jurisdiction}")

        with self._driver.session() as session:
            rows = session.run(
                _SANCTIONS_PROXIMITY_QUERY, entity_id=entity_id
            ).data()

        if rows:
            closest_hop = min(r["hops"] for r in rows)
            proximity_score = max(0, 20.0 - (closest_hop - 1) * 5.0)
            score += proximity_score
            flags.append("SANCTIONS_PROXIMITY")
            for r in rows[:3]:
                evidence.append(
                    f"Sanctioned entity '{r['sanctioned_name']}' "
                    f"({r.get('program', 'N/A')}) at {r['hops']} hop(s) "
                    f"via {' -> '.join(str(n) for n in r['chain'])}"
                )

        return (min(score, 30.0), flags, evidence)

    def _rule_vessel_ais_gaps(
        self, entity_id: str, company: Any, base: Any
    ) -> RuleResult:
        """Rule 2: Vessel with AIS gaps >24h (max 25 pts)."""
        score = 0.0
        flags: list[str] = []
        evidence: list[str] = []

        with self._driver.session() as session:
            rows = session.run(_VESSEL_AIS_QUERY, entity_id=entity_id).data()

        if rows:
            flags.append("VESSEL_AIS_GAP")
            for r in rows:
                gap = r["gap_hours"]
                # Graduated scoring: 24-48h = 10pts, 48-96h = 18pts, >96h = 25pts
                if gap > 96:
                    vessel_score = 25.0
                elif gap > 48:
                    vessel_score = 18.0
                else:
                    vessel_score = 10.0
                score = max(score, vessel_score)
                evidence.append(
                    f"Vessel {r['vessel_name']} (IMO {r['imo']}, flag {r['flag']}) "
                    f"AIS gap of {gap:.0f} hours"
                )

        return (min(score, 25.0), flags, evidence)

    def _rule_shared_agent_cartel(
        self, entity_id: str, company: Any, base: Any
    ) -> RuleResult:
        """Rule 3: Shared formation agent with known cartel entity (max 20 pts)."""
        score = 0.0
        flags: list[str] = []
        evidence: list[str] = []

        with self._driver.session() as session:
            rows = session.run(
                _SHARED_AGENT_CARTEL_QUERY, entity_id=entity_id
            ).data()

        if rows:
            score = min(20.0, len(rows) * 5.0)
            flags.append("SHARED_AGENT_CARTEL")
            for r in rows[:5]:
                evidence.append(
                    f"Shares formation agent '{r['agent_name']}' with "
                    f"cartel-linked entity '{r['cartel_entity_name']}' "
                    f"({r.get('program', 'N/A')})"
                )

        return (min(score, 20.0), flags, evidence)

    def _rule_post_sanction_incorporation(
        self, entity_id: str, company: Any, base: Any
    ) -> RuleResult:
        """Rule 4: Company incorporated after a related entity was sanctioned
        (max 25 pts).  Indicates possible sanctions-evasion vehicle."""
        score = 0.0
        flags: list[str] = []
        evidence: list[str] = []

        with self._driver.session() as session:
            rows = session.run(
                _POST_SANCTION_QUERY, entity_id=entity_id
            ).data()

        if rows:
            score = 25.0
            flags.append("POST_SANCTION_INCORPORATION")
            for r in rows:
                evidence.append(
                    f"Incorporated {r['inc_date']} after related entity "
                    f"'{r['sanctioned_name']}' sanctioned on {r['sanction_date']}"
                )

        return (min(score, 25.0), flags, evidence)

    def _rule_safe_house_address(
        self, entity_id: str, company: Any, base: Any
    ) -> RuleResult:
        """Rule 5: Registered address matches a known safe-house / stash-house
        (max 35 pts)."""
        score = 0.0
        flags: list[str] = []
        evidence: list[str] = []

        with self._driver.session() as session:
            rows = session.run(
                _SAFE_HOUSE_QUERY, entity_id=entity_id
            ).data()

        if rows:
            score = 35.0
            flags.append("SAFE_HOUSE_ADDRESS")
            for r in rows:
                evidence.append(
                    f"Address '{r['address']}' matches safe house "
                    f"'{r['safe_house_name']}' (source: {r.get('source', 'N/A')})"
                )

        return (min(score, 35.0), flags, evidence)

    def _rule_director_events(
        self, entity_id: str, company: Any, base: Any
    ) -> RuleResult:
        """Rule 6: Director appears in ACLED/GDELT conflict or crime events
        (max 20 pts)."""
        score = 0.0
        flags: list[str] = []
        evidence: list[str] = []

        with self._driver.session() as session:
            rows = session.run(
                _DIRECTOR_EVENTS_QUERY, entity_id=entity_id
            ).data()

        if rows:
            score = min(20.0, len(rows) * 4.0)
            flags.append("DIRECTOR_IN_EVENTS")
            for r in rows[:5]:
                evidence.append(
                    f"Director '{r['officer_name']}' mentioned in "
                    f"{r['source']} {r['event_type']} event on {r.get('event_date', 'N/A')}: "
                    f"{(r.get('description') or '')[:120]}"
                )

        return (min(score, 20.0), flags, evidence)

    def _rule_logistics_sector(
        self, entity_id: str, company: Any, base: Any
    ) -> RuleResult:
        """Rule 7: Entity operates in logistics / transshipment sectors
        (max 15 pts)."""
        score = 0.0
        flags: list[str] = []
        evidence: list[str] = []

        sic_codes = base.get("sic_codes") or []
        entity_name = (company.get("name") or "").lower()
        description = (company.get("activity_description") or "").lower()

        # Check SIC codes for logistics-related classifications
        logistics_sics = {
            "4210", "4211", "4212", "4213", "4214", "4215",  # Trucking
            "4220", "4225", "4226",                          # Warehousing
            "4400", "4412", "4424", "4432",                  # Water transport
            "4500", "4512", "4522",                          # Air transport
            "4731", "4783",                                  # Freight/cargo
            "5159",                                          # Farm products
        }
        matching_sics = set(sic_codes) & logistics_sics
        if matching_sics:
            score += 10.0
            flags.append("LOGISTICS_SECTOR_SIC")
            evidence.append(f"Logistics-related SIC codes: {matching_sics}")

        # Check name / description for logistics keywords
        for keyword in LOGISTICS_SECTORS:
            if keyword in entity_name or keyword in description:
                score += 5.0
                flags.append("LOGISTICS_SECTOR_NAME")
                evidence.append(f"Logistics keyword '{keyword}' in name/description")
                break

        return (min(score, 15.0), flags, evidence)

    def _rule_mass_incorporation(
        self, entity_id: str, company: Any, base: Any
    ) -> RuleResult:
        """Rule 8: Registered at an address with a mass-incorporation cluster
        (>20 companies, max 20 pts)."""
        score = 0.0
        flags: list[str] = []
        evidence: list[str] = []

        with self._driver.session() as session:
            rows = session.run(
                _MASS_INCORPORATION_QUERY, entity_id=entity_id
            ).data()

        if rows:
            max_count = max(r["company_count"] for r in rows)
            # 20 companies = 10 pts, 50+ = 20 pts
            score = min(20.0, 10.0 + (max_count - 20) * 0.33)
            score = max(score, 10.0)
            flags.append("MASS_INCORPORATION_CLUSTER")
            for r in rows:
                evidence.append(
                    f"Address '{r['address']}' hosts {r['company_count']} companies"
                )

        return (min(score, 20.0), flags, evidence)

    def _rule_no_business_indicators(
        self, entity_id: str, company: Any, base: Any
    ) -> RuleResult:
        """Rule 9: Entity has zero employees, no SIC, no revenue, overdue
        accounts -- no genuine business indicators (max 10 pts)."""
        score = 0.0
        flags: list[str] = []
        evidence: list[str] = []

        with self._driver.session() as session:
            row = session.run(
                _NO_BUSINESS_QUERY, entity_id=entity_id
            ).single()

        if row:
            score = 10.0
            flags.append("NO_BUSINESS_INDICATORS")
            evidence.append(
                f"Entity '{row['name']}' (inc. {row.get('inc_date', 'N/A')}) "
                f"has no employees, SIC codes, revenue, or current accounts"
            )

        return (min(score, 10.0), flags, evidence)

    def _rule_multi_jurisdiction_nominees(
        self, entity_id: str, company: Any, base: Any
    ) -> RuleResult:
        """Rule 10: Nominee directors serving companies across 3+ jurisdictions
        (max 25 pts)."""
        score = 0.0
        flags: list[str] = []
        evidence: list[str] = []

        with self._driver.session() as session:
            rows = session.run(
                _MULTI_JURISDICTION_NOMINEES_QUERY, entity_id=entity_id
            ).data()

        if rows:
            max_jurisdictions = max(len(r["jurisdictions"]) for r in rows)
            # 3 jurisdictions = 15 pts, each additional = +2.5 up to 25
            score = min(25.0, 15.0 + (max_jurisdictions - 3) * 2.5)
            flags.append("MULTI_JURISDICTION_NOMINEES")
            for r in rows[:5]:
                evidence.append(
                    f"Nominee '{r['nominee_name']}' serves "
                    f"{r['company_count']} companies across "
                    f"{len(r['jurisdictions'])} jurisdictions: "
                    f"{', '.join(r['jurisdictions'][:8])}"
                )

        return (min(score, 25.0), flags, evidence)

    # -- alert emission ------------------------------------------------------

    def _emit_alert(self, alert: TCOAlert) -> None:
        """Publish a TCO alert to Kafka."""
        try:
            producer = self._get_producer()
            payload = asdict(alert)
            producer.send(TCO_ALERT_TOPIC, value=payload)
            producer.flush(timeout=10)
            logger.info(
                "TCO alert emitted for %s [%s] score=%.1f level=%s",
                alert.entity_id,
                alert.entity_name,
                alert.total_score,
                alert.alert_level,
            )
        except Exception:
            logger.exception(
                "Failed to emit TCO alert for %s", alert.entity_id
            )

    def _update_risk_score(self, entity_id: str, score: float) -> None:
        """Persist the TCO risk score back to the Company node in Memgraph."""
        query = """
        MATCH (c:Company {id: $entity_id})
        SET c.tco_risk_score = $score,
            c.tco_risk_updated = $updated_at,
            c.tco_alert_level = $level
        """
        level = "CRITICAL" if score >= 100 else "HIGH" if score >= 70 else "MEDIUM"
        try:
            with self._driver.session() as session:
                session.run(
                    query,
                    entity_id=entity_id,
                    score=score,
                    updated_at=datetime.now(timezone.utc).isoformat(),
                    level=level,
                )
        except Exception:
            logger.exception(
                "Failed to update risk score for %s in Memgraph", entity_id
            )


# ---------------------------------------------------------------------------
# Batch runner
# ---------------------------------------------------------------------------


def run_tco_scan(
    entity_ids: list[str],
    memgraph_uri: str | None = None,
    kafka_bootstrap: str | None = None,
    alert_threshold: int | None = None,
) -> list[TCOAlert]:
    """Run the full TCO detection pipeline on a batch of entities."""
    detector = TCOCorporateDetector(
        memgraph_uri=memgraph_uri,
        kafka_bootstrap=kafka_bootstrap,
        alert_threshold=alert_threshold,
    )
    alerts: list[TCOAlert] = []
    try:
        for eid in entity_ids:
            try:
                alert = detector.run_all_rules(eid)
                alerts.append(alert)
            except Exception:
                logger.exception("TCO scan failed for entity %s", eid)
    finally:
        detector.close()

    flagged = [a for a in alerts if a.total_score >= (alert_threshold or TCO_ALERT_THRESHOLD)]
    logger.info(
        "TCO scan complete: %d entities scanned, %d flagged",
        len(entity_ids),
        len(flagged),
    )
    return alerts
