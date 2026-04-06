"""FastAPI ownership intelligence API for MDA Corporate Ownership Graph.

Provides REST endpoints for UBO traversal, corporate-tree navigation,
shell-company checks, vessel ownership unmasking, sanctions-proximity
analysis, and corporate entity search.

Backed by Memgraph via the Neo4j Python driver.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase
from pydantic import BaseModel, Field

logger = logging.getLogger("mda.corporate.ownership_api")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MEMGRAPH_URI = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
MEMGRAPH_USER = os.getenv("MEMGRAPH_USER", "")
MEMGRAPH_PASS = os.getenv("MEMGRAPH_PASS", "")
API_PREFIX = os.getenv("OWNERSHIP_API_PREFIX", "/api/v1")

# ---------------------------------------------------------------------------
# Lifespan: driver management
# ---------------------------------------------------------------------------

_driver = None


def _get_driver():
    global _driver
    if _driver is None:
        auth = (MEMGRAPH_USER, MEMGRAPH_PASS) if MEMGRAPH_USER else None
        _driver = GraphDatabase.driver(MEMGRAPH_URI, auth=auth)
    return _driver


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage the Memgraph driver lifecycle."""
    _get_driver()
    logger.info("Memgraph driver initialised (%s)", MEMGRAPH_URI)
    yield
    global _driver
    if _driver:
        _driver.close()
        _driver = None
        logger.info("Memgraph driver closed")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="MDA Corporate Ownership API",
    description=(
        "REST API for corporate ownership intelligence: UBO traversal, "
        "shell-company detection, vessel unmasking, and sanctions proximity."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str = "ok"
    memgraph: str = "connected"
    timestamp: str


class UBONode(BaseModel):
    id: str
    name: str
    type: str
    nationality: str | None = None
    ownership_pct: float | None = None
    is_sanctioned: bool = False


class UBOResponse(BaseModel):
    entity_id: str
    entity_name: str
    ubos: list[UBONode]
    chain_depth: int
    computed_at: str


class CorporateTreeNode(BaseModel):
    id: str
    name: str
    jurisdiction: str | None = None
    ownership_pct: float | None = None
    level: int
    is_shell: bool = False
    children: list[CorporateTreeNode] = Field(default_factory=list)


class CorporateTreeResponse(BaseModel):
    entity_id: str
    entity_name: str
    direction: str
    tree: list[CorporateTreeNode]


class ShellCheckResponse(BaseModel):
    entity_id: str
    entity_name: str
    total_score: float
    max_possible: float
    is_shell: bool
    is_possible_shell: bool
    confidence_pct: float
    indicators: dict[str, float]
    evidence: list[str]
    jurisdiction: str


class VesselOwnerLayer(BaseModel):
    entity_id: str
    entity_name: str
    role: str
    jurisdiction: str | None = None
    is_shell: bool = False
    shell_score: float | None = None


class VesselUnmaskResponse(BaseModel):
    imo_number: str
    vessel_name: str
    flag_state: str | None = None
    ownership_layers: list[VesselOwnerLayer]
    ais_gap_hours: float | None = None
    sanctions_flags: list[str]


class SanctionedEntity(BaseModel):
    id: str
    name: str
    sanction_program: str | None = None
    hops: int
    path: list[str]


class SanctionsProximityResponse(BaseModel):
    entity_id: str
    entity_name: str
    max_hops: int
    sanctioned_entities: list[SanctionedEntity]
    closest_hop: int | None = None
    risk_level: str


class SearchResult(BaseModel):
    id: str
    name: str
    jurisdiction: str | None = None
    registration_number: str | None = None
    source: str | None = None
    shell_score: float | None = None


class SearchResponse(BaseModel):
    query_name: str
    query_jurisdiction: str | None = None
    results: list[SearchResult]
    total: int


# ---------------------------------------------------------------------------
# Cypher queries
# ---------------------------------------------------------------------------

_UBO_QUERY = """
MATCH path = (target:Company {id: $entity_id})<-[:OWNS|CONTROLLED_BY*1..10]-(ubo)
WHERE (ubo:Person OR (ubo:Company AND NOT EXISTS((ubo)<-[:OWNS]-(:Company))))
WITH target, ubo, path,
     reduce(pct = 100.0, r IN relationships(path) |
         CASE WHEN r.ownership_pct IS NOT NULL
              THEN pct * r.ownership_pct / 100.0
              ELSE pct END
     ) AS effective_pct,
     length(path) AS depth
RETURN DISTINCT ubo.id AS id,
       ubo.name AS name,
       CASE WHEN ubo:Person THEN 'Person' ELSE 'Company' END AS type,
       ubo.nationality AS nationality,
       effective_pct AS ownership_pct,
       COALESCE(ubo.sanctioned, false) AS is_sanctioned,
       depth
ORDER BY effective_pct DESC
"""

_TREE_UP_QUERY = """
MATCH path = (target:Company {id: $entity_id})<-[:OWNS*1..10]-(parent)
WITH target, parent, path, length(path) AS depth,
     relationships(path)[-1].ownership_pct AS pct
RETURN parent.id AS id,
       parent.name AS name,
       parent.jurisdiction AS jurisdiction,
       pct AS ownership_pct,
       depth AS level,
       COALESCE(parent.is_shell, false) AS is_shell,
       [n IN nodes(path) | n.id] AS chain
ORDER BY depth ASC
"""

_TREE_DOWN_QUERY = """
MATCH path = (target:Company {id: $entity_id})-[:OWNS*1..10]->(child)
WITH target, child, path, length(path) AS depth,
     relationships(path)[-1].ownership_pct AS pct
RETURN child.id AS id,
       child.name AS name,
       child.jurisdiction AS jurisdiction,
       pct AS ownership_pct,
       depth AS level,
       COALESCE(child.is_shell, false) AS is_shell,
       [n IN nodes(path) | n.id] AS chain
ORDER BY depth ASC
"""

_VESSEL_OWNERSHIP_QUERY = """
MATCH (v:Vessel {imo: $imo_number})
OPTIONAL MATCH path = (v)<-[:OWNS|OPERATES|MANAGES*1..8]-(owner)
WITH v, owner, path, length(path) AS depth,
     CASE
       WHEN ANY(r IN relationships(path) WHERE type(r) = 'OWNS') THEN 'owner'
       WHEN ANY(r IN relationships(path) WHERE type(r) = 'OPERATES') THEN 'operator'
       ELSE 'manager'
     END AS role
RETURN v.name AS vessel_name,
       v.flag_state AS flag_state,
       v.ais_gap_hours AS ais_gap_hours,
       owner.id AS owner_id,
       owner.name AS owner_name,
       role,
       owner.jurisdiction AS jurisdiction,
       COALESCE(owner.is_shell, false) AS is_shell,
       owner.shell_score AS shell_score,
       depth
ORDER BY depth ASC
"""

_VESSEL_SANCTIONS_QUERY = """
MATCH (v:Vessel {imo: $imo_number})
OPTIONAL MATCH (v)<-[:OWNS|OPERATES|MANAGES*1..6]-(owner)
WHERE owner.sanctioned = true
RETURN collect(DISTINCT
    owner.name + ' (' + COALESCE(owner.sanction_program, 'N/A') + ')'
) AS sanctions_flags
"""

_SANCTIONS_PROXIMITY_QUERY = """
MATCH (target:Company {id: $entity_id})
MATCH path = (target)-[:OWNS|CONTROLLED_BY|HAS_OFFICER|REGISTERED_AT*1..$max_hops]-(sanctioned)
WHERE sanctioned.sanctioned = true
RETURN DISTINCT sanctioned.id AS id,
       sanctioned.name AS name,
       sanctioned.sanction_program AS sanction_program,
       length(path) AS hops,
       [n IN nodes(path) | n.name] AS path_names
ORDER BY hops ASC
LIMIT 50
"""

_SEARCH_QUERY = """
MATCH (c:Company)
WHERE c.name CONTAINS $name_fragment
  AND ($jurisdiction IS NULL OR c.jurisdiction = $jurisdiction)
RETURN c.id AS id,
       c.name AS name,
       c.jurisdiction AS jurisdiction,
       c.registration_number AS registration_number,
       c.source AS source,
       c.shell_score AS shell_score
ORDER BY c.name ASC
LIMIT 50
"""

_ENTITY_NAME_QUERY = """
MATCH (c:Company {id: $entity_id})
RETURN c.name AS name
"""


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    driver = _get_driver()
    mg_status = "connected"
    try:
        with driver.session() as session:
            session.run("RETURN 1").consume()
    except Exception as exc:
        mg_status = f"error: {exc}"

    return HealthResponse(
        status="ok" if mg_status == "connected" else "degraded",
        memgraph=mg_status,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@app.get(f"{API_PREFIX}/ubo/{{entity_id}}", response_model=UBOResponse)
async def get_ubo(entity_id: str):
    """Traverse the ownership graph to find Ultimate Beneficial Owners."""
    driver = _get_driver()
    with driver.session() as session:
        entity_row = session.run(_ENTITY_NAME_QUERY, entity_id=entity_id).single()
        if not entity_row:
            raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

        rows = session.run(_UBO_QUERY, entity_id=entity_id).data()

    ubos = [
        UBONode(
            id=r["id"],
            name=r["name"],
            type=r["type"],
            nationality=r.get("nationality"),
            ownership_pct=r.get("ownership_pct"),
            is_sanctioned=r.get("is_sanctioned", False),
        )
        for r in rows
    ]

    max_depth = max((r["depth"] for r in rows), default=0)

    return UBOResponse(
        entity_id=entity_id,
        entity_name=entity_row["name"],
        ubos=ubos,
        chain_depth=max_depth,
        computed_at=datetime.now(timezone.utc).isoformat(),
    )


@app.get(
    f"{API_PREFIX}/corporate-tree/{{entity_id}}",
    response_model=CorporateTreeResponse,
)
async def get_corporate_tree(
    entity_id: str,
    direction: str = Query("up", regex="^(up|down)$"),
):
    """Retrieve the corporate ownership tree upward (parents) or
    downward (subsidiaries)."""
    driver = _get_driver()
    with driver.session() as session:
        entity_row = session.run(_ENTITY_NAME_QUERY, entity_id=entity_id).single()
        if not entity_row:
            raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

        query = _TREE_UP_QUERY if direction == "up" else _TREE_DOWN_QUERY
        rows = session.run(query, entity_id=entity_id).data()

    nodes = [
        CorporateTreeNode(
            id=r["id"],
            name=r["name"],
            jurisdiction=r.get("jurisdiction"),
            ownership_pct=r.get("ownership_pct"),
            level=r["level"],
            is_shell=r.get("is_shell", False),
        )
        for r in rows
    ]

    return CorporateTreeResponse(
        entity_id=entity_id,
        entity_name=entity_row["name"],
        direction=direction,
        tree=nodes,
    )


@app.get(
    f"{API_PREFIX}/shell-check/{{entity_id}}",
    response_model=ShellCheckResponse,
)
async def shell_check(entity_id: str):
    """Run the shell-company detection engine on a single entity."""
    from services.corporate.shell_detector.shell_detector import (
        compute_shell_score,
    )

    driver = _get_driver()
    with driver.session() as session:
        entity_row = session.run(_ENTITY_NAME_QUERY, entity_id=entity_id).single()
        if not entity_row:
            raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

    score = compute_shell_score(entity_id, driver=driver)

    return ShellCheckResponse(
        entity_id=score.entity_id,
        entity_name=score.entity_name,
        total_score=score.total_score,
        max_possible=score.max_possible,
        is_shell=score.is_shell,
        is_possible_shell=score.is_possible_shell,
        confidence_pct=score.confidence_pct,
        indicators=score.indicators,
        evidence=score.evidence,
        jurisdiction=score.jurisdiction,
    )


@app.get(
    f"{API_PREFIX}/vessel-unmask/{{imo_number}}",
    response_model=VesselUnmaskResponse,
)
async def vessel_unmask(imo_number: str):
    """Unmask the full ownership chain behind a vessel IMO number."""
    driver = _get_driver()
    with driver.session() as session:
        rows = session.run(
            _VESSEL_OWNERSHIP_QUERY, imo_number=imo_number
        ).data()

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"Vessel IMO {imo_number} not found",
            )

        sanctions_row = session.run(
            _VESSEL_SANCTIONS_QUERY, imo_number=imo_number
        ).single()

    vessel_name = rows[0]["vessel_name"] or ""
    flag_state = rows[0]["flag_state"]
    ais_gap_hours = rows[0]["ais_gap_hours"]

    layers = []
    seen_ids: set[str] = set()
    for r in rows:
        oid = r.get("owner_id")
        if oid and oid not in seen_ids:
            seen_ids.add(oid)
            layers.append(
                VesselOwnerLayer(
                    entity_id=oid,
                    entity_name=r["owner_name"] or "",
                    role=r.get("role", "unknown"),
                    jurisdiction=r.get("jurisdiction"),
                    is_shell=r.get("is_shell", False),
                    shell_score=r.get("shell_score"),
                )
            )

    sanctions_flags = (
        sanctions_row["sanctions_flags"] if sanctions_row else []
    )

    return VesselUnmaskResponse(
        imo_number=imo_number,
        vessel_name=vessel_name,
        flag_state=flag_state,
        ownership_layers=layers,
        ais_gap_hours=ais_gap_hours,
        sanctions_flags=sanctions_flags or [],
    )


@app.get(
    f"{API_PREFIX}/sanctions-proximity/{{entity_id}}",
    response_model=SanctionsProximityResponse,
)
async def sanctions_proximity(
    entity_id: str,
    max_hops: int = Query(4, ge=1, le=10),
):
    """Find sanctioned entities within N hops of a corporate entity."""
    driver = _get_driver()

    # Memgraph does not support parameterised relationship length, so we
    # template it safely (integer-only).
    query = _SANCTIONS_PROXIMITY_QUERY.replace("$max_hops", str(int(max_hops)))

    with driver.session() as session:
        entity_row = session.run(_ENTITY_NAME_QUERY, entity_id=entity_id).single()
        if not entity_row:
            raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

        rows = session.run(query, entity_id=entity_id).data()

    sanctioned = [
        SanctionedEntity(
            id=r["id"],
            name=r["name"],
            sanction_program=r.get("sanction_program"),
            hops=r["hops"],
            path=r.get("path_names", []),
        )
        for r in rows
    ]

    closest = min((s.hops for s in sanctioned), default=None)

    if closest is not None and closest <= 1:
        risk_level = "CRITICAL"
    elif closest is not None and closest <= 2:
        risk_level = "HIGH"
    elif closest is not None and closest <= 3:
        risk_level = "MEDIUM"
    elif sanctioned:
        risk_level = "LOW"
    else:
        risk_level = "NONE"

    return SanctionsProximityResponse(
        entity_id=entity_id,
        entity_name=entity_row["name"],
        max_hops=max_hops,
        sanctioned_entities=sanctioned,
        closest_hop=closest,
        risk_level=risk_level,
    )


@app.get(f"{API_PREFIX}/search", response_model=SearchResponse)
async def search_entities(
    name: str = Query(..., min_length=2, description="Company name fragment"),
    jurisdiction: str | None = Query(None, description="ISO-2 jurisdiction filter"),
):
    """Search for corporate entities by name (and optional jurisdiction)."""
    driver = _get_driver()
    with driver.session() as session:
        rows = session.run(
            _SEARCH_QUERY,
            name_fragment=name,
            jurisdiction=jurisdiction,
        ).data()

    results = [
        SearchResult(
            id=r["id"],
            name=r["name"],
            jurisdiction=r.get("jurisdiction"),
            registration_number=r.get("registration_number"),
            source=r.get("source"),
            shell_score=r.get("shell_score"),
        )
        for r in rows
    ]

    return SearchResponse(
        query_name=name,
        query_jurisdiction=jurisdiction,
        results=results,
        total=len(results),
    )
