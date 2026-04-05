"""Vessel API endpoints — search, detail, network traversal."""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()


@router.get("/vessels/{identifier}")
async def get_vessel(identifier: str, request: Request):
    """Get vessel by IMO or MMSI with ownership chain, sanctions, and recent events."""
    mg = request.app.state.memgraph
    results = list(
        mg.execute_and_fetch(
            """
            MATCH (v:Vessel)
            WHERE v.imo = $id OR v.mmsi = $id
            OPTIONAL MATCH (v)-[:OWNED_BY]->(owner)
            OPTIONAL MATCH (v)-[:OPERATED_BY]->(operator)
            OPTIONAL MATCH (v)-[:HAS_AIS_GAP]->(gap:AIS_Gap_Event)
                WHERE gap.gap_start_time >= localDateTime() - duration({days: 90})
            OPTIONAL MATCH (v)-[:SANCTIONED_BY]->(sanc_auth)
            RETURN v,
                   collect(DISTINCT owner) AS owners,
                   collect(DISTINCT operator) AS operators,
                   collect(DISTINCT gap) AS recent_gaps,
                   collect(DISTINCT sanc_auth) AS sanctions
            LIMIT 1
            """,
            {"id": identifier},
        )
    )

    if not results:
        raise HTTPException(status_code=404, detail=f"Vessel {identifier} not found")

    r = results[0]
    return {
        "vessel": dict(r["v"]) if hasattr(r["v"], "__iter__") else r["v"],
        "owners": [dict(o) if hasattr(o, "__iter__") else o for o in r.get("owners", [])],
        "operators": [dict(o) if hasattr(o, "__iter__") else o for o in r.get("operators", [])],
        "recent_gaps": [dict(g) if hasattr(g, "__iter__") else g for g in r.get("recent_gaps", [])],
        "sanctions": [dict(s) if hasattr(s, "__iter__") else s for s in r.get("sanctions", [])],
    }


@router.get("/vessels/{identifier}/network")
async def get_vessel_network(
    identifier: str,
    request: Request,
    hops: int = Query(default=2, ge=1, le=4),
    min_confidence: float = Query(default=0.5, ge=0.0, le=1.0),
):
    """Get N-hop network around a vessel."""
    mg = request.app.state.memgraph
    results = list(
        mg.execute_and_fetch(
            """
            MATCH (v:Vessel)
            WHERE v.imo = $imo OR v.mmsi = $imo
            CALL {
                WITH v
                MATCH path = (v)-[*1..2]-(related)
                RETURN nodes(path) AS path_nodes, relationships(path) AS path_rels
                LIMIT 200
            }
            RETURN path_nodes, path_rels
            """,
            {"imo": identifier},
        )
    )

    return {"vessel_id": identifier, "hops": hops, "paths_count": len(results)}


@router.get("/vessels")
async def search_vessels(
    request: Request,
    name: Optional[str] = None,
    flag_state: Optional[str] = None,
    min_risk: Optional[float] = None,
    sanctioned_only: bool = False,
    ais_dark: bool = False,
    limit: int = Query(default=50, le=500),
):
    """Search vessels with filters."""
    mg = request.app.state.memgraph

    conditions = ["v.valid_to IS NULL"]
    params: dict = {"limit": limit}

    if name:
        conditions.append("toLower(v.name) CONTAINS toLower($name)")
        params["name"] = name
    if flag_state:
        conditions.append("v.flag_state = $flag")
        params["flag"] = flag_state.upper()
    if min_risk is not None:
        conditions.append("v.risk_score >= $min_risk")
        params["min_risk"] = min_risk
    if sanctioned_only:
        conditions.append("v.sanctions_status = 'SANCTIONED'")
    if ais_dark:
        conditions.append("v.ais_status = 'DARK'")

    where_clause = " AND ".join(conditions)
    query = f"""
        MATCH (v:Vessel)
        WHERE {where_clause}
        RETURN v
        ORDER BY v.risk_score DESC
        LIMIT $limit
    """

    results = list(mg.execute_and_fetch(query, params))
    vessels = [dict(r["v"]) if hasattr(r["v"], "__iter__") else r["v"] for r in results]
    return {"vessels": vessels, "count": len(vessels)}


@router.get("/vessels/{identifier}/track")
async def get_vessel_track(
    identifier: str,
    request: Request,
    days: int = Query(default=30, ge=1, le=365),
):
    """Get vessel track from PostGIS for the last N days."""
    pool = request.app.state.postgres_pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT mmsi, valid_time, ST_AsGeoJSON(position) AS position_json,
                   speed_kts, heading, nav_status
            FROM ais_positions
            WHERE (mmsi = $1 OR imo = $1)
              AND valid_time >= NOW() - make_interval(days => $2)
            ORDER BY valid_time
            LIMIT 10000
            """,
            identifier,
            days,
        )

    return {
        "vessel_id": identifier,
        "days": days,
        "point_count": len(rows),
        "track": [
            {
                "time": str(r["valid_time"]),
                "position": r["position_json"],
                "speed_kts": r["speed_kts"],
                "heading": r["heading"],
            }
            for r in rows
        ],
    }
