"""Analytics API endpoints — network analysis, risk scoring, community detection."""

from fastapi import APIRouter, Query, Request

router = APIRouter()


@router.get("/analytics/sanctioned-network")
async def get_sanctioned_network(
    request: Request,
    hops: int = Query(default=2, ge=1, le=3),
):
    """Get all sanctioned entities and their N-hop connections."""
    mg = request.app.state.memgraph
    results = list(
        mg.execute_and_fetch(
            """
            MATCH (n)
            WHERE n.sanctions_status = 'SANCTIONED'
            RETURN labels(n)[0] AS type, n.entity_id AS id, n.name AS name
            LIMIT 500
            """
        )
    )

    return {"sanctioned_entities": [dict(r) for r in results], "count": len(results)}


@router.get("/analytics/risk-summary")
async def get_risk_summary(request: Request):
    """Get summary of high-risk vessels."""
    mg = request.app.state.memgraph
    results = list(
        mg.execute_and_fetch(
            """
            MATCH (v:Vessel)
            WHERE v.risk_score >= 5.0 AND v.valid_to IS NULL
            RETURN v.imo AS imo, v.name AS name, v.flag_state AS flag,
                   v.risk_score AS risk_score, v.ais_status AS ais_status,
                   v.sanctions_status AS sanctions_status
            ORDER BY v.risk_score DESC
            LIMIT 100
            """
        )
    )

    return {"high_risk_vessels": [dict(r) for r in results], "count": len(results)}


@router.get("/analytics/interdiction-heatmap")
async def get_interdiction_heatmap(
    request: Request,
    months: int = Query(default=12, ge=1, le=60),
    clusters: int = Query(default=10, ge=2, le=50),
):
    """Get clustered interdiction events for heatmap visualization."""
    pool = request.app.state.postgres_pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT ST_ClusterKMeans(location, $1) OVER () AS cluster_id,
                   event_id, event_time, ST_AsGeoJSON(location) AS location_json
            FROM interdiction_locations
            WHERE event_time >= NOW() - make_interval(months => $2)
            """,
            clusters,
            months,
        )

    # Group by cluster
    cluster_map: dict = {}
    for r in rows:
        cid = r["cluster_id"]
        if cid not in cluster_map:
            cluster_map[cid] = {"cluster_id": cid, "events": []}
        cluster_map[cid]["events"].append(
            {"event_id": r["event_id"], "time": str(r["event_time"]), "location": r["location_json"]}
        )

    return {"clusters": list(cluster_map.values()), "total_events": len(rows)}


@router.get("/analytics/ais-gaps-summary")
async def get_ais_gaps_summary(
    request: Request,
    days: int = Query(default=30, ge=1, le=365),
):
    """Get summary of AIS gap events in the last N days."""
    mg = request.app.state.memgraph
    results = list(
        mg.execute_and_fetch(
            """
            MATCH (v:Vessel)-[:HAS_AIS_GAP]->(g:AIS_Gap_Event)
            WHERE g.gap_start_time >= localDateTime() - duration({days: $days})
            RETURN g.probable_cause AS cause,
                   count(g) AS count,
                   avg(g.gap_duration_hours) AS avg_duration_hours,
                   sum(CASE WHEN g.risk_flag THEN 1 ELSE 0 END) AS flagged_count
            ORDER BY count DESC
            """,
            {"days": days},
        )
    )

    return {"gap_summary": [dict(r) for r in results], "period_days": days}
