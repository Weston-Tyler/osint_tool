"""UAS detection and tracking API endpoints."""

from typing import Optional

from fastapi import APIRouter, Query, Request

router = APIRouter()


@router.get("/uas/detections")
async def get_uas_detections(
    request: Request,
    since_hours: int = Query(default=24, le=720),
    min_confidence: float = Query(default=0.5),
    location_context: Optional[str] = None,
    limit: int = Query(default=100, le=1000),
):
    """Get recent UAS detection events."""
    mg = request.app.state.memgraph

    conditions = [
        f"e.detection_timestamp >= localDateTime() - duration({{hours: {int(since_hours)}}})",
        "e.detection_confidence >= $min_conf",
    ]
    params: dict = {"min_conf": min_confidence, "limit": limit}

    if location_context:
        conditions.append("e.location_context = $ctx")
        params["ctx"] = location_context

    where_clause = " AND ".join(conditions)
    results = list(
        mg.execute_and_fetch(
            f"""
            MATCH (e:UAS_Detection_Event)
            WHERE {where_clause}
            RETURN e
            ORDER BY e.detection_timestamp DESC
            LIMIT $limit
            """,
            params,
        )
    )

    detections = [dict(r["e"]) if hasattr(r["e"], "__iter__") else r["e"] for r in results]
    return {"detections": detections, "count": len(detections)}


@router.get("/uas/flight-paths")
async def get_flight_paths(
    request: Request,
    since_hours: int = Query(default=24, le=720),
    crossed_border: Optional[bool] = None,
    limit: int = Query(default=50, le=500),
):
    """Get recent UAS flight paths from PostGIS."""
    pool = request.app.state.postgres_pool
    async with pool.acquire() as conn:
        conditions = ["start_time >= NOW() - make_interval(hours => $1)"]
        args = [since_hours]

        if crossed_border is not None:
            conditions.append(f"crossed_border = ${len(args) + 1}")
            args.append(crossed_border)

        where_clause = " AND ".join(conditions)
        rows = await conn.fetch(
            f"""
            SELECT path_id, drone_id, start_time, end_time,
                   ST_AsGeoJSON(geometry) AS geometry_json,
                   max_alt_m, crossed_border, country_a, country_b
            FROM uas_flight_paths
            WHERE {where_clause}
            ORDER BY start_time DESC
            LIMIT {limit}
            """,
            *args,
        )

    return {
        "flight_paths": [
            {
                "path_id": r["path_id"],
                "drone_id": r["drone_id"],
                "start_time": str(r["start_time"]),
                "end_time": str(r["end_time"]),
                "geometry": r["geometry_json"],
                "max_alt_m": r["max_alt_m"],
                "crossed_border": r["crossed_border"],
            }
            for r in rows
        ],
        "count": len(rows),
    }


@router.get("/uas/sensors")
async def get_sensor_nodes(request: Request):
    """Get all registered sensor nodes."""
    mg = request.app.state.memgraph
    results = list(
        mg.execute_and_fetch(
            """
            MATCH (s:Sensor_Node)
            RETURN s
            ORDER BY s.last_heartbeat DESC
            """
        )
    )

    sensors = [dict(r["s"]) if hasattr(r["s"], "__iter__") else r["s"] for r in results]
    return {"sensors": sensors, "count": len(sensors)}
