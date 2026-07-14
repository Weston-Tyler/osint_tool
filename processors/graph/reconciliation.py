"""Reconcile realized events against open WorldFish predictions (pure logic).

When a realized :Event arrives, it is matched to open :PredictedEvent nodes by
type, location, and timeframe; a match links (:PredictedEvent)-[:REALIZED_BY]->(:Event)
and scores the hit (distance, lead time, confidence-at-prediction). No Memgraph /
Kafka imports here, so the matching is unit-testable in isolation.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

DEFAULT_MAX_DISTANCE_KM = 250.0
# Grace added to a prediction's max window before a late realization stops counting.
WINDOW_GRACE_DAYS = 7

# Realized event types that satisfy a predicted type: predicted -> {realized, ...}.
TYPE_ALIASES: dict[str, set[str]] = {
    "INTERDICTION_OPERATION": {"INTERDICTION_OPERATION", "INTERDICTION", "SEIZURE"},
    "MARITIME_VIOLENT_INCIDENT": {"MARITIME_VIOLENT_INCIDENT", "ARMED_CLASH", "ATTACK"},
    "ARMED_CLASH": {"ARMED_CLASH", "ATTACK", "MARITIME_VIOLENT_INCIDENT"},
    "MARITIME_ROUTE_DISRUPTION": {"MARITIME_ROUTE_DISRUPTION", "PORT_CLOSURE", "BLOCKADE"},
}


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometers."""
    radius = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * radius * math.asin(min(1.0, math.sqrt(a)))


def _parse_ts(value) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def type_matches(predicted_type: str | None, realized_type: str | None) -> bool:
    if not predicted_type or not realized_type:
        return False
    if predicted_type == realized_type:
        return True
    return realized_type in TYPE_ALIASES.get(predicted_type, set())


def match_prediction(prediction: dict, realized: dict, max_distance_km: float = DEFAULT_MAX_DISTANCE_KM):
    """Return match-detail dict if ``realized`` fulfils ``prediction``, else None.

    A match requires a compatible event type, a realization at/after the prediction
    was generated and within its (max-window + grace), and spatial proximity — by
    distance when both carry coordinates, otherwise by region equality.
    """
    if not type_matches(prediction.get("predicted_event_type"), realized.get("event_type")):
        return None

    lead_days = None
    gen = _parse_ts(prediction.get("generated_at"))
    occ = _parse_ts(realized.get("occurred_at"))
    if gen and occ:
        lead_days = (occ - gen).total_seconds() / 86400.0
        if lead_days < 0:
            return None
        window = (prediction.get("timeframe_max_days") or 90) + WINDOW_GRACE_DAYS
        if lead_days > window:
            return None

    distance = None
    plat, plon = prediction.get("predicted_lat"), prediction.get("predicted_lon")
    rlat, rlon = realized.get("lat"), realized.get("lon")
    if None not in (plat, plon, rlat, rlon):
        distance = haversine_km(plat, plon, rlat, rlon)
        if distance > max_distance_km:
            return None
    elif prediction.get("predicted_region") and realized.get("region"):
        if prediction["predicted_region"] != realized["region"]:
            return None

    return {
        "distance_km": round(distance, 2) if distance is not None else None,
        "lead_time_days": round(lead_days, 2) if lead_days is not None else None,
        "confidence_at_prediction": float(prediction.get("confidence", 0.0) or 0.0),
    }


def build_interdiction_event_cypher(msg: dict) -> tuple[str, dict] | None:
    """Upsert a realized interdiction as an :Event node. None if no event_id."""
    eid = msg.get("event_id")
    if not eid:
        return None
    params = {
        "eid": eid,
        "etype": msg.get("event_type", "INTERDICTION_OPERATION"),
        "region": msg.get("region", ""),
        "lat": msg.get("lat"),
        "lon": msg.get("lon"),
        "occurred_at": msg.get("occurred_at", ""),
        "desc": msg.get("description", ""),
        "source": msg.get("source", "interdictions"),
    }
    cypher = """
    MERGE (e:Event {event_id: $eid})
    ON CREATE SET e.system_created_at = localDateTime()
    SET e.event_type = $etype,
        e.region = $region,
        e.latitude = $lat,
        e.longitude = $lon,
        e.occurred_at = $occurred_at,
        e.description = $desc,
        e.source = $source,
        e.system_updated_at = localDateTime()
    """
    return cypher, params


def build_realized_by_cypher(prediction_id: str, event_id: str, match: dict) -> tuple[str, dict]:
    """Link a prediction to the realized event that fulfilled it, with the score."""
    cypher = """
    MATCH (p:PredictedEvent {prediction_id: $pid})
    MATCH (e:Event {event_id: $eid})
    MERGE (p)-[r:REALIZED_BY]->(e)
    SET p.realized = true,
        p.realized_at = localDateTime(),
        r.distance_km = $distance_km,
        r.lead_time_days = $lead_time_days,
        r.confidence_at_prediction = $confidence_at_prediction
    """
    params = {"pid": prediction_id, "eid": event_id, **match}
    return cypher, params
