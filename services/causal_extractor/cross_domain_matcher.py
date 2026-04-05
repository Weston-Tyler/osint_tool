"""Cross-domain event matcher for the MDA causal extraction engine.

For each new validated event, queries PostgreSQL for recent events
within a domain-specific spatial radius and temporal window, computes
four similarity scores (spatial, temporal, entity overlap, CAMEO
transition probability), combines them into a weighted composite with
a cross-domain boost, and emits the top-20 candidate matches per event.
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from uuid import uuid4

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer, KafkaProducer

logger = logging.getLogger("mda.causal.cross_domain_matcher")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CONSUME_TOPIC = os.getenv("CROSS_DOMAIN_CONSUME_TOPIC", "mda.causal.events.validated")
PRODUCE_TOPIC = os.getenv("CROSS_DOMAIN_PRODUCE_TOPIC", "mda.causal.cross_domain.matches")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "mda.dlq")
CONSUMER_GROUP = os.getenv("CROSS_DOMAIN_CONSUMER_GROUP", "causal-cross-domain-matcher")

PG_DSN = os.getenv(
    "PG_DSN",
    "host=localhost port=5432 dbname=mda user=mda password=mda",
)

TOP_K = int(os.getenv("CROSS_DOMAIN_TOP_K", "20"))

# ---------------------------------------------------------------------------
# Domain constants
# ---------------------------------------------------------------------------

# Spatial search radii per domain (km)
DOMAIN_SPATIAL_RADII: dict[str, float] = {
    "maritime": 500.0,
    "territorial": 100.0,
    "market": 5000.0,
    "humanitarian": 250.0,
    "political": 1000.0,
    "criminal": 150.0,
    "conflict": 200.0,
    "information": 2000.0,
    "kinetic": 200.0,
}

# Temporal search windows for domain pairs (days)
# Key format: "source_domain->target_domain"
DOMAIN_TEMPORAL_WINDOWS: dict[str, int] = {
    "maritime->criminal": 60,
    "criminal->maritime": 90,
    "market->humanitarian": 120,
    "information->kinetic": 7,
    "humanitarian->criminal": 90,
    "criminal->political": 60,
    "political->humanitarian": 90,
    "political->criminal": 45,
    "political->market": 30,
    "maritime->market": 30,
    "market->maritime": 30,
    "kinetic->humanitarian": 30,
    "kinetic->political": 14,
    "kinetic->information": 3,
    "information->political": 14,
    "information->humanitarian": 30,
    "maritime->humanitarian": 60,
    "humanitarian->maritime": 60,
    "humanitarian->political": 90,
    "natural_event->criminal": 180,
}

# Default temporal window when no specific domain-pair mapping exists
DEFAULT_TEMPORAL_WINDOW_DAYS = 60

# Default spatial radius when domain is unknown
DEFAULT_SPATIAL_RADIUS_KM = 500.0

# Composite score weights
WEIGHT_SPATIAL = 0.30
WEIGHT_TEMPORAL = 0.35
WEIGHT_ENTITY = 0.25
WEIGHT_CAMEO = 0.10

# Cross-domain boost multiplier
CROSS_DOMAIN_BOOST = 1.15

# Minimum composite score to emit as a candidate
MIN_COMPOSITE = float(os.getenv("CROSS_DOMAIN_MIN_COMPOSITE", "0.10"))

# Maximum lookback for candidate query (days)
MAX_LOOKBACK_DAYS = 180

# ---------------------------------------------------------------------------
# CAMEO transition probability matrix (representative subset)
# ---------------------------------------------------------------------------

# Probability that CAMEO event code X precedes/causes CAMEO code Y.
# These are empirically calibrated from GDELT data for the MDA domain.
CAMEO_TRANSITION_PROBS: dict[tuple[str, str], float] = {
    # Verbal conflict -> material conflict escalation
    ("01", "14"): 0.08,  # Make public statement -> Protest
    ("02", "14"): 0.12,  # Appeal -> Protest
    ("03", "17"): 0.06,  # Express intent to cooperate -> Coerce
    ("05", "14"): 0.10,  # Engage in diplomatic cooperation -> Protest (failure)
    ("06", "17"): 0.15,  # Engage in material cooperation -> Coerce
    ("10", "14"): 0.49,  # Demand -> Protest
    ("10", "17"): 0.18,  # Demand -> Coerce
    ("10", "18"): 0.10,  # Demand -> Assault
    ("11", "14"): 0.22,  # Disapprove -> Protest
    ("11", "17"): 0.14,  # Disapprove -> Coerce
    ("12", "14"): 0.25,  # Reject -> Protest
    ("12", "17"): 0.18,  # Reject -> Coerce
    ("12", "18"): 0.08,  # Reject -> Assault
    ("13", "17"): 0.20,  # Threaten -> Coerce
    ("13", "18"): 0.15,  # Threaten -> Assault
    ("13", "19"): 0.10,  # Threaten -> Fight
    ("14", "17"): 0.64,  # Protest -> Coerce
    ("14", "18"): 0.12,  # Protest -> Assault
    ("14", "19"): 0.08,  # Protest -> Fight
    ("15", "17"): 0.20,  # Exhibit force -> Coerce
    ("15", "18"): 0.18,  # Exhibit force -> Assault
    ("15", "19"): 0.12,  # Exhibit force -> Fight
    ("17", "18"): 0.25,  # Coerce -> Assault
    ("17", "19"): 0.58,  # Coerce -> Fight
    ("17", "20"): 0.10,  # Coerce -> Mass violence
    ("18", "08"): 0.52,  # Assault -> Yield
    ("18", "19"): 0.30,  # Assault -> Fight
    ("18", "20"): 0.15,  # Assault -> Mass violence
    ("19", "18"): 0.71,  # Fight -> Assault (reciprocal)
    ("19", "20"): 0.20,  # Fight -> Mass violence
    ("20", "10"): 0.67,  # Mass violence -> Demand
    # De-escalation patterns
    ("14", "05"): 0.05,  # Protest -> Diplomatic cooperation
    ("17", "05"): 0.08,  # Coerce -> Diplomatic cooperation
    ("18", "05"): 0.03,  # Assault -> Diplomatic cooperation
    ("19", "05"): 0.02,  # Fight -> Diplomatic cooperation
}

# Default CAMEO transition probability for unmapped pairs
DEFAULT_CAMEO_PROB = 0.02


# ---------------------------------------------------------------------------
# Haversine distance
# ---------------------------------------------------------------------------


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute great-circle distance in kilometres."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.asin(math.sqrt(min(a, 1.0)))


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class CrossDomainMatch:
    """A candidate cross-domain causal match."""
    match_id: str = field(default_factory=lambda: str(uuid4()))
    source_event_id: str = ""
    source_event_type: str = ""
    source_domain: str = ""
    candidate_event_id: str = ""
    candidate_event_type: str = ""
    candidate_domain: str = ""
    domain_pair: str = ""
    spatial_score: float = 0.0
    temporal_score: float = 0.0
    entity_overlap_score: float = 0.0
    cameo_score: float = 0.0
    composite_score: float = 0.0
    cross_domain: bool = False
    distance_km: Optional[float] = None
    temporal_lag_days: float = 0.0
    overlapping_entities: list[str] = field(default_factory=list)
    matched_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# ---------------------------------------------------------------------------
# Scoring functions
# ---------------------------------------------------------------------------


def compute_spatial_score(
    distance_km: Optional[float],
    source_domain: str,
    candidate_domain: str,
) -> float:
    """Score spatial proximity relative to domain-specific radius.

    Returns 1.0 when events are co-located, decaying linearly to 0.0
    at the domain radius boundary.  Returns 0.5 when coordinates are
    unavailable.
    """
    if distance_km is None:
        return 0.5

    # Use the larger radius of the two domains
    r1 = DOMAIN_SPATIAL_RADII.get(source_domain, DEFAULT_SPATIAL_RADIUS_KM)
    r2 = DOMAIN_SPATIAL_RADII.get(candidate_domain, DEFAULT_SPATIAL_RADIUS_KM)
    radius = max(r1, r2)

    if radius <= 0:
        return 0.0
    if distance_km <= 0:
        return 1.0

    score = max(0.0, 1.0 - (distance_km / radius))
    return round(score, 4)


def compute_temporal_score(
    lag_days: float,
    source_domain: str,
    candidate_domain: str,
) -> float:
    """Score temporal proximity relative to domain-pair window.

    Returns 1.0 when events are simultaneous, decaying linearly to
    0.0 at the window boundary.  Only considers positive lags where
    the candidate precedes the source event.
    """
    if lag_days < 0:
        return 0.0

    key = f"{candidate_domain}->{source_domain}"
    window = DOMAIN_TEMPORAL_WINDOWS.get(key, DEFAULT_TEMPORAL_WINDOW_DAYS)

    if window <= 0:
        return 0.0
    if lag_days <= 0:
        return 1.0

    score = max(0.0, 1.0 - (lag_days / window))
    return round(score, 4)


def compute_entity_overlap_score(
    source_entities: list[str],
    candidate_entities: list[str],
) -> float:
    """Jaccard similarity between entity sets."""
    if not source_entities or not candidate_entities:
        return 0.0

    s1 = {e.lower().strip() for e in source_entities if e}
    s2 = {e.lower().strip() for e in candidate_entities if e}

    if not s1 or not s2:
        return 0.0

    intersection = s1 & s2
    union = s1 | s2

    return round(len(intersection) / len(union), 4) if union else 0.0


def compute_cameo_score(
    source_cameo: Optional[str],
    candidate_cameo: Optional[str],
) -> float:
    """CAMEO transition probability score.

    Looks up the probability of the candidate's CAMEO root code
    transitioning to the source event's CAMEO root code.
    """
    if not source_cameo or not candidate_cameo:
        return DEFAULT_CAMEO_PROB

    # Normalise CAMEO codes to 2-digit root codes
    cand = candidate_cameo[:2].zfill(2)
    src = source_cameo[:2].zfill(2)

    # candidate -> source direction (candidate is the potential cause)
    return CAMEO_TRANSITION_PROBS.get((cand, src), DEFAULT_CAMEO_PROB)


def compute_composite_score(
    spatial: float,
    temporal: float,
    entity: float,
    cameo: float,
    is_cross_domain: bool,
) -> float:
    """Weighted composite score with optional cross-domain boost."""
    raw = (
        WEIGHT_SPATIAL * spatial
        + WEIGHT_TEMPORAL * temporal
        + WEIGHT_ENTITY * entity
        + WEIGHT_CAMEO * cameo
    )

    if is_cross_domain:
        raw *= CROSS_DOMAIN_BOOST

    return round(min(raw, 1.0), 4)


# ---------------------------------------------------------------------------
# Candidate query
# ---------------------------------------------------------------------------


def _get_temporal_window(source_domain: str) -> int:
    """Get the maximum temporal window for a source domain across all
    domain pairs where this domain is the target (i.e. potential effect)."""
    windows = [
        v for k, v in DOMAIN_TEMPORAL_WINDOWS.items()
        if k.endswith(f"->{source_domain}")
    ]
    if windows:
        return max(windows)
    return DEFAULT_TEMPORAL_WINDOW_DAYS


def _get_spatial_radius_deg(source_domain: str) -> float:
    """Convert km radius to approximate degree offset for bounding-box query."""
    # Use the maximum radius across all domains so we don't miss candidates
    all_radii = list(DOMAIN_SPATIAL_RADII.values())
    radius_km = max(all_radii) if all_radii else DEFAULT_SPATIAL_RADIUS_KM
    # Worst-case (equatorial) conversion: 1 degree ~ 111 km
    return radius_km / 111.0


def query_candidates(
    conn,
    event: dict[str, Any],
    source_domain: str,
) -> list[dict[str, Any]]:
    """Query PostgreSQL for recent events within spatial and temporal bounds.

    Returns events that occurred BEFORE the source event (potential
    upstream causes) within the maximum lookback window.
    """
    lat = event.get("location_lat")
    lon = event.get("location_lon")
    occurred_at = event.get("occurred_at")
    event_id = event.get("event_id", "")

    if occurred_at is None:
        return []

    if isinstance(occurred_at, str):
        try:
            occurred_at = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
        except ValueError:
            return []

    window_days = min(_get_temporal_window(source_domain), MAX_LOOKBACK_DAYS)
    time_start = occurred_at - timedelta(days=window_days)

    # Build query — spatial filter is optional (some events lack coords)
    if lat is not None and lon is not None:
        radius_deg = _get_spatial_radius_deg(source_domain)
        query = """
            SELECT
                event_id,
                event_type,
                domain,
                occurred_at,
                location_lat,
                location_lon,
                location_region,
                entities_involved,
                confidence,
                description
            FROM causal_events
            WHERE event_id != %s
              AND occurred_at BETWEEN %s AND %s
              AND confidence >= 0.4
              AND location_lat BETWEEN %s AND %s
              AND location_lon BETWEEN %s AND %s
            ORDER BY occurred_at DESC
            LIMIT 500
        """
        params = (
            event_id,
            time_start, occurred_at,
            lat - radius_deg, lat + radius_deg,
            lon - radius_deg, lon + radius_deg,
        )
    else:
        query = """
            SELECT
                event_id,
                event_type,
                domain,
                occurred_at,
                location_lat,
                location_lon,
                location_region,
                entities_involved,
                confidence,
                description
            FROM causal_events
            WHERE event_id != %s
              AND occurred_at BETWEEN %s AND %s
              AND confidence >= 0.4
            ORDER BY occurred_at DESC
            LIMIT 500
        """
        params = (event_id, time_start, occurred_at)

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    except Exception as exc:
        logger.error("Candidate query failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Main matcher
# ---------------------------------------------------------------------------


class CrossDomainMatcher:
    """Cross-domain event matcher that scores and ranks candidate matches.

    For each incoming validated event, finds upstream events from
    PostgreSQL, computes four component scores, applies composite
    weighting with cross-domain boost, and returns the top-K matches.
    """

    def __init__(self, pg_dsn: str = PG_DSN, top_k: int = TOP_K) -> None:
        self.pg_dsn = pg_dsn
        self.top_k = top_k
        self._conn: Optional[Any] = None
        logger.info("CrossDomainMatcher initialised (top_k=%d)", top_k)

    def _get_conn(self):
        """Get or create PostgreSQL connection with auto-reconnect."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.pg_dsn)
            self._conn.autocommit = True
        return self._conn

    def match(self, event: dict[str, Any]) -> list[CrossDomainMatch]:
        """Find and score cross-domain matches for a validated event.

        Returns up to ``top_k`` matches sorted by composite score
        descending.
        """
        source_domain = (event.get("domain") or "unknown").lower()
        source_entities = event.get("entities_involved") or []
        source_cameo = event.get("cameo_code") or event.get("event_code")
        source_lat = event.get("location_lat")
        source_lon = event.get("location_lon")
        source_time = event.get("occurred_at")

        if source_time is None:
            return []

        if isinstance(source_time, str):
            try:
                source_time = datetime.fromisoformat(
                    source_time.replace("Z", "+00:00")
                )
            except ValueError:
                return []

        conn = self._get_conn()
        candidates = query_candidates(conn, event, source_domain)

        if not candidates:
            return []

        matches: list[CrossDomainMatch] = []

        for cand in candidates:
            cand_lat = cand.get("location_lat")
            cand_lon = cand.get("location_lon")
            cand_time = cand.get("occurred_at")
            cand_domain = (cand.get("domain") or "unknown").lower()
            cand_entities = cand.get("entities_involved") or []
            cand_cameo = cand.get("cameo_code") or cand.get("event_code")

            if cand_time is None:
                continue

            if isinstance(cand_time, str):
                try:
                    cand_time = datetime.fromisoformat(
                        cand_time.replace("Z", "+00:00")
                    )
                except ValueError:
                    continue

            # Temporal lag (positive means candidate happened before source)
            lag_days = (source_time - cand_time).total_seconds() / 86400.0
            if lag_days < 0 or lag_days > MAX_LOOKBACK_DAYS:
                continue

            # Spatial distance (None if coordinates missing)
            dist_km: Optional[float] = None
            if (
                source_lat is not None
                and source_lon is not None
                and cand_lat is not None
                and cand_lon is not None
            ):
                dist_km = round(
                    haversine_km(source_lat, source_lon, cand_lat, cand_lon), 1,
                )

            is_cross_domain = (
                source_domain != "unknown"
                and cand_domain != "unknown"
                and source_domain != cand_domain
            )
            domain_pair = f"{cand_domain}->{source_domain}"

            # Compute component scores
            spatial = compute_spatial_score(dist_km, cand_domain, source_domain)
            temporal = compute_temporal_score(lag_days, source_domain, cand_domain)
            entity = compute_entity_overlap_score(source_entities, cand_entities)
            cameo = compute_cameo_score(source_cameo, cand_cameo)
            composite = compute_composite_score(
                spatial, temporal, entity, cameo, is_cross_domain,
            )

            if composite < MIN_COMPOSITE:
                continue

            # Compute overlapping entities for explainability
            overlapping: list[str] = []
            if source_entities and cand_entities:
                s1 = {e.lower().strip() for e in source_entities if e}
                s2 = {e.lower().strip() for e in cand_entities if e}
                overlapping = sorted(s1 & s2)

            match_obj = CrossDomainMatch(
                source_event_id=event.get("event_id", ""),
                source_event_type=event.get("event_type", ""),
                source_domain=source_domain,
                candidate_event_id=cand.get("event_id", ""),
                candidate_event_type=cand.get("event_type", ""),
                candidate_domain=cand_domain,
                domain_pair=domain_pair,
                spatial_score=spatial,
                temporal_score=temporal,
                entity_overlap_score=entity,
                cameo_score=cameo,
                composite_score=composite,
                cross_domain=is_cross_domain,
                distance_km=dist_km,
                temporal_lag_days=round(lag_days, 2),
                overlapping_entities=overlapping,
            )
            matches.append(match_obj)

        # Sort by composite score descending and take top-K
        matches.sort(key=lambda m: m.composite_score, reverse=True)
        return matches[: self.top_k]


# ---------------------------------------------------------------------------
# Kafka pipeline
# ---------------------------------------------------------------------------


class CrossDomainMatchPipeline:
    """Kafka consumer/producer pipeline for cross-domain event matching."""

    def __init__(self) -> None:
        self.matcher = CrossDomainMatcher()

        self.consumer = KafkaConsumer(
            CONSUME_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            max_poll_records=20,
            enable_auto_commit=True,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
            retries=3,
        )

        logger.info(
            "CrossDomainMatchPipeline initialised — consuming %s", CONSUME_TOPIC,
        )

    def run(self) -> None:
        """Main processing loop."""
        logger.info("Cross-domain match pipeline started")

        for msg in self.consumer:
            event = msg.value
            event_id = event.get("event_id", "unknown")

            try:
                matches = self.matcher.match(event)

                if matches:
                    payload = {
                        "source_event_id": event_id,
                        "source_event_type": event.get("event_type", ""),
                        "source_domain": event.get("domain", ""),
                        "match_count": len(matches),
                        "top_composite_score": matches[0].composite_score,
                        "cross_domain_count": sum(
                            1 for m in matches if m.cross_domain
                        ),
                        "matches": [asdict(m) for m in matches],
                        "extraction_method": "CROSS_DOMAIN_MATCH",
                        "matched_at": datetime.now(timezone.utc).isoformat(),
                    }
                    self.producer.send(PRODUCE_TOPIC, payload)

                    logger.info(
                        "Cross-domain matched %d candidates for event %s "
                        "(top score=%.3f, cross_domain=%d)",
                        len(matches),
                        event_id,
                        matches[0].composite_score,
                        sum(1 for m in matches if m.cross_domain),
                    )

            except Exception as exc:
                logger.error(
                    "Cross-domain match error on %s: %s",
                    event_id, exc, exc_info=True,
                )
                self.producer.send(DLQ_TOPIC, {
                    "source": "cross_domain_matcher",
                    "error": str(exc),
                    "event_id": event_id,
                    "_original_topic": msg.topic,
                })


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    pipeline = CrossDomainMatchPipeline()
    pipeline.run()
