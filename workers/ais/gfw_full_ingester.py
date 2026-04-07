"""
Global Fishing Watch v3 — Full-coverage ingester.

Pulls every public dataset that's useful for an MDA system:

  Events:
    - Encounters (with sub-type classification)
    - Loitering
    - Port visits
    - Fishing
    - AIS gaps

  Vessels:
    - Identity lookup for every vessel that appears in any event
      (registry, ownership, authorizations, flag history)

  Insights:
    - Per-vessel: fishing/gap/coverage stats + IUU blacklist matching

  4Wings reports:
    - SAR vessel detections (catches dark vessels with no AIS)

  References (one-shot, into PostGIS):
    - EEZ polygons (200nm zones)
    - MPA polygons (Marine Protected Areas)
    - RFMO polygons (Regional Fisheries Management Orgs)

Each event/vessel is published to its own Kafka topic so the
graph-processor can route to typed nodes/relationships in Memgraph.

Reference layers go straight into PostGIS as a one-shot job.

Run:
  python -u gfw_full_ingester.py                  # last 7 days, all event types
  python -u gfw_full_ingester.py --days 30
  python -u gfw_full_ingester.py --references     # one-shot reference layer load
  python -u gfw_full_ingester.py --insights       # vessel insights backfill
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from typing import Any

import asyncpg
import gfwapiclient as gfw
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("mda.worker.ais.gfw_full")

# ── Config ─────────────────────────────────────────────────────────
GFW_API_KEY = os.getenv("GFW_API_KEY")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
PG_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'mda')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'mda')}@"
    f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'mda')}"
)

# ── Topic + dataset mapping ─────────────────────────────────────────
EVENT_DATASETS: dict[str, tuple[str, str]] = {
    # event_type : (dataset, kafka_topic)
    "ENCOUNTER":  ("public-global-encounters-events:latest",  "mda.gfw.encounters"),
    "LOITERING":  ("public-global-loitering-events:latest",   "mda.gfw.loitering"),
    "PORT_VISIT": ("public-global-port-visits-events:latest", "mda.gfw.port_visits"),
    "FISHING":    ("public-global-fishing-events:latest",     "mda.gfw.fishing"),
    "GAP":        ("public-global-gaps-events:latest",        "mda.gfw.gaps"),
}

VESSEL_TOPIC = "mda.gfw.vessels"
INSIGHTS_TOPIC = "mda.gfw.insights"
SAR_TOPIC = "mda.gfw.sar_detections"


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        retries=5,
        acks="all",
    )


def make_client() -> gfw.Client:
    if not GFW_API_KEY:
        raise RuntimeError("GFW_API_KEY environment variable not set")
    return gfw.Client(access_token=GFW_API_KEY)


# ── Normalizers ─────────────────────────────────────────────────────
def normalize_event(event: dict[str, Any], event_type: str) -> dict[str, Any]:
    """Flatten a GFW event into the MDA event schema."""
    pos = event.get("position") or {}
    vessel = event.get("vessel") or {}
    return {
        "source": "global_fishing_watch",
        "event_type": event_type,
        "event_id": f"gfw_{event_type.lower()}_{event.get('id', '')}",
        "start_time": event.get("start"),
        "end_time": event.get("end"),
        "lat": pos.get("lat"),
        "lon": pos.get("lon"),
        "vessel_id": vessel.get("id"),
        "vessel_mmsi": vessel.get("ssvid"),
        "vessel_imo": vessel.get("imo"),
        "vessel_name": vessel.get("name") or vessel.get("shipName"),
        "vessel_flag": vessel.get("flag"),
        "vessel_type": vessel.get("type"),
        # encounter-specific
        "encounter_type": (event.get("encounter") or {}).get("type"),
        "encountered_vessel_id": (event.get("encounter") or {}).get("vessel", {}).get("id"),
        "encountered_vessel_mmsi": (event.get("encounter") or {}).get("vessel", {}).get("ssvid"),
        "median_distance_km": (event.get("encounter") or {}).get("medianDistanceKilometers"),
        "median_speed_knots": (event.get("encounter") or {}).get("medianSpeedKnots"),
        # port-visit-specific
        "port_id": (event.get("port_visit") or {}).get("intermediateAnchorage", {}).get("id"),
        "port_name": (event.get("port_visit") or {}).get("intermediateAnchorage", {}).get("name"),
        "port_country": (event.get("port_visit") or {}).get("intermediateAnchorage", {}).get("flag"),
        # gap-specific
        "gap_hours": (event.get("gap") or {}).get("intentionalDisablingHours"),
        "raw": event,
    }


def normalize_vessel(vessel: dict[str, Any]) -> dict[str, Any]:
    """Flatten a vessel identity record."""
    si = (vessel.get("selfReportedInfo") or [{}])[0] if vessel.get("selfReportedInfo") else {}
    reg = (vessel.get("registryInfo") or [{}])[0] if vessel.get("registryInfo") else {}
    own = (vessel.get("registryOwners") or [{}])[0] if vessel.get("registryOwners") else {}
    return {
        "source": "global_fishing_watch",
        "vessel_id": vessel.get("id") or si.get("id"),
        "mmsi": si.get("ssvid") or reg.get("ssvid"),
        "imo": si.get("imo") or reg.get("imo"),
        "callsign": si.get("callsign") or reg.get("callsign"),
        "name": si.get("shipname") or reg.get("shipname"),
        "flag": si.get("flag") or reg.get("flag"),
        "vessel_type": si.get("shiptype") or reg.get("shiptype"),
        "owner_name": own.get("name"),
        "owner_flag": own.get("flag"),
        "owner_address": own.get("address"),
        "first_seen": si.get("transmissionDateFrom"),
        "last_seen": si.get("transmissionDateTo"),
        "raw": vessel,
    }


def normalize_insight(insight: dict[str, Any], vessel_id: str) -> dict[str, Any]:
    return {
        "source": "global_fishing_watch",
        "vessel_id": vessel_id,
        "iuu_listed": bool(insight.get("vesselIdentity", {}).get("iuuVesselList")),
        "iuu_lists": insight.get("vesselIdentity", {}).get("iuuVesselList") or [],
        "fishing_hours": insight.get("apparentFishing", {}).get("hoursApparentFishing"),
        "gap_hours": insight.get("gap", {}).get("intentionalDisablingHours"),
        "coverage_pct": insight.get("coverage", {}).get("percentage"),
        "raw": insight,
    }


# ── Rate limiting ───────────────────────────────────────────────────
# GFW limit: 50,000 requests/day = ~35 requests/min steady state.
# We pace at REQUEST_INTERVAL_SEC between requests to stay well under that
# AND avoid the per-token concurrent request throttle.
REQUEST_INTERVAL_SEC = float(os.getenv("GFW_REQUEST_INTERVAL_SEC", "1.5"))
MAX_RETRIES = int(os.getenv("GFW_MAX_RETRIES", "5"))
BACKOFF_BASE_SEC = float(os.getenv("GFW_BACKOFF_BASE_SEC", "5.0"))


async def _paced_request(coro_factory, label: str):
    """Run an async GFW client call with retry/backoff and rate-limit pacing."""
    for attempt in range(MAX_RETRIES):
        try:
            result = await coro_factory()
            await asyncio.sleep(REQUEST_INTERVAL_SEC)
            return result
        except Exception as e:
            msg = str(e)
            is_throttle = "429" in msg or "rate" in msg.lower() or "concurrent" in msg.lower()
            if attempt == MAX_RETRIES - 1:
                logger.error("%s: giving up after %d attempts: %s", label, MAX_RETRIES, e)
                raise
            wait = BACKOFF_BASE_SEC * (2 ** attempt) if is_throttle else BACKOFF_BASE_SEC
            logger.warning("%s: attempt %d/%d failed (%s), backing off %.1fs",
                           label, attempt + 1, MAX_RETRIES, type(e).__name__, wait)
            await asyncio.sleep(wait)


# ── Workers ─────────────────────────────────────────────────────────
async def fetch_events(
    client: gfw.Client,
    producer: KafkaProducer,
    event_type: str,
    start_date: date,
    end_date: date,
    max_pages: int | None = None,
) -> set[str]:
    """Fetch all events of a given type, publish to Kafka. Return vessel IDs seen."""
    dataset, topic = EVENT_DATASETS[event_type]
    logger.info("Fetching %s from %s to %s (interval=%.1fs)", event_type, start_date, end_date, REQUEST_INTERVAL_SEC)

    vessel_ids: set[str] = set()
    offset = 0
    page_size = 100
    total = 0
    page_num = 0

    while True:
        if max_pages is not None and page_num >= max_pages:
            logger.info("%s: reached max_pages=%d, stopping early", event_type, max_pages)
            break

        try:
            result = await _paced_request(
                lambda: client.events.get_all_events(
                    datasets=[dataset],
                    start_date=start_date,
                    end_date=end_date,
                    limit=page_size,
                    offset=offset,
                ),
                label=f"{event_type} offset={offset}",
            )
        except Exception as e:
            logger.error("Failed page offset=%d for %s after retries: %s", offset, event_type, e)
            break

        entries = result.data() or []
        if not entries:
            break

        for raw in entries:
            try:
                e = raw.model_dump() if hasattr(raw, "model_dump") else dict(raw)
                normalized = normalize_event(e, event_type)
                producer.send(topic, normalized)
                if normalized.get("vessel_id"):
                    vessel_ids.add(normalized["vessel_id"])
                if normalized.get("encountered_vessel_id"):
                    vessel_ids.add(normalized["encountered_vessel_id"])
                total += 1
            except Exception as ex:
                producer.send("mda.dlq", {"source": "gfw", "event_type": event_type, "error": str(ex)})

        page_num += 1
        if len(entries) < page_size:
            break
        offset += page_size

    producer.flush()
    logger.info("✓ %s: %d events published, %d vessels referenced", event_type, total, len(vessel_ids))
    return vessel_ids


async def fetch_vessel_identities(
    client: gfw.Client,
    producer: KafkaProducer,
    vessel_ids: set[str],
) -> int:
    """Look up identity for every vessel ID we've seen."""
    if not vessel_ids:
        return 0
    logger.info("Looking up identity for %d unique vessels", len(vessel_ids))
    total = 0
    ids = list(vessel_ids)
    batch_size = 50
    for i in range(0, len(ids), batch_size):
        chunk = ids[i : i + batch_size]
        try:
            result = await client.vessels.get_vessels_by_ids(
                ids=chunk,
                datasets=["public-global-vessel-identity:latest"],
            )
            for raw in result.data() or []:
                v = raw.model_dump() if hasattr(raw, "model_dump") else dict(raw)
                producer.send(VESSEL_TOPIC, normalize_vessel(v))
                total += 1
        except Exception as e:
            logger.error("Vessel batch %d-%d failed: %s", i, i + batch_size, e)
    producer.flush()
    logger.info("✓ Vessels: %d identity records published", total)
    return total


async def fetch_insights(
    client: gfw.Client,
    producer: KafkaProducer,
    vessel_ids: set[str],
    start_date: date,
    end_date: date,
) -> int:
    """Pull insights (incl. IUU listing) for each vessel."""
    if not vessel_ids:
        return 0
    logger.info("Fetching insights for %d vessels", len(vessel_ids))
    total = 0
    for vid in vessel_ids:
        try:
            result = await client.insights.get_vessel_insights(
                includes=["FISHING", "GAP", "COVERAGE", "VESSEL-IDENTITY-IUU-VESSEL-LIST"],
                start_date=start_date,
                end_date=end_date,
                vessels=[{"vesselId": vid, "datasetId": "public-global-vessel-identity:latest"}],
            )
            data = result.data() or {}
            d = data.model_dump() if hasattr(data, "model_dump") else dict(data)
            producer.send(INSIGHTS_TOPIC, normalize_insight(d, vid))
            total += 1
        except Exception as e:
            logger.warning("Insight lookup failed for %s: %s", vid, e)
    producer.flush()
    logger.info("✓ Insights: %d records published", total)
    return total


async def fetch_sar_detections(
    client: gfw.Client,
    producer: KafkaProducer,
    start_date: date,
    end_date: date,
) -> int:
    """Pull SAR vessel detections — catches dark vessels with no AIS at all."""
    logger.info("Fetching SAR vessel detections %s → %s", start_date, end_date)
    try:
        result = await client.fourwings.create_sar_presence_report(
            spatial_resolution="HIGH",
            temporal_resolution="DAILY",
            start_date=start_date,
            end_date=end_date,
            spatial_aggregation=False,
        )
    except Exception as e:
        logger.error("SAR report failed: %s", e)
        return 0

    data = result.data() or []
    rows = data if isinstance(data, list) else data.model_dump() if hasattr(data, "model_dump") else []
    for row in rows:
        producer.send(SAR_TOPIC, {"source": "gfw_sar", "raw": row})
    producer.flush()
    logger.info("✓ SAR: %d detections published", len(rows))
    return len(rows)


async def load_reference_layers(client: gfw.Client) -> None:
    """One-shot: load EEZ/MPA/RFMO polygons into PostGIS."""
    pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS gfw_reference_regions (
                id           SERIAL PRIMARY KEY,
                region_type  VARCHAR(16) NOT NULL,
                gfw_id       VARCHAR(64),
                label        TEXT,
                iso3         VARCHAR(8),
                raw          JSONB,
                ingested_at  TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_gfw_ref_type ON gfw_reference_regions(region_type);
            CREATE INDEX IF NOT EXISTS idx_gfw_ref_iso3 ON gfw_reference_regions(iso3);
        """)

        for region_type, fetcher_name in [
            ("EEZ", "get_eez_regions"),
            ("MPA", "get_mpa_regions"),
            ("RFMO", "get_rfmo_regions"),
        ]:
            logger.info("Loading %s regions", region_type)
            try:
                fetcher = getattr(client.references, fetcher_name)
                result = await fetcher()
                items = result.data() or []
                rows = items if isinstance(items, list) else [items]
                for r in rows:
                    rd = r.model_dump() if hasattr(r, "model_dump") else dict(r)
                    await conn.execute(
                        "INSERT INTO gfw_reference_regions (region_type, gfw_id, label, iso3, raw) "
                        "VALUES ($1, $2, $3, $4, $5)",
                        region_type,
                        str(rd.get("id") or ""),
                        rd.get("label") or rd.get("name"),
                        rd.get("iso3"),
                        json.dumps(rd, default=str),
                    )
                logger.info("✓ %s: %d regions loaded", region_type, len(rows))
            except Exception as e:
                logger.error("%s load failed: %s", region_type, e)

    await pool.close()


# ── Main orchestration ──────────────────────────────────────────────
async def run_full_ingest(days: int, do_sar: bool, do_insights: bool, max_pages: int | None) -> None:
    end = date.today()
    start = end - timedelta(days=days)

    client = make_client()
    producer = make_producer()
    all_vessels: set[str] = set()

    try:
        for event_type in ("ENCOUNTER", "LOITERING", "PORT_VISIT", "FISHING", "GAP"):
            try:
                vids = await fetch_events(client, producer, event_type, start, end, max_pages=max_pages)
                all_vessels.update(vids)
            except Exception as e:
                logger.error("%s ingest failed: %s", event_type, e)

        if all_vessels:
            await fetch_vessel_identities(client, producer, all_vessels)

        if do_insights and all_vessels:
            await fetch_insights(client, producer, all_vessels, start, end)

        if do_sar:
            await fetch_sar_detections(client, producer, start, end)

    finally:
        producer.flush()
        producer.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7, help="Lookback window in days")
    parser.add_argument("--no-sar", action="store_true", help="Skip SAR detections (rate-limited)")
    parser.add_argument("--no-insights", action="store_true", help="Skip per-vessel insights")
    parser.add_argument("--references", action="store_true", help="One-shot reference layer load (EEZ/MPA/RFMO)")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Max pages per event type (each page = 100 events). Useful for testing or staying under rate limits.")
    args = parser.parse_args()

    if args.references:
        client = make_client()
        asyncio.run(load_reference_layers(client))
        return 0

    asyncio.run(run_full_ingest(
        days=args.days,
        do_sar=not args.no_sar,
        do_insights=not args.no_insights,
        max_pages=args.max_pages,
    ))
    return 0


if __name__ == "__main__":
    sys.exit(main())
