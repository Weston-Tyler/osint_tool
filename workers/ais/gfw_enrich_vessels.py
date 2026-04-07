"""
Enrich existing Memgraph Vessel nodes with GFW vessel identity data.

The main gfw_full_ingester.py pulls event data first and then does a
separate vessel-identity lookup pass. If that second pass is interrupted
(rate limits, Ctrl+C, etc.) the graph is left with vessels that have
MMSI only and no name/flag/imo/owner info.

This worker:
  1. Finds all :Vessel nodes with a NULL name
  2. Batches the MMSIs
  3. Calls GFW vessels.search_vessels(query=MMSI) for each
  4. Merges the returned identity data back onto the node

Safe to re-run — it's idempotent (only targets vessels with name IS NULL).
Respects the same GFW_REQUEST_INTERVAL_SEC pacing as the main ingester.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import Any

import gfwapiclient as gfw
from gqlalchemy import Memgraph

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("mda.worker.ais.gfw_enrich")

GFW_API_KEY = os.getenv("GFW_API_KEY")
MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "memgraph")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))

REQUEST_INTERVAL_SEC = float(os.getenv("GFW_REQUEST_INTERVAL_SEC", "1.5"))
MAX_RETRIES = int(os.getenv("GFW_MAX_RETRIES", "5"))
BACKOFF_BASE_SEC = float(os.getenv("GFW_BACKOFF_BASE_SEC", "5.0"))


async def _paced(coro_factory, label: str):
    """Run a GFW call with retry/backoff and rate-limit pacing."""
    for attempt in range(MAX_RETRIES):
        try:
            result = await coro_factory()
            await asyncio.sleep(REQUEST_INTERVAL_SEC)
            return result
        except Exception as e:
            msg = str(e)
            throttle = "429" in msg or "rate" in msg.lower() or "concurrent" in msg.lower()
            if attempt == MAX_RETRIES - 1:
                logger.error("%s: giving up after %d attempts: %s", label, MAX_RETRIES, e)
                return None
            wait = BACKOFF_BASE_SEC * (2 ** attempt) if throttle else BACKOFF_BASE_SEC
            logger.warning("%s: attempt %d/%d failed (%s), backing off %.1fs",
                           label, attempt + 1, MAX_RETRIES, type(e).__name__, wait)
            await asyncio.sleep(wait)
    return None


def _pick(record: dict, *keys) -> Any:
    for k in keys:
        v = record.get(k)
        if v is not None and v != "":
            return v
    return None


def normalize(raw: dict) -> dict | None:
    """Flatten a GFW vessel search result into the fields we store.

    The pydantic model_dump output uses snake_case keys (self_reported_info,
    registry_info, registry_owners, combined_sources_info, etc.) — NOT the
    camelCase shown in the REST docs. The ship_types and gear_types come
    from combined_sources_info, not from self_reported_info.
    """
    if not raw:
        return None
    si_list = raw.get("self_reported_info") or []
    reg_list = raw.get("registry_info") or []
    own_list = raw.get("registry_owners") or []
    csi_list = raw.get("combined_sources_info") or []
    si = si_list[0] if si_list else {}
    reg = reg_list[0] if reg_list else {}
    own = own_list[0] if own_list else {}
    csi = csi_list[0] if csi_list else {}

    # ship_types / gear_types are nested lists of {name, source, year_from, year_to}
    ship_type = None
    if csi.get("ship_types"):
        ship_type = csi["ship_types"][0].get("name")
    elif reg.get("ship_type"):
        ship_type = reg.get("ship_type")

    gear_type = None
    if csi.get("gear_types"):
        gear_type = csi["gear_types"][0].get("name")

    return {
        "gfw_vessel_id": _pick(si, "id") or _pick(csi, "vessel_id") or _pick(reg, "id"),
        "mmsi": _pick(si, "ssvid") or _pick(reg, "ssvid"),
        "imo": _pick(si, "imo") or _pick(reg, "imo"),
        "callsign": _pick(si, "call_sign") or _pick(reg, "call_sign"),
        "name": _pick(si, "ship_name") or _pick(reg, "ship_name") or _pick(si, "n_ship_name"),
        "flag": _pick(si, "flag") or _pick(reg, "flag"),
        "vessel_type": ship_type,
        "gear_type": gear_type,
        "owner_name": _pick(own, "name"),
        "owner_flag": _pick(own, "flag"),
        "first_seen": _pick(si, "transmission_date_from"),
        "last_seen": _pick(si, "transmission_date_to"),
    }


async def enrich_one(client: gfw.Client, mmsi: str) -> dict | None:
    """Look up a single vessel by MMSI via GFW search_vessels."""
    result = await _paced(
        lambda: client.vessels.search_vessels(
            query=mmsi,
            datasets=["public-global-vessel-identity:latest"],
            limit=1,
        ),
        label=f"vessel search mmsi={mmsi}",
    )
    if result is None:
        return None
    try:
        entries = result.data() or []
    except Exception:
        return None
    if not entries:
        return None
    first = entries[0]
    raw = first.model_dump() if hasattr(first, "model_dump") else dict(first)
    return normalize(raw)


def fetch_pending_mmsis(mg: Memgraph, limit: int | None = None) -> list[str]:
    """Return MMSIs of vessels that have not yet had a GFW identity lookup.

    Uses v.identity_lookup_complete IS NULL so we don't re-query vessels
    whose GFW record legitimately has no ship_name (many small fishing
    vessels are nameless in the registry but still have flag + callsign).
    """
    q = (
        "MATCH (v:Vessel) "
        "WHERE v.identity_lookup_complete IS NULL AND v.mmsi IS NOT NULL "
        "RETURN v.mmsi AS mmsi"
    )
    if limit:
        q += f" LIMIT {int(limit)}"
    return [row["mmsi"] for row in mg.execute_and_fetch(q)]


def update_vessel(mg: Memgraph, mmsi: str, ident: dict):
    """Merge the enriched identity fields onto the existing Vessel node.

    Uses coalesce($val, property) to only write non-null fields so a
    vessel with null ship_name but real flag/callsign still gets those
    recorded. Also writes identity_lookup_complete so we can distinguish
    'never enriched' from 'enriched but GFW has no name'.
    """
    mg.execute(
        """
        MATCH (v:Vessel {mmsi: $mmsi})
        SET v.gfw_vessel_id = coalesce($gfw_vessel_id, v.gfw_vessel_id),
            v.imo = coalesce($imo, v.imo),
            v.callsign = coalesce($callsign, v.callsign),
            v.name = coalesce($name, v.name),
            v.flag = coalesce($flag, v.flag),
            v.vessel_type = coalesce($vessel_type, v.vessel_type),
            v.gear_type = coalesce($gear_type, v.gear_type),
            v.owner_name = coalesce($owner_name, v.owner_name),
            v.owner_flag = coalesce($owner_flag, v.owner_flag),
            v.first_seen = coalesce($first_seen, v.first_seen),
            v.last_seen = coalesce($last_seen, v.last_seen),
            v.identity_lookup_complete = true,
            v.identity_enriched_at = localDateTime()
        """,
        {
            "mmsi": mmsi,
            "gfw_vessel_id": ident.get("gfw_vessel_id"),
            "imo": ident.get("imo"),
            "callsign": ident.get("callsign"),
            "name": ident.get("name"),
            "flag": ident.get("flag"),
            "vessel_type": ident.get("vessel_type"),
            "gear_type": ident.get("gear_type"),
            "owner_name": ident.get("owner_name"),
            "owner_flag": ident.get("owner_flag"),
            "first_seen": ident.get("first_seen"),
            "last_seen": ident.get("last_seen"),
        },
    )


async def run(limit: int | None, only_mmsi_prefix: str | None):
    if not GFW_API_KEY:
        raise RuntimeError("GFW_API_KEY environment variable not set")

    mg = Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
    client = gfw.Client(access_token=GFW_API_KEY)

    mmsis = fetch_pending_mmsis(mg, limit)
    if only_mmsi_prefix:
        mmsis = [m for m in mmsis if m.startswith(only_mmsi_prefix)]

    logger.info("Enriching %d vessels (interval=%.1fs, ~%.1f min ETA)",
                len(mmsis), REQUEST_INTERVAL_SEC,
                len(mmsis) * (REQUEST_INTERVAL_SEC + 0.3) / 60.0)

    ok = 0
    nameless = 0
    miss = 0
    for i, mmsi in enumerate(mmsis, 1):
        ident = await enrich_one(client, mmsi)
        if ident is None:
            # No GFW match at all — don't mark complete, let next run retry
            miss += 1
        else:
            # Got a record; write whatever fields are present (flag/callsign
            # are usually set even when ship_name is null). Mark complete.
            update_vessel(mg, mmsi, ident)
            if ident.get("name"):
                ok += 1
            else:
                nameless += 1
        if i % 25 == 0:
            logger.info("Progress: %d/%d (named=%d, nameless=%d, no_match=%d)",
                        i, len(mmsis), ok, nameless, miss)

    logger.info("✓ Done. Named %d, nameless %d, no_match %d, total %d",
                ok, nameless, miss, len(mmsis))


def main() -> int:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None,
                   help="Only enrich up to N vessels this run (for testing)")
    p.add_argument("--prefix", type=str, default=None,
                   help="Only enrich MMSIs starting with this prefix (e.g. 403 for Iran)")
    args = p.parse_args()
    asyncio.run(run(args.limit, args.prefix))
    return 0


if __name__ == "__main__":
    sys.exit(main())
