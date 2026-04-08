"""GDELT GEO API client for geographic event search.

Provides spatial search against GDELT's event geography with pre-defined
Maritime Domain Awareness zones and GeoJSON output for Memgraph ingestion.

API reference: https://blog.gdeltproject.org/gdelt-geo-2-0-api-debuts/
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.gdelt.geo_api")

GEO_API_BASE_URL = "https://api.gdeltproject.org/api/v2/geo/geo"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_GEO_EVENTS = "mda.gdelt.geo.events"

# Minimum interval between API requests (seconds). GDELT enforces a
# global 5s/IP limit; we use 5.5s for safety margin.
_RATE_LIMIT_INTERVAL = 5.5
_MAX_RETRIES = 4
_BACKOFF_BASE = 6.0

# Heads up: as of 2026-04, the GDELT GEO 2.0 endpoint
# (https://api.gdeltproject.org/api/v2/geo/geo) appears to return HTML
# 404 from the web server, suggesting the endpoint was removed. The
# rate-limit/backoff/loop scaffolding here is preserved in case GDELT
# restores the endpoint or we point at a working alternative; in the
# meantime, geographic data should be sourced from GKG (Locations field)
# and the DOC API with `near:lat,lon,radius` query syntax.
_MAX_NEAR_RADIUS_KM = 500.0

# Pre-defined MDA maritime zones of interest
# Each zone is defined by a bounding box: (min_lat, max_lat, min_lon, max_lon)
MDA_ZONES: dict[str, dict[str, Any]] = {
    "eastern_pacific": {
        "description": "Eastern Pacific corridor (Central America to Mexico)",
        "min_lat": 0,
        "max_lat": 20,
        "min_lon": -105,
        "max_lon": -75,
        "query_suffix": "drug OR trafficking OR cartel OR maritime",
    },
    "caribbean": {
        "description": "Caribbean Sea",
        "min_lat": 10,
        "max_lat": 25,
        "min_lon": -90,
        "max_lon": -60,
        "query_suffix": "drug OR trafficking OR maritime OR smuggling",
    },
    "gulf_of_mexico": {
        "description": "Gulf of Mexico",
        "min_lat": 18,
        "max_lat": 31,
        "min_lon": -100,
        "max_lon": -80,
        "query_suffix": "maritime OR cartel OR oil OR border",
    },
    "suez_canal": {
        "description": "Suez Canal and approaches",
        "min_lat": 27,
        "max_lat": 32,
        "min_lon": 32,
        "max_lon": 35,
        "query_suffix": "maritime OR shipping OR blockade OR vessel",
    },
    "strait_of_malacca": {
        "description": "Strait of Malacca",
        "min_lat": -1,
        "max_lat": 7,
        "min_lon": 99,
        "max_lon": 105,
        "query_suffix": "piracy OR maritime OR shipping OR vessel seizure",
    },
    "gulf_of_guinea": {
        "description": "Gulf of Guinea (West Africa)",
        "min_lat": -5,
        "max_lat": 10,
        "min_lon": -10,
        "max_lon": 12,
        "query_suffix": "piracy OR maritime OR oil OR kidnapping",
    },
}


class GDELTGeoAPI:
    """Client for the GDELT GEO 2.0 API with MDA spatial search."""

    def __init__(self, publish_to_kafka: bool = True) -> None:
        self._session = requests.Session()
        self._last_request_time: float = 0.0
        self._producer: KafkaProducer | None = None
        self._publish_to_kafka = publish_to_kafka

    # ------------------------------------------------------------------
    # Kafka helpers
    # ------------------------------------------------------------------

    def _get_producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
        return self._producer

    def _publish(self, topic: str, records: list[dict[str, Any]]) -> None:
        if not self._publish_to_kafka or not records:
            return
        producer = self._get_producer()
        for record in records:
            producer.send(topic, record)
        producer.flush()

    # ------------------------------------------------------------------
    # Rate-limited request
    # ------------------------------------------------------------------

    def _rate_limited_get(self, params: dict[str, Any]) -> requests.Response:
        """Execute a GET request with rate limiting and 429/5xx backoff."""
        for attempt in range(_MAX_RETRIES + 1):
            elapsed = time.monotonic() - self._last_request_time
            if elapsed < _RATE_LIMIT_INTERVAL:
                time.sleep(_RATE_LIMIT_INTERVAL - elapsed)

            logger.debug("GEO API request params: %s", params)
            resp = self._session.get(GEO_API_BASE_URL, params=params, timeout=60)
            self._last_request_time = time.monotonic()

            if resp.status_code in (429, 502, 503, 504):
                if attempt >= _MAX_RETRIES:
                    resp.raise_for_status()
                delay = _BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    "GEO API %d on attempt %d/%d, sleeping %ds before retry",
                    resp.status_code, attempt + 1, _MAX_RETRIES + 1, int(delay),
                )
                time.sleep(delay)
                continue

            resp.raise_for_status()
            return resp
        raise RuntimeError("retry loop exited without returning")

    # ------------------------------------------------------------------
    # Core search methods
    # ------------------------------------------------------------------

    def search_point_data(
        self,
        query: str,
        timespan: str = "7d",
        format: str = "GeoJSON",
        sourcelang: str | None = None,
        sourcecountry: str | None = None,
        geo_resolution: float | None = None,
    ) -> dict[str, Any]:
        """Search for geographic point data matching a query.

        Args:
            query: GDELT GEO API query string.
            timespan: Lookback window (e.g. ``"7d"``, ``"30d"``).
            format: Output format -- ``GeoJSON``, ``KML``, or ``CSV``.
            sourcelang: Optional ISO language filter.
            sourcecountry: Optional ISO country filter.
            geo_resolution: Resolution in degrees (GEORES parameter).

        Returns:
            GeoJSON FeatureCollection dict (or raw text for non-JSON formats).
        """
        params: dict[str, Any] = {
            "query": query,
            "format": format,
            "timespan": timespan,
            "mode": "PointData",
        }
        if sourcelang:
            params["sourcelang"] = sourcelang
        if sourcecountry:
            params["sourcecountry"] = sourcecountry
        if geo_resolution is not None:
            params["GEORES"] = str(geo_resolution)

        resp = self._rate_limited_get(params)

        if format == "GeoJSON":
            try:
                geojson = resp.json()
            except ValueError:
                logger.error("GEO API returned non-JSON for query: %s", query)
                return {"type": "FeatureCollection", "features": []}

            # Publish individual features as events
            features = geojson.get("features", [])
            causal_events = [
                self._feature_to_causal_event(f, query) for f in features
            ]
            causal_events = [e for e in causal_events if e is not None]
            self._publish(TOPIC_GEO_EVENTS, causal_events)

            logger.info(
                "GEO API returned %d features for query: %s",
                len(features),
                query,
            )
            return geojson
        else:
            logger.info("GEO API returned %s data for query: %s", format, query)
            return {"raw": resp.text, "format": format}

    def search_heatmap(
        self,
        query: str,
        timespan: str = "7d",
        format: str = "GeoJSON",
        geo_resolution: float | None = None,
    ) -> dict[str, Any]:
        """Search for geographic heatmap data.

        Args:
            query: GDELT GEO API query string.
            timespan: Lookback window.
            format: Output format.
            geo_resolution: Resolution in degrees.

        Returns:
            GeoJSON FeatureCollection or raw response.
        """
        params: dict[str, Any] = {
            "query": query,
            "format": format,
            "timespan": timespan,
            "mode": "HeatMap",
        }
        if geo_resolution is not None:
            params["GEORES"] = str(geo_resolution)

        resp = self._rate_limited_get(params)

        if format == "GeoJSON":
            try:
                return resp.json()
            except ValueError:
                logger.error("GEO API heatmap returned non-JSON for: %s", query)
                return {"type": "FeatureCollection", "features": []}
        return {"raw": resp.text, "format": format}

    def search_near_location(
        self,
        lat: float,
        lon: float,
        radius_km: float,
        timespan: str = "7d",
        query: str | None = None,
    ) -> dict[str, Any]:
        """Search for events near a specific geographic point.

        Builds a geo-bounded query using the ``near:`` operator.

        Args:
            lat: Latitude of center point.
            lon: Longitude of center point.
            radius_km: Search radius in kilometers.
            timespan: Lookback window.
            query: Optional additional keyword query to combine with location.

        Returns:
            GeoJSON FeatureCollection dict.
        """
        # GDELT GEO API uses "near:LAT,LON,RADIUS_KM" syntax
        geo_query = f"near:{lat},{lon},{radius_km}"
        if query:
            geo_query = f"{query} {geo_query}"

        return self.search_point_data(query=geo_query, timespan=timespan)

    def search_mda_zones(
        self, timespan: str = "7d"
    ) -> dict[str, dict[str, Any]]:
        """Run pre-defined searches for all MDA maritime zones.

        Iterates over each zone in ``MDA_ZONES``, builds a bounding-box query,
        and returns the collected GeoJSON results per zone.

        Args:
            timespan: Lookback window for all zone queries.

        Returns:
            Dict mapping zone name to GeoJSON FeatureCollection.
        """
        results: dict[str, dict[str, Any]] = {}
        for zone_name, zone_def in MDA_ZONES.items():
            try:
                center_lat = (zone_def["min_lat"] + zone_def["max_lat"]) / 2
                center_lon = (zone_def["min_lon"] + zone_def["max_lon"]) / 2

                # Approximate radius from bounding box diagonal
                lat_span = zone_def["max_lat"] - zone_def["min_lat"]
                lon_span = zone_def["max_lon"] - zone_def["min_lon"]
                # Rough km conversion: 1 degree latitude ~ 111 km
                radius_km = max(lat_span, lon_span) * 111 / 2

                query = zone_def["query_suffix"]

                if radius_km > _MAX_NEAR_RADIUS_KM:
                    # Zone is too large for near: syntax (would 404).
                    # Fall back to a plain text query without geo filter.
                    logger.info(
                        "MDA zone [%s]: radius %dkm > %dkm cap, "
                        "using plain query (no near: filter)",
                        zone_name, int(radius_km), int(_MAX_NEAR_RADIUS_KM),
                    )
                    geojson = self.search_point_data(
                        query=query, timespan=timespan
                    )
                else:
                    geojson = self.search_near_location(
                        lat=center_lat,
                        lon=center_lon,
                        radius_km=radius_km,
                        timespan=timespan,
                        query=query,
                    )

                feature_count = len(geojson.get("features", []))
                logger.info(
                    "MDA zone [%s] (%s): %d features",
                    zone_name,
                    zone_def["description"],
                    feature_count,
                )
                results[zone_name] = geojson
            except Exception:
                logger.exception("Error searching MDA zone: %s", zone_name)
                results[zone_name] = {"type": "FeatureCollection", "features": []}

        return results

    # ------------------------------------------------------------------
    # GeoJSON to CausalEvent conversion
    # ------------------------------------------------------------------

    @staticmethod
    def _feature_to_causal_event(
        feature: dict[str, Any], source_query: str
    ) -> dict[str, Any] | None:
        """Convert a GeoJSON Feature to a CausalEvent-compatible dict for Memgraph.

        The CausalEvent schema is the internal MDA event representation used
        for graph-based causal reasoning.

        Args:
            feature: GeoJSON Feature dict from the GDELT GEO API.
            source_query: The query that produced this feature.

        Returns:
            CausalEvent dict or None if the feature lacks required geometry.
        """
        geometry = feature.get("geometry")
        properties = feature.get("properties", {})

        if not geometry or geometry.get("type") != "Point":
            return None

        coords = geometry.get("coordinates", [])
        if len(coords) < 2:
            return None

        lon, lat = coords[0], coords[1]

        # Extract properties from GDELT GEO API response
        name = properties.get("name", properties.get("title", ""))
        url = properties.get("url", properties.get("shareimage", ""))
        tone = properties.get("tone", 0.0)
        source = properties.get("domain", properties.get("source", ""))

        return {
            "event_id": f"gdelt_geo_{hash(f'{lat}_{lon}_{name}')}",
            "event_type": "GDELT_GEO_EVENT",
            "source": "gdelt_geo_api",
            "source_query": source_query,
            "title": name,
            "url": url,
            "latitude": lat,
            "longitude": lon,
            "tone": float(tone) if tone else 0.0,
            "source_domain": source,
            "properties": properties,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close underlying HTTP session and Kafka producer."""
        self._session.close()
        if self._producer is not None:
            try:
                self._producer.flush()
                self._producer.close()
            except Exception:
                logger.exception("Error closing Kafka producer")
            self._producer = None


def main() -> None:
    """CLI: run a sweep of all MDA zones, optionally on a loop."""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    ap = argparse.ArgumentParser(description="GDELT GEO API MDA zone sweep")
    ap.add_argument(
        "--loop",
        type=int,
        default=0,
        metavar="SECONDS",
        help="Re-run the sweep every N seconds (default: one-shot)",
    )
    args = ap.parse_args()

    client = GDELTGeoAPI(publish_to_kafka=True)
    iteration = 0
    try:
        while True:
            iteration += 1
            logger.info("GEO API sweep iteration %d starting", iteration)
            try:
                results = client.search_mda_zones(timespan="7d")
                total_features = sum(
                    len(r.get("features", [])) for r in results.values()
                )
                logger.info(
                    "GEO API sweep iteration %d complete: %d zones, %d features",
                    iteration, len(results), total_features,
                )
            except Exception:
                logger.exception("GEO sweep iteration %d failed", iteration)
            if args.loop <= 0:
                break
            logger.info("Sleeping %ds before next sweep", args.loop)
            time.sleep(args.loop)
    finally:
        client.close()


if __name__ == "__main__":
    main()
