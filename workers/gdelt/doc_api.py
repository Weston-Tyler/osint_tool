"""GDELT DOC API client for full-text article search.

Provides structured access to the GDELT DOC 2.0 API with pre-built
Maritime Domain Awareness query templates and Kafka publishing.

API reference: https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.gdelt.doc_api")

DOC_API_BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_DOC_ARTICLES = "mda.gdelt.doc.articles"

# Pre-built MDA domain query templates.
# GDELT DOC query syntax: quotes = exact phrase, parentheses = OR groups,
# theme:CODE filters by GKG theme. Wrapping an OR clause in quotes searches
# for the literal phrase including the word "OR", so use parentheses instead.
MDA_QUERY_TEMPLATES: dict[str, dict[str, str]] = {
    "cartel_mexico": {
        "query": "(cartel OR narco OR trafficking) sourcecountry:MX",
        "description": "Cartel and narcotics activity in Mexico",
    },
    "maritime_piracy": {
        "query": "(maritime piracy OR vessel seizure) theme:MARITIME",
        "description": "Maritime piracy and vessel seizure events",
    },
    "sanctions_evasion": {
        "query": "(sanctions evasion OR flag state) theme:SANCTIONS",
        "description": "Sanctions evasion and flag state issues",
    },
    "fentanyl_seizure": {
        "query": "(fentanyl OR cocaine) (seizure OR seized OR confiscated)",
        "description": "Fentanyl and cocaine seizure reports",
    },
    "drone_border": {
        "query": "(drone OR UAS) (border OR incursion)",
        "description": "Drone and UAS border incursion reports",
    },
}

# Minimum interval between API requests (seconds). GDELT enforces a
# global 5s/IP limit and applies extended cooldowns after bursts. From
# a shared Codespace IP we observed throttling for tens of minutes after
# the first burst, so we use a generous 8s spacing. Combined with the
# 60s startup cooldown below, this avoids tripping the extended throttle.
_RATE_LIMIT_INTERVAL = 8.0
_MAX_RETRIES = 4
_BACKOFF_BASE = 10.0
_STARTUP_COOLDOWN_SEC = 60


class GDELTDocAPI:
    """Client for the GDELT DOC 2.0 API with MDA-focused helpers."""

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

            logger.debug("DOC API request params: %s", params)
            resp = self._session.get(DOC_API_BASE_URL, params=params, timeout=60)
            self._last_request_time = time.monotonic()

            if resp.status_code in (429, 502, 503, 504):
                if attempt >= _MAX_RETRIES:
                    resp.raise_for_status()
                delay = _BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    "DOC API %d on attempt %d/%d, sleeping %ds before retry",
                    resp.status_code, attempt + 1, _MAX_RETRIES + 1, int(delay),
                )
                time.sleep(delay)
                continue

            resp.raise_for_status()
            return resp
        # Unreachable, but mypy/lint demands a return
        raise RuntimeError("retry loop exited without returning")

    # ------------------------------------------------------------------
    # Core search methods
    # ------------------------------------------------------------------

    def search_articles(
        self,
        query: str,
        timespan: str = "30d",
        max_records: int = 250,
        mode: str = "artlist",
        sourcelang: str | None = None,
        sourcecountry: str | None = None,
        startdatetime: str | None = None,
        enddatetime: str | None = None,
    ) -> list[dict[str, Any]]:
        """Search for articles matching the given query.

        Args:
            query: GDELT DOC API query string.
            timespan: Lookback window (e.g. ``"7d"``, ``"30d"``).
            max_records: Maximum articles to return (max 250).
            mode: API mode -- ``artlist``, ``artgallery``, etc.
            sourcelang: Optional ISO language filter (e.g. ``"english"``).
            sourcecountry: Optional ISO country filter.
            startdatetime: Optional start datetime ``YYYYMMDDHHMMSS``.
            enddatetime: Optional end datetime ``YYYYMMDDHHMMSS``.

        Returns:
            List of article dicts.
        """
        params: dict[str, Any] = {
            "query": query,
            "mode": mode,
            "format": "json",
            "timespan": timespan,
            "maxrecords": min(max_records, 250),
        }
        if sourcelang:
            params["sourcelang"] = sourcelang
        if sourcecountry:
            params["sourcecountry"] = sourcecountry
        if startdatetime:
            params["startdatetime"] = startdatetime
        if enddatetime:
            params["enddatetime"] = enddatetime

        resp = self._rate_limited_get(params)

        try:
            data = resp.json()
        except ValueError:
            logger.error("DOC API returned non-JSON response for query: %s", query)
            return []

        articles = data.get("articles", [])

        # Annotate with ingestion metadata
        enriched: list[dict[str, Any]] = []
        for article in articles:
            article["_mda_source"] = "gdelt_doc_api"
            article["_mda_query"] = query
            article["_mda_ingested_at"] = datetime.now(timezone.utc).isoformat()
            enriched.append(article)

        self._publish(TOPIC_DOC_ARTICLES, enriched)
        logger.info(
            "DOC API search returned %d articles for query: %s", len(enriched), query
        )
        return enriched

    def search_by_theme(
        self, theme_code: str, timespan: str = "30d"
    ) -> list[dict[str, Any]]:
        """Search for articles matching a specific GDELT theme code.

        Args:
            theme_code: GDELT theme code (e.g. ``"MARITIME_PIRACY"``).
            timespan: Lookback window.

        Returns:
            List of article dicts.
        """
        query = f"theme:{theme_code}"
        return self.search_articles(query=query, timespan=timespan)

    def search_timeline_tone(
        self, query: str, timespan: str = "30d"
    ) -> dict[str, Any]:
        """Retrieve tone timeline data for a query.

        Returns:
            Raw JSON response with timeline tone series.
        """
        params = {
            "query": query,
            "mode": "timelinetone",
            "format": "json",
            "timespan": timespan,
        }
        resp = self._rate_limited_get(params)
        try:
            return resp.json()
        except ValueError:
            logger.error("DOC API timeline tone returned non-JSON for: %s", query)
            return {}

    def search_timeline_volume(
        self, query: str, timespan: str = "30d"
    ) -> dict[str, Any]:
        """Retrieve volume timeline data for a query.

        Returns:
            Raw JSON response with timeline volume series.
        """
        params = {
            "query": query,
            "mode": "timelinevolume",
            "format": "json",
            "timespan": timespan,
        }
        resp = self._rate_limited_get(params)
        try:
            return resp.json()
        except ValueError:
            logger.error("DOC API timeline volume returned non-JSON for: %s", query)
            return {}

    def search_mda_domain(
        self, domain: str, region: str | None = None, timespan: str = "7d"
    ) -> list[dict[str, Any]]:
        """Convenience method using pre-built MDA query templates.

        Args:
            domain: One of the MDA_QUERY_TEMPLATES keys:
                ``cartel_mexico``, ``maritime_piracy``, ``sanctions_evasion``,
                ``fentanyl_seizure``, ``drone_border``.
            region: Optional additional region/country filter appended to query.
            timespan: Lookback window.

        Returns:
            List of article dicts.
        """
        template = MDA_QUERY_TEMPLATES.get(domain)
        if template is None:
            available = ", ".join(MDA_QUERY_TEMPLATES.keys())
            raise ValueError(
                f"Unknown MDA domain '{domain}'. Available: {available}"
            )

        query = template["query"]
        if region:
            query = f"{query} sourcecountry:{region}"

        logger.info("MDA domain search [%s]: %s", domain, query)
        return self.search_articles(query=query, timespan=timespan)

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


def _sweep_once(client: "GDELTDocAPI") -> int:
    """Run one sweep of all MDA domain templates, returning total article count."""
    total = 0
    for domain in MDA_QUERY_TEMPLATES:
        try:
            articles = client.search_mda_domain(domain, timespan="7d")
            logger.info("Domain [%s]: %d articles", domain, len(articles))
            total += len(articles)
        except Exception:
            logger.exception("Error searching domain %s", domain)
    return total


def main() -> None:
    """CLI: run a sweep of all MDA domain templates, optionally on a loop."""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    ap = argparse.ArgumentParser(description="GDELT DOC API MDA sweep")
    ap.add_argument(
        "--loop",
        type=int,
        default=0,
        metavar="SECONDS",
        help="Re-run the sweep every N seconds (default: one-shot)",
    )
    args = ap.parse_args()

    if _STARTUP_COOLDOWN_SEC > 0:
        logger.info(
            "DOC API startup cooldown: sleeping %ds before first request "
            "to escape any extended IP throttle",
            _STARTUP_COOLDOWN_SEC,
        )
        time.sleep(_STARTUP_COOLDOWN_SEC)

    client = GDELTDocAPI(publish_to_kafka=True)
    iteration = 0
    try:
        while True:
            iteration += 1
            logger.info("DOC API sweep iteration %d starting", iteration)
            try:
                total = _sweep_once(client)
                logger.info(
                    "DOC API sweep iteration %d complete: %d total articles",
                    iteration, total,
                )
            except Exception:
                logger.exception("Sweep iteration %d failed", iteration)
            if args.loop <= 0:
                break
            logger.info("Sleeping %ds before next sweep", args.loop)
            time.sleep(args.loop)
    finally:
        client.close()


if __name__ == "__main__":
    main()
