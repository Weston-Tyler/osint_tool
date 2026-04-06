"""ACLED (Armed Conflict Location and Event Data) ingestion worker.

Fetches conflict events from the ACLED API for MDA-relevant countries,
normalizes them to CausalEvent-compatible dicts, and publishes to Kafka.

Source: https://api.acleddata.com/acled/read
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.acled")

ACLED_API_URL = "https://api.acleddata.com/acled/read"
ACLED_API_KEY = os.getenv("ACLED_API_KEY", "")
ACLED_EMAIL = os.getenv("ACLED_EMAIL", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = "mda.acled.events"

# ACLED event_type -> CausalEvent event_type mapping
EVENT_TYPE_MAP = {
    "Battles": "ARMED_CLASH",
    "Violence against civilians": "VIOLENCE_AGAINST_CIVILIANS",
    "Protests": "PROTEST",
    "Riots": "RIOT",
    "Strategic developments": "STRATEGIC_DEVELOPMENT",
    "Explosions/Remote violence": "REMOTE_VIOLENCE",
}

# MDA-relevant countries: ISO-3166-1 alpha-3 codes
MDA_COUNTRIES = [
    "MEX", "COL", "VEN", "GTM", "HND", "SLV", "NIC", "HTI",
    "SDN", "SOM", "YEM", "ETH", "AFG", "MMR",
]

# ISO alpha-3 -> ISO numeric mapping for ACLED API
ISO3_TO_NUMERIC = {
    "MEX": 484, "COL": 170, "VEN": 862, "GTM": 320, "HND": 340,
    "SLV": 222, "NIC": 558, "HTI": 332, "SDN": 736, "SOM": 706,
    "YEM": 887, "ETH": 231, "AFG": 4, "MMR": 104,
}


class ACLEDIngester:
    """Fetches ACLED conflict events and publishes to Kafka."""

    def __init__(
        self,
        api_key: str = ACLED_API_KEY,
        email: str = ACLED_EMAIL,
        kafka_bootstrap: str = KAFKA_BOOTSTRAP,
    ):
        self.api_key = api_key
        self.email = email
        self.kafka_bootstrap = kafka_bootstrap
        self._producer = None

    @property
    def producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
        return self._producer

    def close(self):
        """Flush and close the Kafka producer."""
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            self._producer = None

    def fetch_events(
        self,
        country_iso3: str,
        start_date: str,
        end_date: str,
        limit: int = 5000,
    ) -> list[dict]:
        """Fetch events from the ACLED API with pagination.

        Args:
            country_iso3: ISO-3166-1 alpha-3 country code (e.g. "COL").
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format.
            limit: Maximum results per page (ACLED max is 5000).

        Returns:
            List of raw ACLED event dicts.
        """
        iso_numeric = ISO3_TO_NUMERIC.get(country_iso3)
        if iso_numeric is None:
            logger.warning("Unknown country ISO3 code: %s", country_iso3)
            return []

        all_events: list[dict] = []
        page = 1

        while True:
            params = {
                "key": self.api_key,
                "email": self.email,
                "event_date": f"{start_date}|{end_date}",
                "event_date_where": "BETWEEN",
                "iso": iso_numeric,
                "limit": limit,
                "page": page,
            }

            logger.info(
                "Fetching ACLED events: country=%s page=%d start=%s end=%s",
                country_iso3, page, start_date, end_date,
            )

            try:
                resp = requests.get(ACLED_API_URL, params=params, timeout=120)
                resp.raise_for_status()
                payload = resp.json()
            except requests.RequestException as exc:
                logger.error("ACLED API request failed: %s", exc)
                break

            if not payload.get("success", False):
                logger.error(
                    "ACLED API returned error: %s",
                    payload.get("error", "unknown"),
                )
                break

            events = payload.get("data", [])
            if not events:
                break

            all_events.extend(events)
            logger.info(
                "Fetched %d events (page %d, total so far: %d)",
                len(events), page, len(all_events),
            )

            # If we got fewer than the limit, we have all results
            if len(events) < limit:
                break

            page += 1

        return all_events

    @staticmethod
    def normalize_event(raw: dict) -> dict:
        """Normalize an ACLED raw event to a CausalEvent-compatible dict.

        Args:
            raw: Raw event dict from the ACLED API.

        Returns:
            Normalized event dict.
        """
        acled_type = raw.get("event_type", "")
        mapped_type = EVENT_TYPE_MAP.get(acled_type, acled_type.upper().replace(" ", "_"))

        lat = raw.get("latitude")
        lon = raw.get("longitude")

        actors = []
        if raw.get("actor1"):
            actors.append(raw["actor1"])
        if raw.get("actor2"):
            actors.append(raw["actor2"])

        fatalities_raw = raw.get("fatalities", 0)
        try:
            fatalities = int(fatalities_raw)
        except (ValueError, TypeError):
            fatalities = 0

        return {
            "event_id": f"acled_{raw.get('data_id', '')}",
            "source": "acled",
            "event_type": mapped_type,
            "sub_event_type": raw.get("sub_event_type", ""),
            "occurred_at": raw.get("event_date", ""),
            "location_lat": float(lat) if lat else None,
            "location_lon": float(lon) if lon else None,
            "location_name": raw.get("location", ""),
            "location_region": raw.get("region", ""),
            "location_admin1": raw.get("admin1", ""),
            "location_admin2": raw.get("admin2", ""),
            "location_country_iso": raw.get("iso3", ""),
            "fatalities": fatalities,
            "actors": actors,
            "actor1": raw.get("actor1", ""),
            "actor2": raw.get("actor2", ""),
            "assoc_actor1": raw.get("assoc_actor_1", ""),
            "assoc_actor2": raw.get("assoc_actor_2", ""),
            "interaction": raw.get("interaction", ""),
            "acled_source": raw.get("source", ""),
            "source_scale": raw.get("source_scale", ""),
            "notes": raw.get("notes", ""),
            "confidence": _estimate_confidence(raw),
            "ingest_time": datetime.now(timezone.utc).isoformat(),
        }

    def ingest_country(self, country_iso3: str, days_back: int = 30) -> int:
        """Fetch recent events for a single country and publish to Kafka.

        Args:
            country_iso3: ISO-3166-1 alpha-3 country code.
            days_back: Number of days to look back from today.

        Returns:
            Number of events published.
        """
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        start_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")

        raw_events = self.fetch_events(country_iso3, start_date, end_date)
        count = 0

        for raw in raw_events:
            event = self.normalize_event(raw)
            self.producer.send(KAFKA_TOPIC, value=event)
            count += 1

        self.producer.flush()
        logger.info(
            "Published %d ACLED events for %s (%s to %s)",
            count, country_iso3, start_date, end_date,
        )
        return count

    def ingest_all_mda_countries(self, days_back: int = 30) -> dict[str, int]:
        """Ingest events for all MDA-relevant countries.

        Args:
            days_back: Number of days to look back from today.

        Returns:
            Dict mapping country ISO3 -> number of events published.
        """
        results: dict[str, int] = {}

        for country in MDA_COUNTRIES:
            try:
                count = self.ingest_country(country, days_back=days_back)
                results[country] = count
            except Exception:
                logger.exception("Failed to ingest ACLED data for %s", country)
                results[country] = 0

        total = sum(results.values())
        logger.info("ACLED ingestion complete: %d total events across %d countries", total, len(results))
        return results

    def run_polling_loop(self, interval_hours: int = 6):
        """Run continuous polling loop.

        Args:
            interval_hours: Hours between ingestion cycles.
        """
        logger.info("Starting ACLED polling loop (interval=%dh)", interval_hours)
        interval_seconds = interval_hours * 3600

        while True:
            try:
                self.ingest_all_mda_countries()
            except Exception:
                logger.exception("Error in ACLED polling cycle")

            logger.info("Sleeping %d hours until next cycle", interval_hours)
            time.sleep(interval_seconds)


def _estimate_confidence(raw: dict) -> float:
    """Estimate confidence score from ACLED metadata.

    Uses source count and source scale as heuristics.
    Returns a float between 0.0 and 1.0.
    """
    score = 0.5

    # ACLED provides a source_scale field indicating source locality
    source_scale = (raw.get("source_scale") or "").lower()
    if source_scale in ("national", "international"):
        score += 0.2
    elif source_scale in ("subnational", "provincial"):
        score += 0.1

    # Multiple sources increase confidence
    source = raw.get("source", "")
    if source:
        source_count = len(source.split(";"))
        if source_count >= 3:
            score += 0.2
        elif source_count >= 2:
            score += 0.1

    # Presence of geolocation data increases confidence
    if raw.get("latitude") and raw.get("longitude"):
        score += 0.1

    return min(score, 1.0)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    ingester = ACLEDIngester()
    try:
        ingester.run_polling_loop()
    finally:
        ingester.close()
