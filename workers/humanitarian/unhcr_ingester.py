"""UNHCR Refugee Data Finder ingestion worker.

Fetches refugee, IDP, and asylum-seeker population data, demographic breakdowns,
and displacement flows from the UNHCR Population Statistics API, detects
year-over-year displacement surges, and publishes to Kafka.

Reference: https://api.unhcr.org/population/v1/population/
No API key required.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.humanitarian.unhcr")

UNHCR_BASE_URL = "https://api.unhcr.org/population/v1"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# Countries of interest (ISO-3).
DEFAULT_ORIGIN_COUNTRIES = [
    "AFG", "SYR", "UKR", "VEN", "SSD", "MMR", "SOM", "COD", "SDN", "CAF",
    "ERI", "BDI", "IRQ", "NGA", "ETH",
]

DEFAULT_ASYLUM_COUNTRIES = [
    "TUR", "COL", "DEU", "PAK", "UGA", "PER", "POL", "BGD", "SDN", "ETH",
]

# Surge detection: flag YoY increases above this fraction.
SURGE_THRESHOLD = 0.25  # 25 %


class UNHCRIngester:
    """Fetches UNHCR population and displacement data, detects surges, and
    publishes to Kafka."""

    def __init__(
        self,
        kafka_bootstrap: str | None = None,
        origin_countries: list[str] | None = None,
        asylum_countries: list[str] | None = None,
    ):
        self.kafka_bootstrap = kafka_bootstrap or KAFKA_BOOTSTRAP
        self.origin_countries = origin_countries or DEFAULT_ORIGIN_COUNTRIES
        self.asylum_countries = asylum_countries or DEFAULT_ASYLUM_COUNTRIES
        self._producer: KafkaProducer | None = None

    # ------------------------------------------------------------------
    # Kafka helpers
    # ------------------------------------------------------------------

    def _get_producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
        return self._producer

    def _send(self, topic: str, record: dict) -> None:
        try:
            self._get_producer().send(topic, record)
        except Exception as exc:
            logger.error("Kafka send to %s failed: %s", topic, exc)
            self._get_producer().send(
                "mda.dlq",
                {"source": "unhcr", "topic": topic, "error": str(exc), "raw": str(record)[:500]},
            )

    # ------------------------------------------------------------------
    # API helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get(path: str, params: dict | None = None) -> Any:
        url = f"{UNHCR_BASE_URL}{path}"
        all_params = {"format": "json"}
        if params:
            all_params.update(params)
        resp = requests.get(url, params=all_params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _paginate(path: str, params: dict | None = None) -> list[dict]:
        """Fetch all pages from a UNHCR paginated endpoint."""
        url = f"{UNHCR_BASE_URL}{path}"
        all_params: dict[str, Any] = {"format": "json", "page": 1, "limit": 1000}
        if params:
            all_params.update(params)

        results: list[dict] = []
        while True:
            resp = requests.get(url, params=all_params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("items", data.get("results", data.get("data", [])))
            if isinstance(items, list):
                results.extend(items)
            else:
                # Single-page response without pagination wrapper.
                if isinstance(data, list):
                    results.extend(data)
                break
            if len(items) < all_params["limit"]:
                break
            all_params["page"] += 1

        return results

    # ------------------------------------------------------------------
    # Data fetchers
    # ------------------------------------------------------------------

    def fetch_population_data(
        self,
        year: int,
        country_of_origin: str | None = None,
        country_of_asylum: str | None = None,
    ) -> list[dict]:
        """Fetch refugee / IDP / asylum-seeker counts.

        Parameters
        ----------
        year : int
            Data year.
        country_of_origin, country_of_asylum : str, optional
            ISO-3 country codes.
        """
        params: dict[str, Any] = {"year": year}
        if country_of_origin:
            params["country_of_origin"] = country_of_origin
        if country_of_asylum:
            params["country_of_asylum"] = country_of_asylum

        items = self._paginate("/population/", params)
        logger.info(
            "Fetched %d population records (year=%d, origin=%s, asylum=%s)",
            len(items), year, country_of_origin, country_of_asylum,
        )
        return items

    def fetch_demographics(self, year: int, country: str) -> list[dict]:
        """Fetch age/gender demographic breakdowns.

        Parameters
        ----------
        year : int
            Data year.
        country : str
            ISO-3 country code (country of asylum).
        """
        params: dict[str, Any] = {"year": year, "country_of_asylum": country}
        items = self._paginate("/demographics/", params)
        logger.info("Fetched %d demographic records for %s (%d)", len(items), country, year)
        return items

    def fetch_displacement_flows(self, year: int) -> list[dict]:
        """Fetch origin-destination displacement flow data for a given year."""
        items = self._paginate("/flows/", {"year": year})
        logger.info("Fetched %d displacement flow records for %d", len(items), year)
        return items

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------

    def detect_displacement_surge(
        self,
        country: str,
        lookback_years: int = 3,
        threshold: float = SURGE_THRESHOLD,
    ) -> list[dict]:
        """Flag year-over-year refugee/IDP increases > *threshold* (default 25 %).

        Compares the most recent year to each of the prior *lookback_years*
        years.
        """
        current_year = datetime.now(timezone.utc).year
        yearly_totals: dict[int, int] = {}

        for offset in range(lookback_years + 1):
            yr = current_year - offset
            try:
                records = self.fetch_population_data(yr, country_of_origin=country)
                total = 0
                for rec in records:
                    for key in ("refugees", "refugee", "totalPopulation", "total", "idps"):
                        val = rec.get(key)
                        if val is not None:
                            try:
                                total += int(val)
                            except (ValueError, TypeError):
                                pass
                            break
                yearly_totals[yr] = total
            except Exception as exc:
                logger.warning("Could not fetch %d data for %s: %s", yr, country, exc)

        surges: list[dict] = []
        latest_year = max(yearly_totals.keys()) if yearly_totals else None
        if latest_year is None or latest_year not in yearly_totals:
            return surges

        latest_total = yearly_totals[latest_year]

        for yr in sorted(yearly_totals.keys()):
            if yr == latest_year:
                continue
            prev_total = yearly_totals[yr]
            if prev_total == 0:
                continue
            pct_change = (latest_total - prev_total) / prev_total

            if pct_change >= threshold:
                surges.append({
                    "event_type": "DISPLACEMENT_SURGE",
                    "country_of_origin": country,
                    "comparison_year": yr,
                    "latest_year": latest_year,
                    "previous_total": prev_total,
                    "latest_total": latest_total,
                    "pct_change": round(pct_change * 100, 1),
                    "severity": min(10, int(pct_change * 10)),
                    "source": "unhcr",
                    "confidence": 0.90,
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                })

        if surges:
            logger.warning("Detected %d displacement surges for %s", len(surges), country)
        return surges

    # ------------------------------------------------------------------
    # Normalizers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_population(record: dict) -> dict:
        origin = record.get("country_of_origin") or record.get("coo", "")
        asylum = record.get("country_of_asylum") or record.get("coa", "")
        year = record.get("year", "")
        return {
            "event_id": f"unhcr_pop_{origin}_{asylum}_{year}",
            "source": "unhcr",
            "event_type": "DISPLACEMENT_POPULATION",
            "country_of_origin": origin,
            "country_of_origin_name": record.get("country_of_origin_en", ""),
            "country_of_asylum": asylum,
            "country_of_asylum_name": record.get("country_of_asylum_en", ""),
            "year": year,
            "refugees": record.get("refugees"),
            "asylum_seekers": record.get("asylum_seekers"),
            "idps": record.get("idps"),
            "stateless": record.get("stateless"),
            "others_of_concern": record.get("others_of_concern"),
            "total": record.get("totalPopulation") or record.get("total"),
            "ingest_time": datetime.now(timezone.utc).isoformat(),
            "raw": record,
        }

    # ------------------------------------------------------------------
    # Publisher
    # ------------------------------------------------------------------

    def publish(self) -> int:
        """Full ingestion cycle: population data, demographics, flows, and
        surge detection.  All published to ``mda.unhcr.displacement``."""
        producer = self._get_producer()
        total = 0
        current_year = datetime.now(timezone.utc).year

        # --- Population data ---
        for country in self.origin_countries:
            try:
                records = self.fetch_population_data(current_year, country_of_origin=country)
                for rec in records:
                    self._send("mda.unhcr.displacement", self._normalize_population(rec))
                    total += 1
            except Exception as exc:
                logger.error("Population fetch failed for %s: %s", country, exc)

        # --- Displacement flows ---
        try:
            flows = self.fetch_displacement_flows(current_year)
            for flow in flows:
                normalized = {
                    "event_id": f"unhcr_flow_{flow.get('country_of_origin', '')}_{flow.get('country_of_asylum', '')}_{current_year}",
                    "source": "unhcr",
                    "event_type": "DISPLACEMENT_FLOW",
                    "country_of_origin": flow.get("country_of_origin", ""),
                    "country_of_asylum": flow.get("country_of_asylum", ""),
                    "year": current_year,
                    "refugees": flow.get("refugees"),
                    "asylum_seekers": flow.get("asylum_seekers"),
                    "ingest_time": datetime.now(timezone.utc).isoformat(),
                    "raw": flow,
                }
                self._send("mda.unhcr.displacement", normalized)
                total += 1
        except Exception as exc:
            logger.error("Displacement flow fetch failed: %s", exc)

        # --- Demographics for asylum countries ---
        for country in self.asylum_countries:
            try:
                demos = self.fetch_demographics(current_year, country)
                for rec in demos:
                    normalized = {
                        "event_id": f"unhcr_demo_{country}_{rec.get('age_group', '')}_{rec.get('sex', '')}_{current_year}",
                        "source": "unhcr",
                        "event_type": "DISPLACEMENT_DEMOGRAPHICS",
                        "country_of_asylum": country,
                        "year": current_year,
                        "age_group": rec.get("age_group"),
                        "sex": rec.get("sex"),
                        "total": rec.get("total") or rec.get("value"),
                        "ingest_time": datetime.now(timezone.utc).isoformat(),
                        "raw": rec,
                    }
                    self._send("mda.unhcr.displacement", normalized)
                    total += 1
            except Exception as exc:
                logger.error("Demographics fetch failed for %s: %s", country, exc)

        # --- Surge detection ---
        for country in self.origin_countries:
            try:
                surges = self.detect_displacement_surge(country)
                for surge in surges:
                    self._send("mda.unhcr.displacement", surge)
                    total += 1
            except Exception as exc:
                logger.error("Surge detection failed for %s: %s", country, exc)

        producer.flush()
        logger.info("UNHCR publish cycle complete: %d records", total)
        return total

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run_polling_loop(self, interval_days: int = 30) -> None:
        """Run the ingestion loop on a fixed interval (default monthly)."""
        logger.info(
            "Starting UNHCR polling loop (interval=%dd, origin_countries=%d, asylum_countries=%d)",
            interval_days, len(self.origin_countries), len(self.asylum_countries),
        )
        while True:
            cycle_start = time.monotonic()
            try:
                self.publish()
            except Exception as exc:
                logger.exception("UNHCR polling cycle failed: %s", exc)

            elapsed = time.monotonic() - cycle_start
            sleep_secs = max(0, interval_days * 86400 - elapsed)
            logger.info("UNHCR cycle complete in %.1fs, sleeping %.0fs", elapsed, sleep_secs)
            time.sleep(sleep_secs)

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            self._producer = None


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    ingester = UNHCRIngester()
    try:
        ingester.run_polling_loop()
    except KeyboardInterrupt:
        logger.info("Shutting down UNHCR ingester")
    finally:
        ingester.close()
