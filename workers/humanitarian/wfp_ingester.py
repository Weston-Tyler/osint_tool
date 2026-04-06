"""WFP Food Price Monitoring (VAM Data Bridges) ingestion worker.

Fetches monthly food prices and IPC phase classifications from the World Food
Programme VAM Data Bridges API, detects price spikes via rolling z-scores, and
publishes structured events to Kafka.

Reference: https://api.wfp.org/vam-data-bridges/4.0.0/
API key: register at https://dataviz.vam.wfp.org
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from statistics import mean, stdev
from typing import Any

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.humanitarian.wfp")

WFP_API_KEY = os.getenv("WFP_API_KEY")
WFP_BASE_URL = "https://api.wfp.org/vam-data-bridges/4.0.0"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# Key commodities tracked across most countries.  IDs follow the VAM commodity
# catalogue; the actual numeric IDs vary per country but these are the most
# common global defaults.
DEFAULT_COMMODITIES = {
    "wheat_flour": 23,
    "rice": 51,
    "maize": 56,
    "cooking_oil": 87,
    "sugar": 58,
}

# Monitored countries (ISO-3).  Expand as needed.
DEFAULT_COUNTRIES = [
    "AFG", "ETH", "YEM", "SOM", "SSD", "NGA", "COD", "HTI", "SDN", "MMR",
]


class WFPIngester:
    """Fetches WFP VAM food price and IPC data, detects price spikes, and
    publishes to Kafka."""

    def __init__(
        self,
        api_key: str | None = None,
        kafka_bootstrap: str | None = None,
        commodities: dict[str, int] | None = None,
        countries: list[str] | None = None,
    ):
        self.api_key = api_key or WFP_API_KEY
        if not self.api_key:
            raise RuntimeError("WFP_API_KEY environment variable not set")

        self.kafka_bootstrap = kafka_bootstrap or KAFKA_BOOTSTRAP
        self.commodities = commodities or DEFAULT_COMMODITIES
        self.countries = countries or DEFAULT_COUNTRIES

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

    def _publish(self, topic: str, records: list[dict]) -> int:
        producer = self._get_producer()
        count = 0
        for record in records:
            try:
                producer.send(topic, record)
                count += 1
            except Exception as exc:
                logger.error("Failed to publish to %s: %s", topic, exc)
                producer.send(
                    "mda.dlq",
                    {"source": "wfp", "topic": topic, "error": str(exc), "raw": str(record)[:500]},
                )
        producer.flush()
        return count

    # ------------------------------------------------------------------
    # API helpers
    # ------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }

    def _get(self, path: str, params: dict | None = None) -> Any:
        url = f"{WFP_BASE_URL}{path}"
        resp = requests.get(url, headers=self._headers(), params=params or {}, timeout=60)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Data fetchers
    # ------------------------------------------------------------------

    def fetch_food_prices(
        self,
        country_iso3: str,
        start_date: str | None = None,
        end_date: str | None = None,
        commodity_id: int | None = None,
        price_type_id: int = 15,  # 15 = Retail
    ) -> list[dict]:
        """Fetch monthly food prices from /MarketPrices/PriceMonthly.

        Parameters
        ----------
        country_iso3 : str
            ISO-3166-1 alpha-3 country code (e.g. ``"AFG"``).
        start_date, end_date : str, optional
            Date range in ``YYYY-MM-DD`` format.  Defaults to last 24 months.
        commodity_id : int, optional
            WFP commodity ID.  If ``None`` all default commodities are queried.
        price_type_id : int
            Price type (15 = Retail, 16 = Wholesale).
        """
        if start_date is None:
            start_date = (datetime.now(timezone.utc) - timedelta(days=730)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        commodity_ids = [commodity_id] if commodity_id else list(self.commodities.values())
        all_prices: list[dict] = []

        for cid in commodity_ids:
            params = {
                "CountryCode": country_iso3,
                "CommodityID": cid,
                "PriceTypeID": price_type_id,
                "startDate": start_date,
                "endDate": end_date,
                "page": 1,
                "format": "json",
            }
            page = 1
            while True:
                params["page"] = page
                data = self._get("/MarketPrices/PriceMonthly", params)
                items = data.get("items", data) if isinstance(data, dict) else data
                if not isinstance(items, list):
                    items = []
                if not items:
                    break
                all_prices.extend(items)
                # If the API returns fewer items than a full page, we are done.
                if len(items) < 200:
                    break
                page += 1

        logger.info(
            "Fetched %d price records for %s (%s–%s)",
            len(all_prices), country_iso3, start_date, end_date,
        )
        return all_prices

    def fetch_ipc_data(self, country_iso3: str) -> list[dict]:
        """Fetch IPC phase classification data from /Ipc.

        Parameters
        ----------
        country_iso3 : str
            ISO-3166-1 alpha-3 country code.
        """
        params = {"CountryCode": country_iso3, "format": "json"}
        data = self._get("/Ipc", params)
        items = data.get("items", data) if isinstance(data, dict) else data
        if not isinstance(items, list):
            items = []
        logger.info("Fetched %d IPC records for %s", len(items), country_iso3)
        return items

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------

    def detect_price_spike(
        self,
        country_iso3: str,
        commodity_id: int,
        z_threshold: float = 2.0,
        lookback_months: int = 24,
    ) -> list[dict]:
        """Detect price spikes using a rolling z-score on monthly prices.

        Returns a list of spike events where the current month's price
        exceeds ``z_threshold`` standard deviations above the rolling mean.
        """
        start = (datetime.now(timezone.utc) - timedelta(days=lookback_months * 31)).strftime("%Y-%m-%d")
        prices = self.fetch_food_prices(country_iso3, start_date=start, commodity_id=commodity_id)

        if not prices:
            return []

        # Sort by date ascending.
        prices.sort(key=lambda p: p.get("commodityPriceDateMonth", p.get("date", "")))

        # Extract numeric price values.
        values: list[float] = []
        dated_values: list[tuple[str, float]] = []
        for rec in prices:
            price_val = rec.get("commodityPriceMonthly") or rec.get("price")
            date_val = rec.get("commodityPriceDateMonth") or rec.get("date", "")
            if price_val is not None:
                try:
                    fv = float(price_val)
                    values.append(fv)
                    dated_values.append((str(date_val), fv))
                except (ValueError, TypeError):
                    continue

        if len(values) < 6:
            logger.warning(
                "Insufficient data for z-score (%d points) — %s commodity %d",
                len(values), country_iso3, commodity_id,
            )
            return []

        spikes: list[dict] = []
        window = 12  # rolling window size

        for i in range(window, len(dated_values)):
            window_vals = [v for _, v in dated_values[i - window : i]]
            mu = mean(window_vals)
            sigma = stdev(window_vals) if len(window_vals) > 1 else 0.0
            if sigma == 0:
                continue
            date_str, current = dated_values[i]
            z = (current - mu) / sigma
            if z >= z_threshold:
                spikes.append({
                    "event_type": "FOOD_PRICE_SPIKE",
                    "country": country_iso3,
                    "commodity_id": commodity_id,
                    "date": date_str,
                    "price": current,
                    "rolling_mean": round(mu, 2),
                    "rolling_std": round(sigma, 2),
                    "z_score": round(z, 2),
                    "severity": min(10, int(z * 2)),
                    "source": "wfp_vam",
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                })

        logger.info(
            "Detected %d price spikes for %s commodity %d (z>=%.1f)",
            len(spikes), country_iso3, commodity_id, z_threshold,
        )
        return spikes

    # ------------------------------------------------------------------
    # Normalizers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_price_record(record: dict, country_iso3: str) -> dict:
        return {
            "event_id": f"wfp_price_{country_iso3}_{record.get('commodityID', '')}_{record.get('commodityPriceDateMonth', '')}",
            "source": "wfp_vam",
            "event_type": "FOOD_PRICE",
            "country": country_iso3,
            "commodity_id": record.get("commodityID"),
            "commodity_name": record.get("commodityName", ""),
            "market": record.get("marketName", ""),
            "price": record.get("commodityPriceMonthly") or record.get("price"),
            "currency": record.get("currencyName", ""),
            "unit": record.get("commodityUnitName", ""),
            "date": record.get("commodityPriceDateMonth") or record.get("date"),
            "price_type": record.get("priceTypeName", ""),
            "ingest_time": datetime.now(timezone.utc).isoformat(),
            "raw": record,
        }

    @staticmethod
    def _normalize_ipc_record(record: dict, country_iso3: str) -> dict:
        return {
            "event_id": f"wfp_ipc_{country_iso3}_{record.get('ipcPeriod', '')}_{record.get('areaName', '')}",
            "source": "wfp_vam",
            "event_type": "IPC_CLASSIFICATION",
            "country": country_iso3,
            "area": record.get("areaName", ""),
            "ipc_phase": record.get("ipcPhase"),
            "population_phase1": record.get("populationPhase1"),
            "population_phase2": record.get("populationPhase2"),
            "population_phase3": record.get("populationPhase3"),
            "population_phase4": record.get("populationPhase4"),
            "population_phase5": record.get("populationPhase5"),
            "analysis_period": record.get("ipcPeriod", ""),
            "ingest_time": datetime.now(timezone.utc).isoformat(),
            "raw": record,
        }

    # ------------------------------------------------------------------
    # Publishers
    # ------------------------------------------------------------------

    def publish_prices(self) -> int:
        """Fetch and publish food prices for all monitored countries."""
        all_records: list[dict] = []
        for country in self.countries:
            try:
                prices = self.fetch_food_prices(country)
                normalized = [self._normalize_price_record(p, country) for p in prices]
                all_records.extend(normalized)
            except Exception as exc:
                logger.error("Price fetch failed for %s: %s", country, exc)

        count = self._publish("mda.wfp.food_prices", all_records)
        logger.info("Published %d food price records", count)

        # Also detect and publish spikes.
        spike_records: list[dict] = []
        for country in self.countries:
            for cid in self.commodities.values():
                try:
                    spikes = self.detect_price_spike(country, cid)
                    spike_records.extend(spikes)
                except Exception as exc:
                    logger.error("Spike detection failed for %s/%d: %s", country, cid, exc)

        if spike_records:
            spike_count = self._publish("mda.wfp.food_prices", spike_records)
            logger.info("Published %d price spike alerts", spike_count)

        return count

    def publish_ipc(self) -> int:
        """Fetch and publish IPC phase data for all monitored countries."""
        all_records: list[dict] = []
        for country in self.countries:
            try:
                ipc = self.fetch_ipc_data(country)
                normalized = [self._normalize_ipc_record(r, country) for r in ipc]
                all_records.extend(normalized)
            except Exception as exc:
                logger.error("IPC fetch failed for %s: %s", country, exc)

        count = self._publish("mda.wfp.ipc_phases", all_records)
        logger.info("Published %d IPC records", count)
        return count

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run_polling_loop(self, interval_hours: int = 24) -> None:
        """Run the ingestion loop on a fixed interval."""
        logger.info(
            "Starting WFP polling loop (interval=%dh, countries=%d)",
            interval_hours, len(self.countries),
        )
        while True:
            cycle_start = time.monotonic()
            try:
                self.publish_prices()
                self.publish_ipc()
            except Exception as exc:
                logger.exception("WFP polling cycle failed: %s", exc)

            elapsed = time.monotonic() - cycle_start
            sleep_secs = max(0, interval_hours * 3600 - elapsed)
            logger.info("WFP cycle complete in %.1fs, sleeping %.0fs", elapsed, sleep_secs)
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
    ingester = WFPIngester()
    try:
        ingester.run_polling_loop()
    except KeyboardInterrupt:
        logger.info("Shutting down WFP ingester")
    finally:
        ingester.close()
