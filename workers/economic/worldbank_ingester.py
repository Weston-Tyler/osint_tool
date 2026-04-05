"""World Bank Open Data ingestion for MDA economic monitoring.

Fetches development indicators and commodity prices from the World Bank API
for MDA-monitored countries, computes economic stress scores, and publishes
to Kafka.

Source: https://api.worldbank.org/v2/ (free, no API key required)
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Any

import pandas as pd
import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.economic.worldbank")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
WB_BASE_URL = "https://api.worldbank.org/v2"

# World Bank commodity price data (Pink Sheet)
WB_COMMODITY_URL = (
    "https://thedocs.worldbank.org/en/doc/"
    "5d903e848db1d1b83e0ec8f744e55570-0350012021/related/"
    "CMOHistoricalDataMonthly.xlsx"
)


class WorldBankIngester:
    """Weekly ingestion worker for World Bank development indicators."""

    # Indicators relevant to MDA economic intelligence
    TRACKED_INDICATORS: dict[str, str] = {
        "NY.GDP.MKTP.CD": "GDP (current USD)",
        "NY.GDP.PCAP.CD": "GDP per capita (current USD)",
        "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
        "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
        "SL.UEM.TOTL.ZS": "Unemployment (% of labor force, ILO)",
        "NE.TRD.GNFS.ZS": "Trade openness (% of GDP)",
        "BN.CAB.XOKA.CD": "Current account balance (BoP, current USD)",
        "SI.POV.DDAY": "Poverty headcount ratio ($2.15/day, %)",
        "BX.TRF.PWKR.CD.DT": "Personal remittances received (current USD)",
        "PA.NUS.FCRF": "Official exchange rate (LCU per USD, period avg)",
        "GC.DOD.TOTL.GD.ZS": "Central government debt (% of GDP)",
        "FI.RES.TOTL.CD": "Total reserves incl. gold (current USD)",
    }

    # MDA-monitored countries (ISO-2 codes the WB API expects)
    MDA_COUNTRIES: list[str] = [
        "US", "MX", "CO", "VE", "EC", "PA", "GT", "HN", "NI", "CR",
        "BZ", "CU", "JM", "PE", "BO", "BR", "DO", "HT", "SV", "PY",
    ]

    def __init__(self, kafka_bootstrap: str | None = None):
        self.kafka_bootstrap = kafka_bootstrap or KAFKA_BOOTSTRAP
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

    def _close_producer(self) -> None:
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            self._producer = None

    # ------------------------------------------------------------------
    # World Bank API
    # ------------------------------------------------------------------

    def fetch_indicator(
        self,
        indicator_id: str,
        country_codes: list[str] | None = None,
        start_year: int | None = None,
        end_year: int | None = None,
        per_page: int = 1000,
    ) -> pd.DataFrame:
        """Fetch a single indicator for one or more countries (paginated).

        Parameters
        ----------
        indicator_id : str
            World Bank indicator code.
        country_codes : list[str], optional
            ISO-2 country codes.  Defaults to :pyattr:`MDA_COUNTRIES`.
        start_year, end_year : int, optional
            Year range.  Defaults to ``[current_year - 20, current_year]``.
        per_page : int
            Page size for API pagination.

        Returns
        -------
        pd.DataFrame
            Columns: ``country_code``, ``country_name``, ``indicator_id``,
            ``indicator_name``, ``year``, ``value``.
        """
        if country_codes is None:
            country_codes = self.MDA_COUNTRIES
        now_year = datetime.utcnow().year
        if start_year is None:
            start_year = now_year - 20
        if end_year is None:
            end_year = now_year

        country_str = ";".join(country_codes)
        url = f"{WB_BASE_URL}/country/{country_str}/indicator/{indicator_id}"

        all_rows: list[dict[str, Any]] = []
        page = 1

        while True:
            params = {
                "format": "json",
                "date": f"{start_year}:{end_year}",
                "per_page": per_page,
                "page": page,
            }
            logger.debug("GET %s page=%d", url, page)
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            payload = resp.json()

            # WB API returns [metadata, data] where data can be None
            if not isinstance(payload, list) or len(payload) < 2 or payload[1] is None:
                break

            meta, data = payload[0], payload[1]
            for item in data:
                if item.get("value") is None:
                    continue
                all_rows.append(
                    {
                        "country_code": item["countryiso3code"],
                        "country_name": item["country"]["value"],
                        "indicator_id": indicator_id,
                        "indicator_name": item["indicator"]["value"],
                        "year": int(item["date"]),
                        "value": float(item["value"]),
                    }
                )

            total_pages = int(meta.get("pages", 1))
            if page >= total_pages:
                break
            page += 1
            time.sleep(0.3)

        df = pd.DataFrame(all_rows)
        logger.info(
            "Fetched %d rows for indicator %s across %d countries",
            len(df),
            indicator_id,
            len(country_codes),
        )
        return df

    def fetch_commodity_prices(self) -> pd.DataFrame:
        """Download World Bank Pink Sheet commodity price data.

        Returns a DataFrame with monthly commodity prices (energy, agriculture,
        metals, fertilizers).  Falls back to an empty DataFrame on error.
        """
        logger.info("Downloading World Bank Pink Sheet commodity prices")
        try:
            df = pd.read_excel(
                WB_COMMODITY_URL,
                sheet_name="Monthly Prices",
                header=4,
                engine="openpyxl",
            )
            df = df.rename(columns={df.columns[0]: "date"})
            df = df.dropna(subset=["date"])
            logger.info("Loaded %d rows of commodity price data", len(df))
            return df
        except Exception:
            logger.exception("Failed to fetch commodity prices from World Bank Pink Sheet")
            return pd.DataFrame()

    def fetch_all_mda_countries(
        self,
        start_year: int | None = None,
        end_year: int | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Fetch all tracked indicators for MDA-monitored countries.

        Returns a dict keyed by indicator ID.
        """
        results: dict[str, pd.DataFrame] = {}
        for indicator_id in self.TRACKED_INDICATORS:
            try:
                df = self.fetch_indicator(
                    indicator_id,
                    start_year=start_year,
                    end_year=end_year,
                )
                results[indicator_id] = df
            except Exception:
                logger.exception("Failed to fetch indicator %s", indicator_id)
            time.sleep(0.5)

        logger.info(
            "Fetched %d / %d indicators for MDA countries",
            len(results),
            len(self.TRACKED_INDICATORS),
        )
        return results

    # ------------------------------------------------------------------
    # Economic stress scoring
    # ------------------------------------------------------------------

    def detect_economic_stress(
        self,
        country: str,
        lookback_years: int = 5,
    ) -> dict[str, Any]:
        """Compute a composite economic stress score for a country.

        Components (each 0-1, higher = more stress):
        1. GDP contraction — negative GDP growth in latest year.
        2. High inflation — inflation above 10 % threshold.
        3. Currency depreciation — year-over-year exchange rate increase > 15 %.
        4. High unemployment — unemployment above 12 %.
        5. Low reserves — reserves/GDP below 10 %.

        Returns a dict with individual component scores and weighted composite.
        """
        now_year = datetime.utcnow().year
        start = now_year - lookback_years

        def _latest_value(indicator_id: str) -> float | None:
            try:
                df = self.fetch_indicator(
                    indicator_id,
                    country_codes=[country],
                    start_year=start,
                    end_year=now_year,
                )
                if df.empty:
                    return None
                latest = df.sort_values("year").iloc[-1]
                return float(latest["value"])
            except Exception:
                logger.warning("Could not fetch %s for %s", indicator_id, country)
                return None

        gdp_growth = _latest_value("NY.GDP.MKTP.KD.ZG")
        inflation = _latest_value("FP.CPI.TOTL.ZG")
        fx_rate = _latest_value("PA.NUS.FCRF")
        unemployment = _latest_value("SL.UEM.TOTL.ZS")

        # Previous year exchange rate for depreciation calc
        prev_fx: float | None = None
        if fx_rate is not None:
            try:
                df_fx = self.fetch_indicator(
                    "PA.NUS.FCRF",
                    country_codes=[country],
                    start_year=start,
                    end_year=now_year,
                )
                if len(df_fx) >= 2:
                    sorted_fx = df_fx.sort_values("year")
                    prev_fx = float(sorted_fx.iloc[-2]["value"])
            except Exception:
                pass

        # Component scores (0.0 = no stress, 1.0 = max stress)
        scores: dict[str, float | None] = {}

        # GDP contraction
        if gdp_growth is not None:
            scores["gdp_contraction"] = min(max(-gdp_growth / 10.0, 0.0), 1.0)
        else:
            scores["gdp_contraction"] = None

        # Inflation stress
        if inflation is not None:
            scores["high_inflation"] = min(max((inflation - 5.0) / 45.0, 0.0), 1.0)
        else:
            scores["high_inflation"] = None

        # Currency depreciation
        if fx_rate is not None and prev_fx is not None and prev_fx > 0:
            pct_change = (fx_rate - prev_fx) / prev_fx * 100
            scores["currency_depreciation"] = min(max(pct_change / 50.0, 0.0), 1.0)
        else:
            scores["currency_depreciation"] = None

        # Unemployment
        if unemployment is not None:
            scores["high_unemployment"] = min(max((unemployment - 5.0) / 20.0, 0.0), 1.0)
        else:
            scores["high_unemployment"] = None

        # Composite — weighted average of available components
        weights = {
            "gdp_contraction": 0.30,
            "high_inflation": 0.25,
            "currency_depreciation": 0.20,
            "high_unemployment": 0.15,
        }
        weighted_sum = 0.0
        weight_total = 0.0
        for key, weight in weights.items():
            val = scores.get(key)
            if val is not None:
                weighted_sum += val * weight
                weight_total += weight

        composite = weighted_sum / weight_total if weight_total > 0 else None

        result = {
            "country": country,
            "scores": scores,
            "composite_stress": composite,
            "assessment_date": datetime.utcnow().isoformat(),
            "source": "worldbank",
        }
        logger.info("Economic stress for %s: composite=%.3f", country, composite or 0)
        return result

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def publish_indicators(self) -> int:
        """Fetch all indicators for MDA countries and publish to Kafka.

        Topic: ``mda.economic.worldbank.indicators``

        Returns total messages published.
        """
        producer = self._get_producer()
        total = 0

        all_data = self.fetch_all_mda_countries()

        for indicator_id, df in all_data.items():
            if df.empty:
                continue
            for _, row in df.iterrows():
                msg = {
                    "indicator_id": row["indicator_id"],
                    "indicator_name": row["indicator_name"],
                    "country_code": row["country_code"],
                    "country_name": row["country_name"],
                    "year": int(row["year"]),
                    "value": float(row["value"]),
                    "source": "worldbank",
                    "ingest_time": datetime.utcnow().isoformat(),
                }
                producer.send("mda.economic.worldbank.indicators", msg)
                total += 1

        # Publish stress scores for each country
        for country in self.MDA_COUNTRIES:
            try:
                stress = self.detect_economic_stress(country)
                producer.send("mda.economic.worldbank.indicators", stress)
                total += 1
            except Exception:
                logger.exception("Stress detection failed for %s", country)
            time.sleep(0.5)

        producer.flush()
        logger.info("Published %d messages to mda.economic.worldbank.indicators", total)
        return total

    # ------------------------------------------------------------------
    # Polling loop
    # ------------------------------------------------------------------

    def run_polling_loop(self, interval_days: float = 7) -> None:
        """Continuously refresh World Bank data on a weekly cadence.

        Cycle:
        1. Publish indicator data for all MDA countries.
        2. Sleep for *interval_days*.
        """
        logger.info(
            "Starting World Bank polling loop — %d indicators, %d countries, interval %d d",
            len(self.TRACKED_INDICATORS),
            len(self.MDA_COUNTRIES),
            interval_days,
        )

        while True:
            cycle_start = time.time()
            try:
                self.publish_indicators()
            except Exception:
                logger.exception("Error in World Bank polling cycle")
            finally:
                self._close_producer()

            elapsed = time.time() - cycle_start
            sleep_secs = max(0, interval_days * 86400 - elapsed)
            logger.info("World Bank cycle finished in %.1f s — sleeping %.0f s", elapsed, sleep_secs)
            time.sleep(sleep_secs)


# ------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    ingester = WorldBankIngester()
    ingester.run_polling_loop()
