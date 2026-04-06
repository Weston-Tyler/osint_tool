"""FRED (Federal Reserve Economic Data) ingestion for MDA economic monitoring.

Fetches tracked macroeconomic time series from the FRED API, detects anomalies
via rolling z-score, and publishes raw series and anomaly alerts to Kafka.

Source: https://api.stlouisfed.org/fred/
Requires: FRED_API_KEY (free at https://fred.stlouisfed.org/docs/api/api_key.html)
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.economic.fred")

FRED_API_KEY = os.getenv("FRED_API_KEY", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
FRED_BASE_URL = "https://api.stlouisfed.org/fred"


class FREDIngester:
    """Continuous ingestion worker for FRED economic time series."""

    # ~30 series relevant to Maritime Domain Awareness and drug-corridor economics.
    TRACKED_SERIES: dict[str, str] = {
        # GDP & output
        "GDPC1": "Real GDP (quarterly, SAAR)",
        "A191RL1Q225SBEA": "Real GDP growth rate (quarterly)",
        "INDPRO": "Industrial Production Index",
        # Prices & inflation
        "CPIAUCSL": "CPI All Urban Consumers (monthly)",
        "CPILFESL": "Core CPI (ex food & energy)",
        "PPIACO": "PPI All Commodities",
        # Energy
        "DCOILWTICO": "Crude Oil WTI (daily)",
        "DCOILBRENTEU": "Crude Oil Brent (daily)",
        "DHHNGSP": "Henry Hub Natural Gas Spot (daily)",
        "GASREGW": "US Regular Gasoline Price (weekly)",
        # Agriculture / food commodities
        "WPU0121": "PPI Wheat",
        "WPU0122": "PPI Corn",
        "WPU0223": "PPI Processed Poultry",
        "APU0000708111": "CPI Eggs (monthly)",
        # Metals & safe havens
        "GOLDAMGBD228NLBM": "Gold London Fix (daily)",
        # Currency & trade
        "DTWEXBGS": "Trade-Weighted US Dollar Index (daily)",
        "DEXMXUS": "MXN/USD Exchange Rate (daily)",
        "DEXCOUS": "COP/USD Exchange Rate (daily)",
        "EXVZUS": "VEF/USD Exchange Rate (monthly)",
        "DEXBZUS": "BRL/USD Exchange Rate (daily)",
        # Labor
        "UNRATE": "Unemployment Rate (monthly)",
        "ICSA": "Initial Jobless Claims (weekly)",
        # Trade & balance of payments
        "BOPGSTB": "Trade Balance Goods & Services (monthly)",
        "IMPGS": "Imports of Goods & Services",
        # Consumer & business sentiment
        "UMCSENT": "U Michigan Consumer Sentiment (monthly)",
        "CSCICP03USM665S": "OECD Consumer Confidence (monthly)",
        # Financial stress / volatility
        "VIXCLS": "CBOE VIX (daily)",
        "TEDRATE": "TED Spread (daily)",
        "T10Y2Y": "10Y-2Y Treasury Spread (daily)",
        "STLFSI2": "St. Louis Fed Financial Stress Index (weekly)",
    }

    # Note: The Baltic Dry Index (BDI) is not available on FRED.  A separate
    # ingester or manual commentary feed should be used for BDI data (e.g.
    # from the Baltic Exchange or scraped proxies).  We leave a placeholder
    # constant for downstream consumers that expect this key.
    BDI_COMMENTARY = (
        "Baltic Dry Index (BDI) is not directly available from FRED. "
        "Integrate via Baltic Exchange API or third-party provider."
    )

    def __init__(
        self,
        api_key: str | None = None,
        kafka_bootstrap: str | None = None,
    ):
        self.api_key = api_key or FRED_API_KEY
        if not self.api_key:
            raise ValueError("FRED_API_KEY must be set via env var or constructor arg")
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
    # FRED API
    # ------------------------------------------------------------------

    def fetch_series(
        self,
        series_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
        *,
        realtime_start: str | None = None,
        realtime_end: str | None = None,
    ) -> pd.DataFrame:
        """Fetch observations for a single FRED series.

        Parameters
        ----------
        series_id : str
            FRED series identifier (e.g. ``"GDPC1"``).
        start_date, end_date : str, optional
            ISO-8601 date strings (``"YYYY-MM-DD"``).  Defaults to 5 years of
            history when *start_date* is omitted.
        realtime_start, realtime_end : str, optional
            Vintage date range for real-time data.

        Returns
        -------
        pd.DataFrame
            Columns: ``date``, ``value``, ``series_id``.
        """
        if start_date is None:
            start_date = (datetime.utcnow() - timedelta(days=5 * 365)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.utcnow().strftime("%Y-%m-%d")

        params: dict[str, Any] = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": start_date,
            "observation_end": end_date,
            "sort_order": "asc",
        }
        if realtime_start:
            params["realtime_start"] = realtime_start
        if realtime_end:
            params["realtime_end"] = realtime_end

        url = f"{FRED_BASE_URL}/series/observations"
        logger.debug("GET %s  series_id=%s", url, series_id)
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()

        observations = resp.json().get("observations", [])
        rows = []
        for obs in observations:
            val = obs.get("value", ".")
            if val == ".":
                continue  # FRED uses "." for missing values
            rows.append(
                {
                    "date": obs["date"],
                    "value": float(val),
                    "series_id": series_id,
                }
            )

        df = pd.DataFrame(rows)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        logger.info("Fetched %d observations for %s", len(df), series_id)
        return df

    def fetch_all_tracked(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Fetch all tracked series and return a dict keyed by series ID.

        Requests are made sequentially with a small delay to respect FRED API
        rate limits (120 requests/minute for free keys).
        """
        results: dict[str, pd.DataFrame] = {}
        for series_id in self.TRACKED_SERIES:
            try:
                df = self.fetch_series(series_id, start_date, end_date)
                results[series_id] = df
            except Exception:
                logger.exception("Failed to fetch series %s", series_id)
            time.sleep(0.6)  # ~100 req/min to stay under rate limit
        logger.info("Fetched %d / %d tracked series", len(results), len(self.TRACKED_SERIES))
        return results

    # ------------------------------------------------------------------
    # Anomaly detection
    # ------------------------------------------------------------------

    def detect_anomalies(
        self,
        series_id: str,
        z_threshold: float = 2.5,
        window: int = 60,
        start_date: str | None = None,
    ) -> pd.DataFrame:
        """Detect anomalies in a FRED series via rolling z-score.

        Parameters
        ----------
        series_id : str
            FRED series ID.
        z_threshold : float
            Absolute z-score above which an observation is flagged.
        window : int
            Rolling window size (number of observations) for mean/std.
        start_date : str, optional
            Override the look-back start date (default 5 years).

        Returns
        -------
        pd.DataFrame
            Rows flagged as anomalies with columns:
            ``date``, ``value``, ``series_id``, ``rolling_mean``,
            ``rolling_std``, ``z_score``.
        """
        df = self.fetch_series(series_id, start_date=start_date)
        if df.empty or len(df) < window:
            logger.warning("Insufficient data for anomaly detection on %s (%d rows)", series_id, len(df))
            return pd.DataFrame()

        df = df.sort_values("date").reset_index(drop=True)
        df["rolling_mean"] = df["value"].rolling(window=window, min_periods=window).mean()
        df["rolling_std"] = df["value"].rolling(window=window, min_periods=window).std()
        df["z_score"] = (df["value"] - df["rolling_mean"]) / df["rolling_std"].replace(0, float("nan"))

        anomalies = df[df["z_score"].abs() >= z_threshold].copy()
        logger.info(
            "Detected %d anomalies (|z| >= %.1f) in %s over %d observations",
            len(anomalies),
            z_threshold,
            series_id,
            len(df),
        )
        return anomalies

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def publish_time_series(self, series_id: str, start_date: str | None = None) -> int:
        """Publish raw time-series observations to Kafka.

        Topic: ``mda.economic.fred.series``

        Returns the number of messages published.
        """
        df = self.fetch_series(series_id, start_date=start_date)
        if df.empty:
            return 0

        producer = self._get_producer()
        count = 0
        for _, row in df.iterrows():
            msg = {
                "series_id": series_id,
                "series_label": self.TRACKED_SERIES.get(series_id, ""),
                "date": row["date"].isoformat(),
                "value": row["value"],
                "source": "fred",
                "ingest_time": datetime.utcnow().isoformat(),
            }
            producer.send("mda.economic.fred.series", msg)
            count += 1

        producer.flush()
        logger.info("Published %d observations for %s to mda.economic.fred.series", count, series_id)
        return count

    def publish_anomalies(self, z_threshold: float = 2.5) -> int:
        """Detect anomalies across all tracked series and publish alerts.

        Topic: ``mda.economic.anomalies``

        Returns the total number of anomaly messages published.
        """
        producer = self._get_producer()
        total = 0

        for series_id, label in self.TRACKED_SERIES.items():
            try:
                anomalies = self.detect_anomalies(series_id, z_threshold=z_threshold)
                if anomalies.empty:
                    continue

                # Publish only the most recent anomaly per series to avoid flooding
                latest = anomalies.iloc[-1]
                msg = {
                    "series_id": series_id,
                    "series_label": label,
                    "date": latest["date"].isoformat(),
                    "value": float(latest["value"]),
                    "rolling_mean": float(latest["rolling_mean"]),
                    "rolling_std": float(latest["rolling_std"]),
                    "z_score": float(latest["z_score"]),
                    "anomaly_direction": "above" if latest["z_score"] > 0 else "below",
                    "z_threshold": z_threshold,
                    "source": "fred",
                    "ingest_time": datetime.utcnow().isoformat(),
                }
                producer.send("mda.economic.anomalies", msg)
                total += 1
            except Exception:
                logger.exception("Anomaly detection failed for %s", series_id)
            time.sleep(0.6)

        producer.flush()
        logger.info("Published %d anomaly alerts to mda.economic.anomalies", total)
        return total

    # ------------------------------------------------------------------
    # Polling loop
    # ------------------------------------------------------------------

    def run_polling_loop(self, interval_hours: float = 4) -> None:
        """Continuously fetch series and publish anomalies.

        Cycle:
        1. Publish raw observations for every tracked series.
        2. Run anomaly detection and publish alerts.
        3. Sleep for *interval_hours*.
        """
        logger.info(
            "Starting FRED polling loop — %d series, interval %.1f h",
            len(self.TRACKED_SERIES),
            interval_hours,
        )

        while True:
            cycle_start = time.time()
            try:
                for series_id in self.TRACKED_SERIES:
                    try:
                        self.publish_time_series(series_id)
                    except Exception:
                        logger.exception("Failed to publish series %s", series_id)
                    time.sleep(0.6)

                self.publish_anomalies()
            except Exception:
                logger.exception("Error in FRED polling cycle")
            finally:
                self._close_producer()

            elapsed = time.time() - cycle_start
            sleep_secs = max(0, interval_hours * 3600 - elapsed)
            logger.info("FRED cycle finished in %.1f s — sleeping %.0f s", elapsed, sleep_secs)
            time.sleep(sleep_secs)


# ------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    ingester = FREDIngester()
    ingester.run_polling_loop()
