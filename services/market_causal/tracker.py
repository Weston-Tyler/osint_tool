"""Market causal tracker — fetches financial instrument prices, detects
anomalies via rolling z-scores, and runs pairwise Granger causality tests
to discover lead/lag relationships across commodity, shipping, currency,
crypto, and equity instruments relevant to MDA threat domains.
"""

import hashlib
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yfinance as yf
from gqlalchemy import Memgraph
from statsmodels.tsa.stattools import adfuller, grangercausalitytests

logger = logging.getLogger("mda.market_causal.tracker")

MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))

# ── Tracked instruments ─────────────────────────────────────

TRACKED_INSTRUMENTS: Dict[str, str] = {
    # Energy
    "CL=F": "WTI Crude Oil Futures",
    "BZ=F": "Brent Crude Oil Futures",
    "NG=F": "Natural Gas Futures",
    # Agriculture
    "ZW=F": "CBOT Wheat Futures",
    "ZC=F": "CBOT Corn Futures",
    "ZS=F": "CBOT Soybean Futures",
    # Shipping
    "DSX": "Diana Shipping (dry bulk proxy)",
    "BDRY": "Breakwave Dry Bulk Shipping ETF",
    # LatAm currencies
    "MXN=X": "USD/MXN",
    "COP=X": "USD/COP",
    "VES=X": "USD/VES (Venezuelan bolívar)",
    "PEN=X": "USD/PEN",
    # Crypto
    "BTC-USD": "Bitcoin",
    "XMR-USD": "Monero (privacy coin)",
    # Equity ETFs
    "EWW": "iShares MSCI Mexico ETF",
    "GXG": "Global X MSCI Colombia ETF",
}

# ── Data classes ────────────────────────────────────────────


@dataclass
class PriceAnomaly:
    """A detected z-score anomaly for a single instrument on a single date."""

    ticker: str
    instrument_name: str
    date: datetime
    close_price: float
    rolling_mean: float
    rolling_std: float
    z_score: float
    direction: str  # "spike" or "drop"

    @property
    def severity(self) -> str:
        abs_z = abs(self.z_score)
        if abs_z >= 4.0:
            return "CRITICAL"
        if abs_z >= 3.0:
            return "HIGH"
        return "MODERATE"

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "instrument_name": self.instrument_name,
            "date": self.date.isoformat(),
            "close_price": self.close_price,
            "rolling_mean": round(self.rolling_mean, 4),
            "rolling_std": round(self.rolling_std, 4),
            "z_score": round(self.z_score, 4),
            "direction": self.direction,
            "severity": self.severity,
        }


@dataclass
class GrangerResult:
    """Result of a pairwise Granger-causality test between two instruments."""

    cause_ticker: str
    effect_ticker: str
    optimal_lag: int
    f_statistic: float
    p_value: float
    is_significant: bool
    adf_cause_stationary: bool
    adf_effect_stationary: bool
    test_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def confidence(self) -> float:
        """Map p-value to a 0-1 confidence score (lower p = higher confidence)."""
        return round(max(0.0, 1.0 - self.p_value), 4)

    def to_dict(self) -> dict:
        return {
            "cause_ticker": self.cause_ticker,
            "effect_ticker": self.effect_ticker,
            "optimal_lag": self.optimal_lag,
            "f_statistic": round(self.f_statistic, 4),
            "p_value": round(self.p_value, 6),
            "is_significant": self.is_significant,
            "confidence": self.confidence,
            "adf_cause_stationary": self.adf_cause_stationary,
            "adf_effect_stationary": self.adf_effect_stationary,
            "test_timestamp": self.test_timestamp.isoformat(),
        }


# ── MarketCausalTracker ────────────────────────────────────


class MarketCausalTracker:
    """Fetches price series, detects anomalies, and runs Granger tests for
    the 16 tracked instruments, persisting causal edges to Memgraph."""

    def __init__(
        self,
        mg: Optional[Memgraph] = None,
        lookback_days: int = 180,
        z_window: int = 30,
        z_threshold: float = 2.5,
        granger_max_lag: int = 10,
        granger_p_threshold: float = 0.05,
    ):
        self.mg = mg or Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
        self.lookback_days = lookback_days
        self.z_window = z_window
        self.z_threshold = z_threshold
        self.granger_max_lag = granger_max_lag
        self.granger_p_threshold = granger_p_threshold

    # ── Price retrieval ──────────────────────────────────────

    def fetch_price_series(
        self,
        ticker: str,
        lookback_days: Optional[int] = None,
    ) -> pd.Series:
        """Download daily closing prices from Yahoo Finance via yfinance.

        Returns a pandas Series indexed by date with the adjusted close.
        """
        days = lookback_days or self.lookback_days
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)

        logger.info("Fetching %s from %s to %s", ticker, start.date(), end.date())
        data = yf.download(
            ticker,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,
        )

        if data.empty:
            logger.warning("No data returned for %s", ticker)
            return pd.Series(dtype=float, name=ticker)

        series = data["Close"].squeeze()
        series.name = ticker
        series = series.dropna()
        logger.info("Fetched %d rows for %s", len(series), ticker)
        return series

    # ── Anomaly detection ────────────────────────────────────

    def detect_price_anomaly(
        self,
        ticker: str,
        series: Optional[pd.Series] = None,
    ) -> List[PriceAnomaly]:
        """Detect anomalies using a rolling z-score with a 30-day window
        and a threshold of 2.5 standard deviations."""
        if series is None:
            series = self.fetch_price_series(ticker)

        if len(series) < self.z_window + 1:
            logger.warning(
                "Insufficient data for %s (%d rows, need %d)",
                ticker,
                len(series),
                self.z_window + 1,
            )
            return []

        rolling_mean = series.rolling(window=self.z_window).mean()
        rolling_std = series.rolling(window=self.z_window).std()

        # Avoid division by zero
        rolling_std = rolling_std.replace(0, np.nan)

        z_scores = (series - rolling_mean) / rolling_std
        z_scores = z_scores.dropna()

        anomalies: List[PriceAnomaly] = []
        instrument_name = TRACKED_INSTRUMENTS.get(ticker, ticker)

        for date, z in z_scores.items():
            if abs(z) >= self.z_threshold:
                anomalies.append(
                    PriceAnomaly(
                        ticker=ticker,
                        instrument_name=instrument_name,
                        date=pd.Timestamp(date).to_pydatetime(),
                        close_price=float(series.loc[date]),
                        rolling_mean=float(rolling_mean.loc[date]),
                        rolling_std=float(rolling_std.loc[date]),
                        z_score=float(z),
                        direction="spike" if z > 0 else "drop",
                    )
                )

        logger.info("Detected %d anomalies for %s", len(anomalies), ticker)
        return anomalies

    # ── Granger causality ────────────────────────────────────

    @staticmethod
    def _is_stationary(series: pd.Series, significance: float = 0.05) -> bool:
        """ADF stationarity test.  Returns True if the null hypothesis of a
        unit root is rejected at the given significance level."""
        if len(series) < 20:
            return False
        result = adfuller(series.dropna(), autolag="AIC")
        p_value = result[1]
        return p_value < significance

    @staticmethod
    def _make_stationary(series: pd.Series) -> pd.Series:
        """First-difference the series to achieve stationarity."""
        return series.diff().dropna()

    def run_granger_test(
        self,
        cause_ticker: str,
        effect_ticker: str,
        cause_series: Optional[pd.Series] = None,
        effect_series: Optional[pd.Series] = None,
    ) -> Optional[GrangerResult]:
        """Run a Granger causality test between two price series.

        1. Fetch price data if not provided.
        2. Check ADF stationarity; difference if non-stationary.
        3. Align the two series on common dates.
        4. Run statsmodels grangercausalitytests up to ``granger_max_lag``.
        5. Return the best (lowest-p) lag result.
        """
        if cause_series is None:
            cause_series = self.fetch_price_series(cause_ticker)
        if effect_series is None:
            effect_series = self.fetch_price_series(effect_ticker)

        if cause_series.empty or effect_series.empty:
            logger.warning(
                "Empty series for %s or %s — skipping Granger test",
                cause_ticker,
                effect_ticker,
            )
            return None

        # Stationarity checks
        adf_cause = self._is_stationary(cause_series)
        adf_effect = self._is_stationary(effect_series)

        cause_stat = cause_series if adf_cause else self._make_stationary(cause_series)
        effect_stat = effect_series if adf_effect else self._make_stationary(effect_series)

        # Align on common dates
        combined = pd.concat([effect_stat, cause_stat], axis=1, join="inner").dropna()
        combined.columns = ["effect", "cause"]

        min_obs = self.granger_max_lag + 2
        if len(combined) < min_obs:
            logger.warning(
                "Insufficient aligned observations for %s -> %s (%d rows, need %d)",
                cause_ticker,
                effect_ticker,
                len(combined),
                min_obs,
            )
            return None

        try:
            results = grangercausalitytests(
                combined[["effect", "cause"]],
                maxlag=self.granger_max_lag,
                verbose=False,
            )
        except Exception as exc:
            logger.error(
                "Granger test failed for %s -> %s: %s",
                cause_ticker,
                effect_ticker,
                exc,
            )
            return None

        # Find best lag by lowest p-value (using ssr_ftest)
        best_lag = 1
        best_p = 1.0
        best_f = 0.0

        for lag, res in results.items():
            test_results = res[0]
            f_stat = test_results["ssr_ftest"][0]
            p_val = test_results["ssr_ftest"][1]
            if p_val < best_p:
                best_p = p_val
                best_f = f_stat
                best_lag = lag

        is_significant = best_p < self.granger_p_threshold

        granger = GrangerResult(
            cause_ticker=cause_ticker,
            effect_ticker=effect_ticker,
            optimal_lag=best_lag,
            f_statistic=best_f,
            p_value=best_p,
            is_significant=is_significant,
            adf_cause_stationary=adf_cause,
            adf_effect_stationary=adf_effect,
        )

        logger.info(
            "Granger %s -> %s: lag=%d F=%.3f p=%.6f sig=%s",
            cause_ticker,
            effect_ticker,
            best_lag,
            best_f,
            best_p,
            is_significant,
        )
        return granger

    # ── Batch causality ──────────────────────────────────────

    def run_batch_market_causality(
        self,
        tickers: Optional[List[str]] = None,
    ) -> List[GrangerResult]:
        """Run pairwise Granger tests across all tracked instruments and
        store significant edges in Memgraph."""
        tickers = tickers or list(TRACKED_INSTRUMENTS.keys())
        logger.info("Starting batch Granger analysis for %d instruments", len(tickers))

        # Pre-fetch all price series
        series_cache: Dict[str, pd.Series] = {}
        for t in tickers:
            try:
                series_cache[t] = self.fetch_price_series(t)
            except Exception as exc:
                logger.error("Failed to fetch %s: %s", t, exc)

        significant_results: List[GrangerResult] = []

        for i, cause in enumerate(tickers):
            if cause not in series_cache:
                continue
            for effect in tickers[i + 1 :]:
                if effect not in series_cache:
                    continue

                # Test both directions
                for c_ticker, e_ticker in [(cause, effect), (effect, cause)]:
                    result = self.run_granger_test(
                        cause_ticker=c_ticker,
                        effect_ticker=e_ticker,
                        cause_series=series_cache[c_ticker],
                        effect_series=series_cache[e_ticker],
                    )
                    if result and result.is_significant:
                        significant_results.append(result)
                        try:
                            self.store_market_causal_edge(result)
                        except Exception as exc:
                            logger.error(
                                "Failed to store edge %s -> %s: %s",
                                c_ticker,
                                e_ticker,
                                exc,
                            )

        logger.info(
            "Batch analysis complete: %d significant Granger edges found",
            len(significant_results),
        )
        return significant_results

    # ── Memgraph persistence ─────────────────────────────────

    def store_market_causal_edge(self, result: GrangerResult) -> None:
        """Upsert a GRANGER_CAUSES relationship between two MarketInstrument
        nodes in Memgraph."""
        cause_name = TRACKED_INSTRUMENTS.get(result.cause_ticker, result.cause_ticker)
        effect_name = TRACKED_INSTRUMENTS.get(result.effect_ticker, result.effect_ticker)

        # Deterministic edge ID
        edge_id = hashlib.sha256(
            f"granger:{result.cause_ticker}:{result.effect_ticker}".encode()
        ).hexdigest()[:16]

        query = """
            MERGE (cause:MarketInstrument {ticker: $cause_ticker})
            ON CREATE SET cause.name = $cause_name,
                          cause.instrument_type = $cause_type
            MERGE (effect:MarketInstrument {ticker: $effect_ticker})
            ON CREATE SET effect.name = $effect_name,
                          effect.instrument_type = $effect_type
            MERGE (cause)-[r:GRANGER_CAUSES {edge_id: $edge_id}]->(effect)
            SET r.optimal_lag       = $optimal_lag,
                r.f_statistic       = $f_statistic,
                r.p_value           = $p_value,
                r.confidence        = $confidence,
                r.adf_cause_ok      = $adf_cause,
                r.adf_effect_ok     = $adf_effect,
                r.updated_at        = $updated_at
        """

        params = {
            "cause_ticker": result.cause_ticker,
            "cause_name": cause_name,
            "cause_type": _instrument_type(result.cause_ticker),
            "effect_ticker": result.effect_ticker,
            "effect_name": effect_name,
            "effect_type": _instrument_type(result.effect_ticker),
            "edge_id": edge_id,
            "optimal_lag": result.optimal_lag,
            "f_statistic": result.f_statistic,
            "p_value": result.p_value,
            "confidence": result.confidence,
            "adf_cause": result.adf_cause_stationary,
            "adf_effect": result.adf_effect_stationary,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        self.mg.execute_and_fetch(query, params)
        logger.info(
            "Stored GRANGER_CAUSES: %s -> %s (conf=%.3f)",
            result.cause_ticker,
            result.effect_ticker,
            result.confidence,
        )


# ── Helpers ─────────────────────────────────────────────────


def _instrument_type(ticker: str) -> str:
    """Infer instrument type from ticker suffix / name."""
    if ticker.endswith("=F"):
        return "futures"
    if ticker.endswith("=X"):
        return "currency"
    if ticker.endswith("-USD"):
        return "crypto"
    if ticker in ("DSX", "BDRY"):
        return "shipping_equity"
    if ticker in ("EWW", "GXG"):
        return "country_etf"
    return "equity"
