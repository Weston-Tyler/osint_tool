"""Granger causality testing for MDA event time series.

Builds daily event-frequency time series from the PostgreSQL
``causal_events`` table, tests stationarity via ADF, differences as
needed, runs ``statsmodels.tsa.stattools.grangercausalitytests`` for
all lags up to ``max_lag``, identifies optimal lag, tests both
directions for bidirectionality, and stores significant pairs as
``GRANGER_CAUSES`` edges in Memgraph.

Includes ``run_mda_standard_battery()`` — 14 standard event-type
pairs across 9 regions drawn from the MDA ontology.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from gqlalchemy import Memgraph
from statsmodels.tsa.stattools import adfuller, grangercausalitytests

logger = logging.getLogger("mda.causal.granger")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PG_DSN = os.getenv(
    "PG_DSN",
    "host=localhost port=5432 dbname=mda user=mda password=mda",
)
MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))

DEFAULT_MAX_LAG = int(os.getenv("GRANGER_MAX_LAG", "14"))
DEFAULT_SIGNIFICANCE = float(os.getenv("GRANGER_SIGNIFICANCE", "0.05"))
MIN_OBSERVATIONS = int(os.getenv("GRANGER_MIN_OBS", "60"))
MAX_DIFFERENCING = int(os.getenv("GRANGER_MAX_DIFF", "2"))

# ---------------------------------------------------------------------------
# Standard MDA event-pair battery
# ---------------------------------------------------------------------------

STANDARD_MDA_PAIRS: list[tuple[str, str]] = [
    ("COMMODITY_PRICE_SPIKE", "SOCIAL_UNREST"),
    ("CARTEL_TERRITORIAL_EXPANSION", "AIS_GAP_EVENT"),
    ("SOCIAL_MEDIA_ACTIVITY_SPIKE", "ARMED_CLASH"),
    ("SANCTIONS_DESIGNATION", "VESSEL_FLAG_CHANGE"),
    ("NATURAL_DISASTER", "CARTEL_TERRITORIAL_EXPANSION"),
    ("GDELT_VERBAL_CONFLICT", "ACLED_BATTLE_EVENT"),
    ("DROUGHT_EVENT", "FOOD_PRICE_SPIKE"),
    ("FOOD_PRICE_SPIKE", "SOCIAL_UNREST"),
    ("ELECTION_EVENT", "POLITICAL_INSTABILITY"),
    ("POLITICAL_INSTABILITY", "ARMED_CLASH"),
    ("ARMS_SHIPMENT_DETECTED", "ARMED_CLASH"),
    ("SANCTIONS_DESIGNATION", "DARK_VESSEL_ACTIVITY"),
    ("OIL_PRICE_SPIKE", "SMUGGLING_ROUTE_SHIFT"),
    ("CARTEL_LEADERSHIP_CHANGE", "CARTEL_TERRITORIAL_EXPANSION"),
]

STANDARD_REGIONS: list[str] = [
    "Caribbean",
    "Eastern Pacific",
    "Gulf of Mexico",
    "Central America",
    "Northern South America",
    "West Africa",
    "Sahel",
    "Horn of Africa",
    "Southeast Asia",
]

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class GrangerResult:
    """Result of a single Granger causality test."""
    result_id: str = field(default_factory=lambda: str(uuid4()))
    cause_event_type: str = ""
    effect_event_type: str = ""
    region: Optional[str] = None
    optimal_lag: int = 0
    p_value: float = 1.0
    f_statistic: float = 0.0
    test_statistic_type: str = "ssr_ftest"
    significant: bool = False
    significance_level: float = DEFAULT_SIGNIFICANCE
    bidirectional: bool = False
    reverse_p_value: Optional[float] = None
    reverse_optimal_lag: Optional[int] = None
    n_observations: int = 0
    differencing_order: int = 0
    adf_cause_pvalue: float = 1.0
    adf_effect_pvalue: float = 1.0
    tested_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# ---------------------------------------------------------------------------
# Time series construction
# ---------------------------------------------------------------------------


def build_daily_frequency_series(
    conn,
    event_type: str,
    region: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.Series:
    """Query PostgreSQL causal_events and build a daily event count series.

    Returns a pandas Series indexed by date with zero-filled gaps.
    """
    query = """
        SELECT occurred_at::date AS event_date, COUNT(*) AS event_count
        FROM causal_events
        WHERE event_type = %s
    """
    params: list = [event_type]

    if region:
        query += " AND location_region = %s"
        params.append(region)
    if start_date:
        query += " AND occurred_at >= %s"
        params.append(start_date)
    if end_date:
        query += " AND occurred_at <= %s"
        params.append(end_date)

    query += " GROUP BY event_date ORDER BY event_date"

    df = pd.read_sql_query(query, conn, params=params)

    if df.empty:
        return pd.Series(dtype=float)

    df["event_date"] = pd.to_datetime(df["event_date"])
    df = df.set_index("event_date")

    # Reindex to fill gaps with zeros
    full_range = pd.date_range(
        start=df.index.min(), end=df.index.max(), freq="D",
    )
    series = df["event_count"].reindex(full_range, fill_value=0).astype(float)
    series.name = event_type

    return series


# ---------------------------------------------------------------------------
# Stationarity testing and differencing
# ---------------------------------------------------------------------------


def test_stationarity(series: pd.Series, significance: float = 0.05) -> tuple[bool, float]:
    """Run Augmented Dickey-Fuller test. Returns (is_stationary, p_value)."""
    if len(series) < 20:
        return False, 1.0
    result = adfuller(series, autolag="AIC")
    p_value = result[1]
    return p_value < significance, p_value


def make_stationary(
    series: pd.Series,
    max_diff: int = MAX_DIFFERENCING,
    significance: float = 0.05,
) -> tuple[pd.Series, int]:
    """Difference a series until stationary or max_diff reached.

    Returns (stationary_series, differencing_order).
    """
    current = series.copy()
    for d in range(max_diff + 1):
        is_stat, _ = test_stationarity(current, significance)
        if is_stat:
            return current, d
        if d < max_diff:
            current = current.diff().dropna()
    return current, max_diff


# ---------------------------------------------------------------------------
# Granger testing core
# ---------------------------------------------------------------------------


def run_granger_test(
    cause_series: pd.Series,
    effect_series: pd.Series,
    max_lag: int = DEFAULT_MAX_LAG,
    significance: float = DEFAULT_SIGNIFICANCE,
) -> Optional[dict]:
    """Run Granger causality tests for all lags up to max_lag.

    Returns a dict with optimal lag info or None if insufficient data.
    The test checks whether ``cause_series`` Granger-causes
    ``effect_series``.
    """
    # Align series on common index
    combined = pd.concat([effect_series, cause_series], axis=1).dropna()
    combined.columns = ["effect", "cause"]

    if len(combined) < MIN_OBSERVATIONS:
        logger.warning(
            "Insufficient observations (%d < %d) for Granger test",
            len(combined), MIN_OBSERVATIONS,
        )
        return None

    # Ensure max_lag does not exceed reasonable bounds
    effective_max_lag = min(max_lag, len(combined) // 3)
    if effective_max_lag < 1:
        return None

    try:
        # grangercausalitytests expects [effect, cause] as column order
        results = grangercausalitytests(
            combined[["effect", "cause"]].values,
            maxlag=effective_max_lag,
            verbose=False,
        )
    except Exception as exc:
        logger.error("Granger test failed: %s", exc)
        return None

    # Find optimal lag (lowest p-value from ssr_ftest)
    best_lag = 1
    best_pvalue = 1.0
    best_fstat = 0.0
    all_lag_results: dict[int, dict] = {}

    for lag in range(1, effective_max_lag + 1):
        lag_result = results[lag]
        tests = lag_result[0]  # dict of test results

        # ssr_ftest returns (F-statistic, p-value, df_denom, df_num)
        f_stat, p_val, _, _ = tests["ssr_ftest"]
        all_lag_results[lag] = {"f_stat": f_stat, "p_value": p_val}

        if p_val < best_pvalue:
            best_pvalue = p_val
            best_lag = lag
            best_fstat = f_stat

    return {
        "optimal_lag": best_lag,
        "p_value": best_pvalue,
        "f_statistic": best_fstat,
        "significant": best_pvalue < significance,
        "n_observations": len(combined),
        "effective_max_lag": effective_max_lag,
        "all_lag_results": all_lag_results,
    }


# ---------------------------------------------------------------------------
# Full bidirectional test
# ---------------------------------------------------------------------------


def test_pair(
    conn,
    cause_type: str,
    effect_type: str,
    region: Optional[str] = None,
    max_lag: int = DEFAULT_MAX_LAG,
    significance: float = DEFAULT_SIGNIFICANCE,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Optional[GrangerResult]:
    """Run a full bidirectional Granger test for an event-type pair."""
    cause_series = build_daily_frequency_series(
        conn, cause_type, region=region,
        start_date=start_date, end_date=end_date,
    )
    effect_series = build_daily_frequency_series(
        conn, effect_type, region=region,
        start_date=start_date, end_date=end_date,
    )

    if cause_series.empty or effect_series.empty:
        logger.warning(
            "Empty series for %s -> %s (region=%s)", cause_type, effect_type, region,
        )
        return None

    # Test stationarity and difference if needed
    adf_cause_stat, adf_cause_p = test_stationarity(cause_series)
    adf_effect_stat, adf_effect_p = test_stationarity(effect_series)

    cause_stat, cause_diff_order = make_stationary(cause_series)
    effect_stat, effect_diff_order = make_stationary(effect_series)
    diff_order = max(cause_diff_order, effect_diff_order)

    # If different differencing orders, re-difference both to the max
    if cause_diff_order != effect_diff_order:
        for _ in range(diff_order - cause_diff_order):
            cause_stat = cause_stat.diff().dropna()
        for _ in range(diff_order - effect_diff_order):
            effect_stat = effect_stat.diff().dropna()

    # Forward direction: cause -> effect
    forward = run_granger_test(cause_stat, effect_stat, max_lag, significance)
    if forward is None:
        return None

    result = GrangerResult(
        cause_event_type=cause_type,
        effect_event_type=effect_type,
        region=region,
        optimal_lag=forward["optimal_lag"],
        p_value=forward["p_value"],
        f_statistic=forward["f_statistic"],
        significant=forward["significant"],
        significance_level=significance,
        n_observations=forward["n_observations"],
        differencing_order=diff_order,
        adf_cause_pvalue=adf_cause_p,
        adf_effect_pvalue=adf_effect_p,
    )

    # Reverse direction: effect -> cause
    reverse = run_granger_test(effect_stat, cause_stat, max_lag, significance)
    if reverse is not None:
        result.reverse_p_value = reverse["p_value"]
        result.reverse_optimal_lag = reverse["optimal_lag"]
        result.bidirectional = (
            forward["significant"] and reverse["p_value"] < significance
        )

    return result


# ---------------------------------------------------------------------------
# Memgraph persistence
# ---------------------------------------------------------------------------


def store_granger_edge(mg: Memgraph, result: GrangerResult) -> None:
    """Create or update a GRANGER_CAUSES edge in Memgraph."""
    if not result.significant:
        return

    try:
        mg.execute(
            """
            MERGE (c:CausalEventType {event_type: $cause_type})
            MERGE (e:CausalEventType {event_type: $effect_type})
            MERGE (c)-[g:GRANGER_CAUSES]->(e)
            SET g.result_id = $result_id,
                g.optimal_lag_days = $lag,
                g.p_value = $pval,
                g.f_statistic = $fstat,
                g.significance_level = $sig,
                g.bidirectional = $bidir,
                g.reverse_p_value = $rev_pval,
                g.region = $region,
                g.n_observations = $nobs,
                g.differencing_order = $diff_order,
                g.tested_at = localDateTime(),
                g.method = 'granger_causality_v1'
            """,
            {
                "cause_type": result.cause_event_type,
                "effect_type": result.effect_event_type,
                "result_id": result.result_id,
                "lag": result.optimal_lag,
                "pval": result.p_value,
                "fstat": result.f_statistic,
                "sig": result.significance_level,
                "bidir": result.bidirectional,
                "rev_pval": result.reverse_p_value,
                "region": result.region,
                "nobs": result.n_observations,
                "diff_order": result.differencing_order,
            },
        )
        logger.info(
            "Stored GRANGER_CAUSES edge: %s -> %s (lag=%d, p=%.4f, region=%s)",
            result.cause_event_type,
            result.effect_event_type,
            result.optimal_lag,
            result.p_value,
            result.region,
        )
    except Exception as exc:
        logger.error("Failed to store Granger edge: %s", exc)


# ---------------------------------------------------------------------------
# Standard battery
# ---------------------------------------------------------------------------


def run_mda_standard_battery(
    max_lag: int = DEFAULT_MAX_LAG,
    significance: float = DEFAULT_SIGNIFICANCE,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> list[GrangerResult]:
    """Run the standard MDA Granger test battery.

    Tests 14 standard event-type pairs across 9 regions plus a
    global (region=None) pass.
    """
    conn = psycopg2.connect(PG_DSN)
    mg = Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)

    all_results: list[GrangerResult] = []
    total_tests = len(STANDARD_MDA_PAIRS) * (len(STANDARD_REGIONS) + 1)
    completed = 0

    for cause_type, effect_type in STANDARD_MDA_PAIRS:
        # Global test (no region filter)
        regions_to_test: list[Optional[str]] = [None] + list(STANDARD_REGIONS)

        for region in regions_to_test:
            completed += 1
            logger.info(
                "[%d/%d] Testing %s -> %s (region=%s)",
                completed, total_tests, cause_type, effect_type, region or "GLOBAL",
            )

            try:
                result = test_pair(
                    conn,
                    cause_type,
                    effect_type,
                    region=region,
                    max_lag=max_lag,
                    significance=significance,
                    start_date=start_date,
                    end_date=end_date,
                )

                if result is None:
                    continue

                all_results.append(result)

                if result.significant:
                    store_granger_edge(mg, result)
                    logger.info(
                        "SIGNIFICANT: %s -> %s (region=%s, lag=%d, p=%.4f, bidir=%s)",
                        cause_type, effect_type, region or "GLOBAL",
                        result.optimal_lag, result.p_value, result.bidirectional,
                    )

            except Exception as exc:
                logger.error(
                    "Test failed for %s -> %s (region=%s): %s",
                    cause_type, effect_type, region, exc,
                )

    conn.close()

    # Summary
    significant_count = sum(1 for r in all_results if r.significant)
    bidirectional_count = sum(1 for r in all_results if r.bidirectional)
    logger.info(
        "Battery complete: %d tests, %d significant, %d bidirectional",
        len(all_results), significant_count, bidirectional_count,
    )

    return all_results


# ---------------------------------------------------------------------------
# Ad-hoc single-pair test utility
# ---------------------------------------------------------------------------


def test_single_pair(
    cause_type: str,
    effect_type: str,
    region: Optional[str] = None,
    max_lag: int = DEFAULT_MAX_LAG,
    significance: float = DEFAULT_SIGNIFICANCE,
    store: bool = True,
) -> Optional[GrangerResult]:
    """Convenience function to test a single pair and optionally store."""
    conn = psycopg2.connect(PG_DSN)
    result = test_pair(
        conn, cause_type, effect_type,
        region=region, max_lag=max_lag, significance=significance,
    )
    conn.close()

    if result and result.significant and store:
        mg = Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
        store_granger_edge(mg, result)

    return result


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    results = run_mda_standard_battery()
    print(f"\nCompleted: {len(results)} tests")
    for r in results:
        if r.significant:
            print(
                f"  * {r.cause_event_type} -> {r.effect_event_type} "
                f"(region={r.region or 'GLOBAL'}, lag={r.optimal_lag}d, "
                f"p={r.p_value:.4f}, bidir={r.bidirectional})"
            )
