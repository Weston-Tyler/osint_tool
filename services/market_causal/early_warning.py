"""Market early-warning system — computes causal pressure scores for each
tracked instrument by aggregating upstream Granger-causal edges weighted
by event severity, then emits alerts when thresholds are exceeded.

Pressure formula:
    P_m(t) = Σ (w_i * c_i * s_i) / max(1, ℓ_i)

where for each upstream GRANGER_CAUSES edge *i* into instrument *m*:
    w_i = edge confidence (0-1)
    c_i = causal weight from the upstream CausalEvent (default 1.0)
    s_i = severity score of the upstream event type
    ℓ_i = optimal lag in days (dampens slow-propagating signals)
"""

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from gqlalchemy import Memgraph

from .tracker import TRACKED_INSTRUMENTS

logger = logging.getLogger("mda.market_causal.early_warning")

MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))

# ── Severity map for upstream event types ───────────────────

SEVERITY_MAP: Dict[str, float] = {
    "SANCTIONS_DESIGNATION": 9.0,
    "ARMED_CONFLICT": 8.5,
    "CARTEL_INTERDICTION": 7.5,
    "PORT_CLOSURE": 7.0,
    "CANAL_DISRUPTION": 8.0,
    "CROP_FAILURE": 6.5,
    "CURRENCY_CRISIS": 7.0,
    "PIPELINE_SABOTAGE": 8.0,
    "MARITIME_SEIZURE": 6.0,
    "CYBER_ATTACK": 7.5,
}

# ── Per-ticker alert thresholds ─────────────────────────────
# Tuned empirically; higher-volatility instruments get wider bands.

ALERT_THRESHOLDS: Dict[str, float] = {
    "CL=F": 12.0,
    "BZ=F": 12.0,
    "NG=F": 14.0,
    "ZW=F": 10.0,
    "ZC=F": 10.0,
    "ZS=F": 10.0,
    "DSX": 8.0,
    "BDRY": 8.0,
    "MXN=X": 9.0,
    "COP=X": 9.0,
    "VES=X": 15.0,
    "PEN=X": 9.0,
    "BTC-USD": 18.0,
    "XMR-USD": 18.0,
    "EWW": 10.0,
    "GXG": 10.0,
}

DEFAULT_THRESHOLD = 10.0

# ── Data classes ────────────────────────────────────────────


@dataclass
class PressureComponent:
    """One upstream contributor to an instrument's pressure score."""

    upstream_ticker: str
    edge_confidence: float
    causal_weight: float
    severity: float
    lag: int
    component_score: float


@dataclass
class PressureScore:
    """Aggregate causal pressure score for a single instrument."""

    ticker: str
    instrument_name: str
    pressure: float
    threshold: float
    is_alert: bool
    components: List[PressureComponent] = field(default_factory=list)
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def alert_level(self) -> str:
        ratio = self.pressure / max(self.threshold, 0.01)
        if ratio >= 2.0:
            return "CRITICAL"
        if ratio >= 1.5:
            return "HIGH"
        if ratio >= 1.0:
            return "ELEVATED"
        if ratio >= 0.7:
            return "WATCH"
        return "NORMAL"

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "instrument_name": self.instrument_name,
            "pressure": round(self.pressure, 4),
            "threshold": self.threshold,
            "is_alert": self.is_alert,
            "alert_level": self.alert_level,
            "num_components": len(self.components),
            "computed_at": self.computed_at.isoformat(),
        }


@dataclass
class AlertBulletin:
    """Formatted bulletin with all alert-level instruments."""

    generated_at: datetime
    alerts: List[PressureScore]
    total_instruments: int
    text: str


# ── MarketEarlyWarningSystem ───────────────────────────────


class MarketEarlyWarningSystem:
    """Computes causal pressure for each tracked instrument and emits
    alerts when the score exceeds the per-ticker threshold."""

    def __init__(self, mg: Optional[Memgraph] = None):
        self.mg = mg or Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)

    # ── Core pressure computation ────────────────────────────

    def compute_pressure(self, ticker: str) -> PressureScore:
        """Compute P_m(t) for a single instrument by querying its upstream
        GRANGER_CAUSES edges from Memgraph.

        P_m(t) = Σ (w_i * c_i * s_i) / max(1, ℓ_i)
        """
        query = """
            MATCH (upstream:MarketInstrument)-[r:GRANGER_CAUSES]->(target:MarketInstrument {ticker: $ticker})
            OPTIONAL MATCH (upstream)<-[:AFFECTS]-(evt:CausalEvent)
            WHERE evt.status IS NULL OR evt.status <> 'RESOLVED'
            RETURN upstream.ticker       AS upstream_ticker,
                   r.confidence          AS edge_confidence,
                   r.optimal_lag         AS optimal_lag,
                   coalesce(evt.causal_weight, 1.0)  AS causal_weight,
                   coalesce(evt.event_type, 'UNKNOWN') AS event_type
        """
        rows = list(self.mg.execute_and_fetch(query, {"ticker": ticker}))

        components: List[PressureComponent] = []
        total_pressure = 0.0

        for row in rows:
            w_i = float(row["edge_confidence"] or 0.0)
            c_i = float(row["causal_weight"] or 1.0)
            event_type = row["event_type"]
            s_i = SEVERITY_MAP.get(event_type, 3.0)
            lag = max(1, int(row["optimal_lag"] or 1))

            component_score = (w_i * c_i * s_i) / lag
            total_pressure += component_score

            components.append(
                PressureComponent(
                    upstream_ticker=row["upstream_ticker"],
                    edge_confidence=w_i,
                    causal_weight=c_i,
                    severity=s_i,
                    lag=lag,
                    component_score=component_score,
                )
            )

        threshold = ALERT_THRESHOLDS.get(ticker, DEFAULT_THRESHOLD)
        is_alert = total_pressure >= threshold
        instrument_name = TRACKED_INSTRUMENTS.get(ticker, ticker)

        score = PressureScore(
            ticker=ticker,
            instrument_name=instrument_name,
            pressure=total_pressure,
            threshold=threshold,
            is_alert=is_alert,
            components=components,
        )

        if is_alert:
            logger.warning(
                "ALERT [%s] %s: pressure=%.2f threshold=%.2f level=%s",
                ticker,
                instrument_name,
                total_pressure,
                threshold,
                score.alert_level,
            )
        else:
            logger.debug(
                "%s pressure=%.2f (threshold=%.2f) — %s",
                ticker,
                total_pressure,
                threshold,
                score.alert_level,
            )

        return score

    # ── Async batch processing ───────────────────────────────

    async def _compute_pressure_async(self, ticker: str) -> PressureScore:
        """Wrap synchronous compute_pressure for asyncio.gather."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.compute_pressure, ticker)

    async def run_all_instruments(
        self,
        tickers: Optional[List[str]] = None,
    ) -> List[PressureScore]:
        """Compute pressure for all tracked instruments concurrently."""
        tickers = tickers or list(TRACKED_INSTRUMENTS.keys())
        logger.info("Computing causal pressure for %d instruments", len(tickers))

        tasks = [self._compute_pressure_async(t) for t in tickers]
        scores = await asyncio.gather(*tasks, return_exceptions=True)

        results: List[PressureScore] = []
        for ticker, score in zip(tickers, scores):
            if isinstance(score, Exception):
                logger.error("Failed to compute pressure for %s: %s", ticker, score)
            else:
                results.append(score)

        # Sort by pressure descending
        results.sort(key=lambda s: s.pressure, reverse=True)

        alerts = [s for s in results if s.is_alert]
        logger.info(
            "Pressure scan complete: %d instruments, %d alerts",
            len(results),
            len(alerts),
        )
        return results

    # ── Alert bulletin formatting ────────────────────────────

    def generate_alert_bulletin(
        self,
        scores: List[PressureScore],
    ) -> AlertBulletin:
        """Format a human-readable alert bulletin from pressure scores."""
        now = datetime.now(timezone.utc)
        alerts = [s for s in scores if s.is_alert]

        lines: List[str] = []
        lines.append("=" * 72)
        lines.append("  MDA MARKET CAUSAL EARLY WARNING BULLETIN")
        lines.append(f"  Generated: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        lines.append(f"  Instruments scanned: {len(scores)}")
        lines.append(f"  Active alerts: {len(alerts)}")
        lines.append("=" * 72)

        if not alerts:
            lines.append("")
            lines.append("  No active alerts. All instruments within normal parameters.")
            lines.append("")
        else:
            for score in alerts:
                lines.append("")
                lines.append(
                    f"  [{score.alert_level}] {score.ticker} — {score.instrument_name}"
                )
                bar_len = min(40, int(score.pressure / score.threshold * 20))
                bar = "#" * bar_len + "." * max(0, 20 - bar_len)
                lines.append(
                    f"    Pressure: {score.pressure:7.2f} / {score.threshold:.1f}  [{bar}]"
                )
                lines.append(f"    Upstream contributors: {len(score.components)}")
                # Top 3 contributors
                top = sorted(
                    score.components, key=lambda c: c.component_score, reverse=True
                )[:3]
                for comp in top:
                    lines.append(
                        f"      - {comp.upstream_ticker}: "
                        f"w={comp.edge_confidence:.2f} s={comp.severity:.1f} "
                        f"lag={comp.lag}d => {comp.component_score:.2f}"
                    )
                lines.append("")

        lines.append("-" * 72)
        lines.append("  SUMMARY BY ALERT LEVEL")
        lines.append("-" * 72)

        for level in ("CRITICAL", "HIGH", "ELEVATED", "WATCH", "NORMAL"):
            count = sum(1 for s in scores if s.alert_level == level)
            if count > 0:
                lines.append(f"    {level:10s}: {count}")

        lines.append("")
        lines.append("=" * 72)

        text = "\n".join(lines)

        return AlertBulletin(
            generated_at=now,
            alerts=alerts,
            total_instruments=len(scores),
            text=text,
        )
