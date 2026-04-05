"""Humanitarian crisis predictor — queries the Memgraph causal graph for
active indicators per country and computes a composite crisis probability
score based on weighted famine-pattern triggers.  Produces a daily
bulletin with ASCII art progress bars and alert levels.
"""

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from gqlalchemy import Memgraph

logger = logging.getLogger("mda.humanitarian_monitor.crisis_predictor")

MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))

# ── Monitored countries ─────────────────────────────────────

MONITORED_COUNTRIES: Dict[str, str] = {
    "MEX": "Mexico",
    "COL": "Colombia",
    "VEN": "Venezuela",
    "HTI": "Haiti",
    "SDN": "Sudan",
    "SOM": "Somalia",
    "YEM": "Yemen",
    "ETH": "Ethiopia",
    "AFG": "Afghanistan",
    "MMR": "Myanmar",
    "HND": "Honduras",
    "GTM": "Guatemala",
    "SLV": "El Salvador",
}

# ── Famine-pattern triggers and weights ─────────────────────
# Each event type contributes a weighted signal toward the composite
# crisis probability.  Weights are normalized internally.

FAMINE_PATTERN_TRIGGERS: Dict[str, float] = {
    "FOOD_INSECURITY": 0.25,
    "ARMED_CONFLICT": 0.20,
    "DISPLACEMENT": 0.15,
    "ECONOMIC_CRISIS": 0.12,
    "DROUGHT": 0.10,
    "EPIDEMIC": 0.10,
    "HIGH_RISK_COUNTRY": 0.08,
}

# Total weight for normalization
_TOTAL_WEIGHT = sum(FAMINE_PATTERN_TRIGGERS.values())

# ── Data classes ────────────────────────────────────────────


@dataclass
class IndicatorSignal:
    """A single indicator signal for a country."""

    event_type: str
    weight: float
    event_count: int
    avg_confidence: float
    avg_risk_score: float
    max_severity_phase: Optional[int]
    total_affected: int
    contribution: float  # weighted contribution to the composite score


@dataclass
class CrisisProbabilityScore:
    """Composite crisis probability score for a country."""

    country_iso: str
    country_name: str
    probability: float  # 0.0 - 1.0
    indicators: List[IndicatorSignal] = field(default_factory=list)
    active_event_count: int = 0
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def alert_level(self) -> str:
        """Map probability to an alert level."""
        if self.probability >= 0.85:
            return "CRITICAL"
        if self.probability >= 0.65:
            return "HIGH"
        if self.probability >= 0.45:
            return "ELEVATED"
        if self.probability >= 0.25:
            return "WATCH"
        return "LOW"

    def to_dict(self) -> dict:
        return {
            "country_iso": self.country_iso,
            "country_name": self.country_name,
            "probability": round(self.probability, 4),
            "alert_level": self.alert_level,
            "active_event_count": self.active_event_count,
            "num_indicators": len(self.indicators),
            "computed_at": self.computed_at.isoformat(),
        }


# ── HumanitarianCrisisPredictor ────────────────────────────


class HumanitarianCrisisPredictor:
    """Computes per-country crisis probability scores by querying active
    humanitarian indicators from Memgraph and aggregating them through
    the famine-pattern trigger weights."""

    def __init__(
        self,
        mg: Optional[Memgraph] = None,
        lookback_days: int = 90,
    ):
        self.mg = mg or Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
        self.lookback_days = lookback_days

    # ── Core scoring ─────────────────────────────────────────

    def compute_country_crisis_score(
        self,
        country_iso: str,
    ) -> CrisisProbabilityScore:
        """Query active indicators for a country from Memgraph and compute
        the composite crisis probability.

        The score aggregates each famine-pattern trigger type:
            P = Σ (weight_t * signal_t) / total_weight

        where signal_t is a 0-1 activation for event type *t* based on
        the number, confidence, and risk score of matching CausalEvents.
        """
        country_name = MONITORED_COUNTRIES.get(country_iso, country_iso)
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=self.lookback_days)
        ).isoformat()

        # Query active indicators grouped by event_type
        query = """
            MATCH (e:CausalEvent)
            WHERE e.location_country_iso = $country_iso
              AND e.domain = 'humanitarian'
              AND (e.status IS NULL OR e.status <> 'RESOLVED')
              AND (e.occurred_at IS NULL OR e.occurred_at >= $cutoff)
            RETURN e.event_type          AS event_type,
                   count(e)              AS event_count,
                   avg(e.confidence)     AS avg_confidence,
                   avg(e.risk_score)     AS avg_risk_score,
                   max(e.severity_phase) AS max_severity_phase,
                   sum(coalesce(e.affected_population, 0)) AS total_affected
        """
        rows = list(self.mg.execute_and_fetch(query, {
            "country_iso": country_iso,
            "cutoff": cutoff,
        }))

        indicators: List[IndicatorSignal] = []
        composite = 0.0
        total_events = 0

        for row in rows:
            event_type = row["event_type"]
            if event_type not in FAMINE_PATTERN_TRIGGERS:
                continue

            weight = FAMINE_PATTERN_TRIGGERS[event_type]
            event_count = int(row["event_count"] or 0)
            avg_conf = float(row["avg_confidence"] or 0.5)
            avg_risk = float(row["avg_risk_score"] or 5.0)
            max_phase = row["max_severity_phase"]
            if max_phase is not None:
                max_phase = int(max_phase)
            total_affected = int(row["total_affected"] or 0)

            total_events += event_count

            # Compute signal activation (0-1) for this event type
            # Factor 1: count saturation (diminishing returns)
            count_signal = min(1.0, event_count / 10.0)
            # Factor 2: average confidence
            conf_signal = avg_conf
            # Factor 3: risk score normalized to 0-1
            risk_signal = min(1.0, avg_risk / 10.0)
            # Factor 4: severity phase boost (IPC phases)
            phase_boost = 0.0
            if max_phase is not None:
                phase_boost = min(1.0, max_phase / 5.0) * 0.3
            # Factor 5: affected population scale
            pop_signal = 0.0
            if total_affected > 0:
                import math
                pop_signal = min(1.0, math.log10(max(total_affected, 1)) / 7.0)

            signal = min(
                1.0,
                (count_signal * 0.25)
                + (conf_signal * 0.25)
                + (risk_signal * 0.25)
                + (phase_boost)
                + (pop_signal * 0.25),
            )

            contribution = weight * signal / _TOTAL_WEIGHT
            composite += contribution

            indicators.append(
                IndicatorSignal(
                    event_type=event_type,
                    weight=weight,
                    event_count=event_count,
                    avg_confidence=round(avg_conf, 3),
                    avg_risk_score=round(avg_risk, 2),
                    max_severity_phase=max_phase,
                    total_affected=total_affected,
                    contribution=round(contribution, 4),
                )
            )

        # Clamp probability to [0, 1]
        probability = max(0.0, min(1.0, composite))

        score = CrisisProbabilityScore(
            country_iso=country_iso,
            country_name=country_name,
            probability=probability,
            indicators=sorted(indicators, key=lambda i: i.contribution, reverse=True),
            active_event_count=total_events,
        )

        logger.info(
            "Crisis score for %s (%s): %.3f [%s] (%d events, %d indicators)",
            country_iso,
            country_name,
            probability,
            score.alert_level,
            total_events,
            len(indicators),
        )
        return score

    # ── Async batch processing ───────────────────────────────

    async def _compute_score_async(
        self,
        country_iso: str,
    ) -> CrisisProbabilityScore:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self.compute_country_crisis_score, country_iso
        )

    async def run_daily_bulletin(
        self,
        countries: Optional[Dict[str, str]] = None,
    ) -> str:
        """Compute crisis scores for all monitored countries and return
        a formatted bulletin string.

        Parameters
        ----------
        countries : dict, optional
            Override the default MONITORED_COUNTRIES dict.

        Returns
        -------
        str : Formatted bulletin text.
        """
        countries = countries or MONITORED_COUNTRIES
        logger.info("Running daily bulletin for %d countries", len(countries))

        tasks = [self._compute_score_async(iso) for iso in countries]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        scores: List[CrisisProbabilityScore] = []
        for iso, result in zip(countries, results):
            if isinstance(result, Exception):
                logger.error("Failed to compute score for %s: %s", iso, result)
            else:
                scores.append(result)

        # Sort by probability descending
        scores.sort(key=lambda s: s.probability, reverse=True)

        bulletin = self._format_bulletin(scores)
        logger.info("Daily bulletin generated (%d chars)", len(bulletin))
        return bulletin

    # ── Bulletin formatting ──────────────────────────────────

    @staticmethod
    def _progress_bar(value: float, width: int = 30) -> str:
        """Render an ASCII art progress bar for a 0-1 value.

        Example: [################..............] 53.2%
        """
        filled = int(round(value * width))
        empty = width - filled
        bar = "#" * filled + "." * empty
        return f"[{bar}] {value * 100:5.1f}%"

    @staticmethod
    def _alert_badge(level: str) -> str:
        """Return a fixed-width alert badge."""
        badges = {
            "CRITICAL": "*** CRITICAL ***",
            "HIGH": "**  HIGH     **",
            "ELEVATED": "*  ELEVATED   *",
            "WATCH": "   WATCH      ",
            "LOW": "   LOW        ",
        }
        return badges.get(level, level)

    def _format_bulletin(
        self,
        scores: List[CrisisProbabilityScore],
    ) -> str:
        """Format the full daily bulletin with ASCII art progress bars."""
        now = datetime.now(timezone.utc)
        lines: List[str] = []

        lines.append("=" * 78)
        lines.append("   MDA HUMANITARIAN CRISIS PREDICTOR — DAILY BULLETIN")
        lines.append(f"   Generated: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        lines.append(f"   Lookback window: {self.lookback_days} days")
        lines.append(f"   Countries monitored: {len(scores)}")
        lines.append("=" * 78)
        lines.append("")

        # Summary counts
        level_counts: Dict[str, int] = {}
        for s in scores:
            level_counts[s.alert_level] = level_counts.get(s.alert_level, 0) + 1

        lines.append("   ALERT SUMMARY")
        lines.append("   " + "-" * 40)
        for level in ("CRITICAL", "HIGH", "ELEVATED", "WATCH", "LOW"):
            count = level_counts.get(level, 0)
            if count > 0:
                lines.append(f"     {level:10s} : {count}")
        lines.append("")

        # Detailed per-country breakdown
        lines.append("-" * 78)
        lines.append("   COUNTRY DETAIL")
        lines.append("-" * 78)

        for score in scores:
            lines.append("")
            badge = self._alert_badge(score.alert_level)
            lines.append(
                f"   {score.country_iso}  {score.country_name:<20s}  {badge}"
            )
            bar = self._progress_bar(score.probability)
            lines.append(f"   Crisis Probability: {bar}")
            lines.append(
                f"   Active events: {score.active_event_count:>5d}    "
                f"Indicators firing: {len(score.indicators)}/{len(FAMINE_PATTERN_TRIGGERS)}"
            )

            if score.indicators:
                lines.append("   Indicator breakdown:")
                for ind in score.indicators:
                    ind_bar_width = 15
                    ind_filled = int(round(ind.contribution * 100 * ind_bar_width))
                    ind_empty = ind_bar_width - min(ind_filled, ind_bar_width)
                    ind_bar = "#" * min(ind_filled, ind_bar_width) + "." * ind_empty
                    phase_str = (
                        f" IPC-{ind.max_severity_phase}" if ind.max_severity_phase else ""
                    )
                    pop_str = ""
                    if ind.total_affected > 0:
                        pop_str = f" pop={ind.total_affected:,}"
                    lines.append(
                        f"     {ind.event_type:<22s} "
                        f"n={ind.event_count:>3d} "
                        f"conf={ind.avg_confidence:.2f} "
                        f"risk={ind.avg_risk_score:>4.1f} "
                        f"[{ind_bar}] +{ind.contribution:.3f}"
                        f"{phase_str}{pop_str}"
                    )

            lines.append("")

        # Footer
        lines.append("=" * 78)
        lines.append("   METHODOLOGY")
        lines.append("   " + "-" * 40)
        lines.append(
            "   Composite score = weighted sum of 7 famine-pattern triggers:"
        )
        for event_type, weight in sorted(
            FAMINE_PATTERN_TRIGGERS.items(), key=lambda x: x[1], reverse=True
        ):
            pct = weight / _TOTAL_WEIGHT * 100
            lines.append(f"     {event_type:<22s} weight={weight:.2f} ({pct:4.1f}%)")

        lines.append("")
        lines.append(
            "   Each trigger signal = f(event_count, confidence, risk_score,"
        )
        lines.append(
            "                          severity_phase, affected_population)"
        )
        lines.append(
            "   Alert levels: CRITICAL>=0.85  HIGH>=0.65  ELEVATED>=0.45"
            "  WATCH>=0.25  LOW<0.25"
        )
        lines.append("=" * 78)

        return "\n".join(lines)
