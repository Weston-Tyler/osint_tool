"""GDELT TV API client for broadcast news monitoring.

Provides keyword search across closed-caption transcripts from major
TV news networks, with narrative shift detection for MDA intelligence.

API reference: https://blog.gdeltproject.org/gdelt-2-0-television-api-debuts/
"""

import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Any

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.gdelt.tv_api")

TV_API_BASE_URL = "https://api.gdeltproject.org/api/v2/tv/tv"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_TV_CLIPS = "mda.gdelt.tv.clips"
TOPIC_TV_NARRATIVE_SHIFTS = "mda.gdelt.tv.narrative_shifts"

# Minimum interval between API requests (seconds)
_RATE_LIMIT_INTERVAL = 1.0

# Pre-defined MDA monitoring keywords
MDA_MONITOR_KEYWORDS: list[str] = [
    "drug cartel",
    "fentanyl",
    "maritime",
    "border drone",
    "sanctions",
    "refugee crisis",
    "food crisis",
    "narco submarine",
]


class GDELTTvAPI:
    """Client for the GDELT Television 2.0 API with MDA broadcast monitoring."""

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
        """Execute a GET request with rate limiting (max 1 req/sec)."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < _RATE_LIMIT_INTERVAL:
            time.sleep(_RATE_LIMIT_INTERVAL - elapsed)

        logger.debug("TV API request params: %s", params)
        resp = self._session.get(TV_API_BASE_URL, params=params, timeout=60)
        self._last_request_time = time.monotonic()
        resp.raise_for_status()
        return resp

    # ------------------------------------------------------------------
    # Core search methods
    # ------------------------------------------------------------------

    def search_clips(
        self,
        query: str,
        timespan: str = "7d",
        network: str | None = None,
        max_records: int = 250,
        startdatetime: str | None = None,
        enddatetime: str | None = None,
    ) -> list[dict[str, Any]]:
        """Search for TV broadcast clips matching a keyword query.

        Args:
            query: Keyword search across closed-caption transcripts.
            timespan: Lookback window (e.g. ``"7d"``, ``"30d"``).
            network: Optional network filter (e.g. ``"CNN"``, ``"FOXNEWS"``).
            max_records: Maximum clips to return.
            startdatetime: Optional start datetime ``YYYYMMDDHHMMSS``.
            enddatetime: Optional end datetime ``YYYYMMDDHHMMSS``.

        Returns:
            List of clip metadata dicts.
        """
        params: dict[str, Any] = {
            "query": query,
            "mode": "clipgallery",
            "format": "json",
            "timespan": timespan,
            "maxrecords": max_records,
        }
        if network:
            params["network"] = network
        if startdatetime:
            params["startdatetime"] = startdatetime
        if enddatetime:
            params["enddatetime"] = enddatetime

        resp = self._rate_limited_get(params)

        try:
            data = resp.json()
        except ValueError:
            logger.error("TV API returned non-JSON for query: %s", query)
            return []

        clips = data.get("clips", [])

        # Annotate with MDA metadata
        enriched: list[dict[str, Any]] = []
        for clip in clips:
            clip["_mda_source"] = "gdelt_tv_api"
            clip["_mda_query"] = query
            clip["_mda_ingested_at"] = datetime.now(timezone.utc).isoformat()
            enriched.append(clip)

        self._publish(TOPIC_TV_CLIPS, enriched)
        logger.info(
            "TV API returned %d clips for query: %s", len(enriched), query
        )
        return enriched

    def search_volume_timeline(
        self, query: str, timespan: str = "30d"
    ) -> dict[str, Any]:
        """Track keyword mention frequency over time in TV broadcasts.

        Args:
            query: Keyword to track.
            timespan: Lookback window.

        Returns:
            Raw JSON response with timeline volume series.
        """
        params: dict[str, Any] = {
            "query": query,
            "mode": "timelinevol",
            "format": "json",
            "timespan": timespan,
        }
        resp = self._rate_limited_get(params)

        try:
            return resp.json()
        except ValueError:
            logger.error(
                "TV API volume timeline returned non-JSON for: %s", query
            )
            return {}

    def search_tone_timeline(
        self, query: str, timespan: str = "30d"
    ) -> dict[str, Any]:
        """Track sentiment around a topic in TV broadcasts over time.

        Args:
            query: Keyword to track tone for.
            timespan: Lookback window.

        Returns:
            Raw JSON response with timeline tone series.
        """
        params: dict[str, Any] = {
            "query": query,
            "mode": "timelinetone",
            "format": "json",
            "timespan": timespan,
        }
        resp = self._rate_limited_get(params)

        try:
            return resp.json()
        except ValueError:
            logger.error(
                "TV API tone timeline returned non-JSON for: %s", query
            )
            return {}

    def monitor_mda_keywords(
        self, timespan: str = "7d"
    ) -> dict[str, list[dict[str, Any]]]:
        """Run clip searches for all pre-defined MDA monitoring keywords.

        Args:
            timespan: Lookback window for all keyword searches.

        Returns:
            Dict mapping keyword to list of clip dicts.
        """
        results: dict[str, list[dict[str, Any]]] = {}
        for keyword in MDA_MONITOR_KEYWORDS:
            try:
                clips = self.search_clips(query=keyword, timespan=timespan)
                results[keyword] = clips
                logger.info(
                    "MDA keyword monitor [%s]: %d clips", keyword, len(clips)
                )
            except Exception:
                logger.exception(
                    "Error monitoring MDA keyword: %s", keyword
                )
                results[keyword] = []
        return results

    def detect_narrative_shift(
        self, keyword: str, lookback_days: int = 30
    ) -> dict[str, Any] | None:
        """Detect narrative tone shifts for a keyword in TV broadcasts.

        Compares the most recent 7-day average tone against the full lookback
        baseline. Flags a shift when the z-score exceeds 2.0.

        Args:
            keyword: Keyword to analyze.
            lookback_days: Total baseline period in days (default 30).

        Returns:
            Narrative shift report dict if a significant shift is detected,
            or None if tone is within normal range.
        """
        timespan = f"{lookback_days}d"
        tone_data = self.search_tone_timeline(query=keyword, timespan=timespan)

        # Extract the timeline series from the response
        # GDELT TV API returns timeline data under various keys
        timeline = tone_data.get("timeline", [])
        if not timeline:
            logger.warning(
                "No tone timeline data for keyword: %s", keyword
            )
            return None

        # The timeline is a list of series; take the first one
        series = timeline[0] if timeline else {}
        data_points = series.get("data", [])

        if len(data_points) < 8:
            logger.warning(
                "Insufficient data points (%d) for narrative shift "
                "detection on keyword: %s",
                len(data_points),
                keyword,
            )
            return None

        # Extract tone values from data points
        tone_values: list[float] = []
        for point in data_points:
            value = point.get("value", point.get("y", 0.0))
            try:
                tone_values.append(float(value))
            except (ValueError, TypeError):
                tone_values.append(0.0)

        if not tone_values:
            return None

        # Split into baseline (full period) and recent (last 7 days)
        # Approximate: 7 days out of lookback_days
        recent_fraction = 7 / lookback_days
        recent_count = max(1, int(len(tone_values) * recent_fraction))

        baseline_values = tone_values[:-recent_count] if recent_count < len(tone_values) else tone_values
        recent_values = tone_values[-recent_count:]

        if not baseline_values or not recent_values:
            return None

        # Calculate baseline statistics
        baseline_mean = sum(baseline_values) / len(baseline_values)
        variance = sum(
            (v - baseline_mean) ** 2 for v in baseline_values
        ) / len(baseline_values)
        baseline_std = math.sqrt(variance) if variance > 0 else 0.0

        recent_mean = sum(recent_values) / len(recent_values)

        # Calculate z-score
        if baseline_std == 0:
            z_score = 0.0
        else:
            z_score = (recent_mean - baseline_mean) / baseline_std

        shift_detected = abs(z_score) > 2.0
        direction = "positive" if z_score > 0 else "negative"

        report: dict[str, Any] = {
            "keyword": keyword,
            "lookback_days": lookback_days,
            "baseline_mean_tone": round(baseline_mean, 4),
            "baseline_std_tone": round(baseline_std, 4),
            "recent_7d_mean_tone": round(recent_mean, 4),
            "z_score": round(z_score, 4),
            "shift_detected": shift_detected,
            "shift_direction": direction if shift_detected else "none",
            "data_points_total": len(tone_values),
            "data_points_baseline": len(baseline_values),
            "data_points_recent": len(recent_values),
            "analyzed_at": datetime.now(timezone.utc).isoformat(),
        }

        if shift_detected:
            logger.warning(
                "NARRATIVE SHIFT DETECTED for '%s': z-score=%.2f, "
                "direction=%s, baseline_tone=%.3f, recent_tone=%.3f",
                keyword,
                z_score,
                direction,
                baseline_mean,
                recent_mean,
            )
            self._publish(TOPIC_TV_NARRATIVE_SHIFTS, [report])
        else:
            logger.info(
                "No narrative shift for '%s': z-score=%.2f", keyword, z_score
            )

        return report

    def detect_all_mda_narrative_shifts(
        self, lookback_days: int = 30
    ) -> list[dict[str, Any]]:
        """Run narrative shift detection for all MDA keywords.

        Args:
            lookback_days: Baseline period in days.

        Returns:
            List of narrative shift reports (only those with shifts detected).
        """
        shifts: list[dict[str, Any]] = []
        for keyword in MDA_MONITOR_KEYWORDS:
            try:
                report = self.detect_narrative_shift(
                    keyword=keyword, lookback_days=lookback_days
                )
                if report and report["shift_detected"]:
                    shifts.append(report)
            except Exception:
                logger.exception(
                    "Error detecting narrative shift for: %s", keyword
                )
        logger.info(
            "Narrative shift scan complete: %d/%d keywords show shifts",
            len(shifts),
            len(MDA_MONITOR_KEYWORDS),
        )
        return shifts

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


def main() -> None:
    """CLI convenience -- monitor MDA keywords and run narrative shift scan."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    client = GDELTTvAPI(publish_to_kafka=True)
    try:
        # Run keyword monitoring
        keyword_results = client.monitor_mda_keywords(timespan="7d")
        total_clips = sum(len(clips) for clips in keyword_results.values())
        logger.info("MDA keyword monitor complete: %d total clips", total_clips)

        # Run narrative shift detection
        shifts = client.detect_all_mda_narrative_shifts(lookback_days=30)
        if shifts:
            logger.warning(
                "Narrative shifts detected in %d keywords", len(shifts)
            )
            for shift in shifts:
                logger.warning(
                    "  - %s: z_score=%.2f (%s)",
                    shift["keyword"],
                    shift["z_score"],
                    shift["shift_direction"],
                )
    finally:
        client.close()


if __name__ == "__main__":
    main()
