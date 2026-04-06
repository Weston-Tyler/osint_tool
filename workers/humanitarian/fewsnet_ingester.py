"""FEWS NET (Famine Early Warning Systems Network) ingestion worker.

Fetches IPC food-security classifications and food-assistance outlook data from
the FEWS NET Data Warehouse (fdw.fews.net), normalises them to CausalEvents,
detects phase escalations, and publishes to Kafka.

Reference: https://fews.net/data | https://fdw.fews.net/api/
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.humanitarian.fewsnet")

FEWSNET_BASE_URL = "https://fdw.fews.net/api"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# Monitored countries (ISO-2).  FEWS NET coverage is limited to food-insecure
# regions so we track the most critical ones.
DEFAULT_COUNTRIES = [
    "AF", "ET", "YE", "SO", "SS", "NG", "CD", "HT", "SD", "MW",
    "MZ", "ZW", "BF", "NE", "ML", "TD", "KE", "UG", "GT", "HN",
]

# IPC phase severity mapping (phase 1-5 -> severity 0-10 scale).
IPC_SEVERITY = {1: 2, 2: 4, 3: 6, 4: 8, 5: 10}


class FEWSNETIngester:
    """Fetches FEWS NET IPC and food-assistance data, detects phase
    escalation, and publishes CausalEvents to Kafka."""

    def __init__(
        self,
        kafka_bootstrap: str | None = None,
        countries: list[str] | None = None,
    ):
        self.kafka_bootstrap = kafka_bootstrap or KAFKA_BOOTSTRAP
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

    def _send(self, topic: str, record: dict) -> None:
        try:
            self._get_producer().send(topic, record)
        except Exception as exc:
            logger.error("Kafka send to %s failed: %s", topic, exc)
            self._get_producer().send(
                "mda.dlq",
                {"source": "fewsnet", "topic": topic, "error": str(exc), "raw": str(record)[:500]},
            )

    # ------------------------------------------------------------------
    # API helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get(path: str, params: dict | None = None) -> Any:
        url = f"{FEWSNET_BASE_URL}{path}"
        resp = requests.get(url, params=params or {}, timeout=90)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        if "json" in content_type:
            return resp.json()
        # Some endpoints return CSV; try JSON first.
        try:
            return resp.json()
        except ValueError:
            return resp.text

    # ------------------------------------------------------------------
    # Data fetchers
    # ------------------------------------------------------------------

    def fetch_ipc_classifications(self, country_iso2: str) -> list[dict]:
        """Fetch current and projected IPC phase classifications.

        Parameters
        ----------
        country_iso2 : str
            ISO-3166-1 alpha-2 country code.
        """
        params = {
            "country_code": country_iso2,
            "format": "json",
        }
        data = self._get("/ipcphase/", params)

        # The API may return a dict with a ``results`` key or a bare list.
        if isinstance(data, dict):
            items = data.get("results", data.get("features", []))
        elif isinstance(data, list):
            items = data
        else:
            items = []

        logger.info("Fetched %d IPC classifications for %s", len(items), country_iso2)
        return items

    def fetch_food_assistance_outlook(self) -> list[dict]:
        """Fetch the latest food-assistance outlook — countries projected to
        need emergency food assistance."""
        data = self._get("/foodassistanceoutlook/", {"format": "json"})

        if isinstance(data, dict):
            items = data.get("results", data.get("features", []))
        elif isinstance(data, list):
            items = data
        else:
            items = []

        logger.info("Fetched %d food-assistance outlook records", len(items))
        return items

    # ------------------------------------------------------------------
    # Normalizer
    # ------------------------------------------------------------------

    @staticmethod
    def normalize_ipc(raw: dict) -> dict:
        """Map a raw FEWS NET IPC record to a CausalEvent structure.

        CausalEvent schema:
            event_type  = FOOD_INSECURITY_IPC_PHASEn
            severity    = phase * 2  (capped at 10)
            location    = admin region / country
            confidence  = 0.92 (FEWS NET analyst-validated)
        """
        props = raw.get("properties", raw)

        phase = props.get("ipc_phase") or props.get("CS", props.get("phase"))
        try:
            phase_int = int(phase)
        except (TypeError, ValueError):
            phase_int = 0

        country = props.get("country_code") or props.get("country", "")
        admin_name = props.get("admin1_name") or props.get("area_name") or props.get("title", "")

        geometry = raw.get("geometry")
        lat = lon = None
        if geometry and geometry.get("type") == "Point":
            coords = geometry.get("coordinates", [])
            if len(coords) >= 2:
                lon, lat = coords[0], coords[1]

        period_start = props.get("start_date") or props.get("period_start", "")
        period_end = props.get("end_date") or props.get("period_end", "")
        projection_type = props.get("projection_type") or props.get("scenario", "current")

        return {
            "event_id": f"fewsnet_ipc_{country}_{admin_name}_{period_start}".replace(" ", "_"),
            "source": "fewsnet",
            "event_type": f"FOOD_INSECURITY_IPC_PHASE{phase_int}",
            "severity": IPC_SEVERITY.get(phase_int, phase_int * 2),
            "country": country,
            "location": admin_name,
            "lat": lat,
            "lon": lon,
            "ipc_phase": phase_int,
            "period_start": period_start,
            "period_end": period_end,
            "projection_type": projection_type,
            "population_affected": props.get("population") or props.get("affected_population"),
            "confidence": 0.92,
            "ingest_time": datetime.now(timezone.utc).isoformat(),
            "raw": raw,
        }

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------

    def detect_phase_escalation(
        self,
        country_iso2: str,
        lookback_months: int = 6,
    ) -> list[dict]:
        """Flag areas where the IPC phase has escalated (2->3 or 3->4)
        compared to the previous analysis period.

        This compares the *current* classification against the most recent
        *previous* classification for each admin region.
        """
        records = self.fetch_ipc_classifications(country_iso2)

        # Build a map: area -> sorted list of (period_start, phase).
        area_history: dict[str, list[tuple[str, int]]] = {}
        for rec in records:
            props = rec.get("properties", rec)
            area = props.get("admin1_name") or props.get("area_name") or props.get("title", "unknown")
            phase = props.get("ipc_phase") or props.get("CS") or props.get("phase")
            period = props.get("start_date") or props.get("period_start", "")
            try:
                phase_int = int(phase)
            except (TypeError, ValueError):
                continue
            area_history.setdefault(area, []).append((period, phase_int))

        escalations: list[dict] = []
        for area, history in area_history.items():
            history.sort(key=lambda x: x[0])
            if len(history) < 2:
                continue

            prev_period, prev_phase = history[-2]
            curr_period, curr_phase = history[-1]

            if curr_phase > prev_phase and prev_phase >= 2:
                escalations.append({
                    "event_type": "IPC_PHASE_ESCALATION",
                    "country": country_iso2,
                    "area": area,
                    "previous_phase": prev_phase,
                    "current_phase": curr_phase,
                    "previous_period": prev_period,
                    "current_period": curr_period,
                    "severity": IPC_SEVERITY.get(curr_phase, curr_phase * 2),
                    "source": "fewsnet",
                    "confidence": 0.92,
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                })

        if escalations:
            logger.warning(
                "Detected %d IPC phase escalations in %s", len(escalations), country_iso2,
            )
        return escalations

    # ------------------------------------------------------------------
    # Publisher
    # ------------------------------------------------------------------

    def publish(self) -> int:
        """Fetch all data, normalize, detect escalations, and publish to Kafka."""
        producer = self._get_producer()
        total = 0

        # --- IPC classifications ---
        for country in self.countries:
            try:
                raw_ipc = self.fetch_ipc_classifications(country)
                for rec in raw_ipc:
                    event = self.normalize_ipc(rec)
                    self._send("mda.fewsnet.ipc", event)
                    total += 1
            except Exception as exc:
                logger.error("IPC fetch/publish failed for %s: %s", country, exc)

        # --- Phase escalations ---
        for country in self.countries:
            try:
                escalations = self.detect_phase_escalation(country)
                for esc in escalations:
                    self._send("mda.fewsnet.ipc", esc)
                    total += 1
            except Exception as exc:
                logger.error("Escalation detection failed for %s: %s", country, exc)

        # --- Food-assistance outlook ---
        try:
            outlook = self.fetch_food_assistance_outlook()
            for rec in outlook:
                normalized = {
                    "event_id": f"fewsnet_fao_{rec.get('country_code', '')}_{rec.get('period', '')}",
                    "source": "fewsnet",
                    "event_type": "FOOD_ASSISTANCE_NEEDED",
                    "country": rec.get("country_code", ""),
                    "period": rec.get("period", ""),
                    "assistance_level": rec.get("assistance_level") or rec.get("level"),
                    "confidence": 0.92,
                    "ingest_time": datetime.now(timezone.utc).isoformat(),
                    "raw": rec,
                }
                self._send("mda.fewsnet.ipc", normalized)
                total += 1
        except Exception as exc:
            logger.error("Food-assistance outlook fetch failed: %s", exc)

        producer.flush()
        logger.info("FEWS NET publish cycle complete: %d records", total)
        return total

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run_polling_loop(self, interval_days: int = 7) -> None:
        """Run the ingestion loop on a fixed interval (default weekly)."""
        logger.info(
            "Starting FEWS NET polling loop (interval=%dd, countries=%d)",
            interval_days, len(self.countries),
        )
        while True:
            cycle_start = time.monotonic()
            try:
                self.publish()
            except Exception as exc:
                logger.exception("FEWS NET polling cycle failed: %s", exc)

            elapsed = time.monotonic() - cycle_start
            sleep_secs = max(0, interval_days * 86400 - elapsed)
            logger.info("FEWS NET cycle complete in %.1fs, sleeping %.0fs", elapsed, sleep_secs)
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
    ingester = FEWSNETIngester()
    try:
        ingester.run_polling_loop()
    except KeyboardInterrupt:
        logger.info("Shutting down FEWS NET ingester")
    finally:
        ingester.close()
