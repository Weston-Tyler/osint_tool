"""HDX ingestion worker — pulls priority humanitarian datasets from the
Humanitarian Data Exchange (HDX) via the hdx-python-api, maps rows into
CausalEvent nodes, and upserts them into Memgraph with deterministic
event_id hashing.
"""

import hashlib
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from gqlalchemy import Memgraph
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource

logger = logging.getLogger("mda.humanitarian_monitor.hdx_worker")

MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))
HDX_SITE = os.getenv("HDX_SITE", "prod")

# ── Priority datasets ──────────────────────────────────────

PRIORITY_DATASETS: Dict[str, Dict[str, str]] = {
    "acled-conflict": {
        "hdx_id": "political-violence-events-and-fatalities",
        "description": "ACLED armed conflict and political violence events",
        "mapper": "_map_conflict_rows",
    },
    "ipc-food-security": {
        "hdx_id": "ipc-country-data",
        "description": "IPC Integrated Food Security Phase Classification",
        "mapper": "_map_food_security_rows",
    },
    "unhcr-displacement": {
        "hdx_id": "unhcr-population-data-for-world",
        "description": "UNHCR refugee and displacement statistics",
        "mapper": "_map_displacement_rows",
    },
    "inform-risk": {
        "hdx_id": "inform-global-risk-index",
        "description": "INFORM Global Risk Index",
        "mapper": "_map_inform_rows",
    },
    "wfp-food-prices": {
        "hdx_id": "wfp-food-prices",
        "description": "WFP food price monitoring",
        "mapper": "_map_food_price_rows",
    },
    "idmc-displacement-events": {
        "hdx_id": "idmc-internally-displaced-persons-idps",
        "description": "IDMC internal displacement events",
        "mapper": "_map_displacement_rows",
    },
}

# ── Data classes ────────────────────────────────────────────


@dataclass
class IngestionResult:
    """Result summary for a single dataset ingestion run."""

    dataset_key: str
    hdx_id: str
    rows_processed: int
    events_upserted: int
    events_skipped: int
    errors: List[str] = field(default_factory=list)
    ingested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def success(self) -> bool:
        return self.events_upserted > 0 and len(self.errors) == 0

    def to_dict(self) -> dict:
        return {
            "dataset_key": self.dataset_key,
            "hdx_id": self.hdx_id,
            "rows_processed": self.rows_processed,
            "events_upserted": self.events_upserted,
            "events_skipped": self.events_skipped,
            "errors": self.errors,
            "success": self.success,
            "ingested_at": self.ingested_at.isoformat(),
        }


# ── HDXIngestionWorker ──────────────────────────────────────


class HDXIngestionWorker:
    """Downloads priority humanitarian datasets from HDX, maps rows to
    CausalEvent nodes, and upserts them into the Memgraph causal graph."""

    def __init__(
        self,
        mg: Optional[Memgraph] = None,
        hdx_site: Optional[str] = None,
    ):
        self.mg = mg or Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
        self._hdx_configured = False
        self._hdx_site = hdx_site or HDX_SITE

    def _ensure_hdx_config(self) -> None:
        """Initialize HDX configuration once."""
        if not self._hdx_configured:
            try:
                Configuration.read()
            except Exception:
                Configuration.create(
                    hdx_site=self._hdx_site,
                    user_agent="MDA_OSINT_Tool/1.0",
                )
            self._hdx_configured = True

    # ── Deterministic event ID ───────────────────────────────

    @staticmethod
    def _make_event_id(source: str, *components: Any) -> str:
        """Generate a deterministic SHA-256 event_id from source and key
        components so that re-ingestion produces the same node."""
        payload = "|".join(str(c) for c in [source, *components])
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]

    # ── Memgraph upsert ──────────────────────────────────────

    def _upsert_causal_event(self, props: Dict[str, Any]) -> None:
        """MERGE a CausalEvent node using its deterministic event_id."""
        query = """
            MERGE (e:CausalEvent {event_id: $event_id})
            SET e.event_type              = $event_type,
                e.description             = $description,
                e.domain                  = $domain,
                e.occurred_at             = $occurred_at,
                e.location_country_iso    = $country_iso,
                e.location_region         = $region,
                e.confidence              = $confidence,
                e.causal_weight           = $causal_weight,
                e.source                  = $source,
                e.source_dataset          = $source_dataset,
                e.risk_score              = $risk_score,
                e.fatalities              = $fatalities,
                e.affected_population     = $affected_population,
                e.severity_phase          = $severity_phase,
                e.status                  = $status,
                e.ingested_at             = $ingested_at
        """
        self.mg.execute_and_fetch(query, props)

    # ── Dataset ingestion ────────────────────────────────────

    def ingest_dataset(self, dataset_key: str) -> IngestionResult:
        """Ingest a single priority dataset by key.

        Steps:
        1. Look up the HDX dataset by its slug / id.
        2. Download the first CSV/XLSX resource.
        3. Route rows through the appropriate mapper.
        4. Upsert resulting CausalEvent nodes into Memgraph.
        """
        if dataset_key not in PRIORITY_DATASETS:
            raise ValueError(
                f"Unknown dataset key '{dataset_key}'. "
                f"Available: {', '.join(PRIORITY_DATASETS.keys())}"
            )

        self._ensure_hdx_config()
        meta = PRIORITY_DATASETS[dataset_key]
        hdx_id = meta["hdx_id"]
        mapper_name = meta["mapper"]

        logger.info("Ingesting dataset '%s' (HDX: %s)", dataset_key, hdx_id)

        try:
            dataset = Dataset.read_from_hdx(hdx_id)
        except Exception as exc:
            logger.error("Failed to read dataset '%s' from HDX: %s", hdx_id, exc)
            return IngestionResult(
                dataset_key=dataset_key,
                hdx_id=hdx_id,
                rows_processed=0,
                events_upserted=0,
                events_skipped=0,
                errors=[f"HDX read failed: {exc}"],
            )

        if dataset is None:
            logger.error("Dataset '%s' not found on HDX", hdx_id)
            return IngestionResult(
                dataset_key=dataset_key,
                hdx_id=hdx_id,
                rows_processed=0,
                events_upserted=0,
                events_skipped=0,
                errors=[f"Dataset not found on HDX: {hdx_id}"],
            )

        # Find first usable resource (CSV preferred)
        resources: List[Resource] = dataset.get_resources()
        target_resource: Optional[Resource] = None
        for res in resources:
            fmt = (res.get("format") or "").upper()
            if fmt in ("CSV", "XLSX", "XLS"):
                target_resource = res
                break

        if target_resource is None:
            logger.error("No CSV/XLSX resource found for '%s'", hdx_id)
            return IngestionResult(
                dataset_key=dataset_key,
                hdx_id=hdx_id,
                rows_processed=0,
                events_upserted=0,
                events_skipped=0,
                errors=["No CSV/XLSX resource found"],
            )

        # Download and read
        try:
            import pandas as pd

            url, path = target_resource.download()
            fmt = (target_resource.get("format") or "").upper()
            if fmt == "CSV":
                df = pd.read_csv(path, low_memory=False)
            else:
                df = pd.read_excel(path)
            logger.info("Downloaded %d rows from '%s'", len(df), dataset_key)
        except Exception as exc:
            logger.error("Failed to download resource for '%s': %s", hdx_id, exc)
            return IngestionResult(
                dataset_key=dataset_key,
                hdx_id=hdx_id,
                rows_processed=0,
                events_upserted=0,
                events_skipped=0,
                errors=[f"Download failed: {exc}"],
            )

        # Map rows to CausalEvent dicts
        mapper = getattr(self, mapper_name, None)
        if mapper is None:
            logger.error("Mapper '%s' not found", mapper_name)
            return IngestionResult(
                dataset_key=dataset_key,
                hdx_id=hdx_id,
                rows_processed=len(df),
                events_upserted=0,
                events_skipped=0,
                errors=[f"Mapper not found: {mapper_name}"],
            )

        events = mapper(df, dataset_key)
        logger.info("Mapped %d events from '%s'", len(events), dataset_key)

        # Upsert into Memgraph
        upserted = 0
        skipped = 0
        errors: List[str] = []

        for evt in events:
            try:
                self._upsert_causal_event(evt)
                upserted += 1
            except Exception as exc:
                skipped += 1
                if len(errors) < 10:
                    errors.append(f"Upsert failed for {evt.get('event_id', '?')}: {exc}")

        result = IngestionResult(
            dataset_key=dataset_key,
            hdx_id=hdx_id,
            rows_processed=len(df),
            events_upserted=upserted,
            events_skipped=skipped,
            errors=errors,
        )

        logger.info(
            "Ingestion of '%s' complete: %d upserted, %d skipped, %d errors",
            dataset_key,
            upserted,
            skipped,
            len(errors),
        )
        return result

    # ── Batch ingestion ──────────────────────────────────────

    def run_priority_ingestion(self) -> List[IngestionResult]:
        """Ingest all priority datasets sequentially.

        Returns a list of IngestionResult summaries.
        """
        logger.info("Starting priority ingestion of %d datasets", len(PRIORITY_DATASETS))
        results: List[IngestionResult] = []

        for key in PRIORITY_DATASETS:
            try:
                result = self.ingest_dataset(key)
                results.append(result)
            except Exception as exc:
                logger.error("Unhandled error ingesting '%s': %s", key, exc)
                results.append(
                    IngestionResult(
                        dataset_key=key,
                        hdx_id=PRIORITY_DATASETS[key]["hdx_id"],
                        rows_processed=0,
                        events_upserted=0,
                        events_skipped=0,
                        errors=[f"Unhandled: {exc}"],
                    )
                )

        total_upserted = sum(r.events_upserted for r in results)
        total_errors = sum(len(r.errors) for r in results)
        logger.info(
            "Priority ingestion complete: %d total events upserted, %d total errors",
            total_upserted,
            total_errors,
        )
        return results

    # ── Row mappers ──────────────────────────────────────────

    def _map_conflict_rows(
        self,
        df: "pd.DataFrame",
        dataset_key: str,
    ) -> List[Dict[str, Any]]:
        """Map ACLED-format conflict data to CausalEvent property dicts.

        Expected columns: event_id_cnty, event_date, event_type, sub_event_type,
        country, iso, admin1, fatalities, notes, source.
        """
        import pandas as pd

        now = datetime.now(timezone.utc).isoformat()
        events: List[Dict[str, Any]] = []

        for _, row in df.iterrows():
            try:
                event_id = self._make_event_id(
                    "acled",
                    row.get("event_id_cnty", ""),
                    row.get("event_date", ""),
                )

                fatalities = int(row.get("fatalities", 0) or 0)
                # Severity heuristic based on fatalities
                if fatalities >= 100:
                    causal_weight = 0.95
                    risk_score = 9.0
                elif fatalities >= 10:
                    causal_weight = 0.7
                    risk_score = 7.0
                elif fatalities >= 1:
                    causal_weight = 0.5
                    risk_score = 5.0
                else:
                    causal_weight = 0.3
                    risk_score = 3.0

                occurred_at = str(row.get("event_date", ""))
                # Parse date to ISO format
                try:
                    dt = pd.to_datetime(occurred_at)
                    occurred_at = dt.isoformat()
                except Exception:
                    pass

                events.append({
                    "event_id": event_id,
                    "event_type": "ARMED_CONFLICT",
                    "description": (
                        f"{row.get('event_type', 'Conflict')}: "
                        f"{row.get('sub_event_type', '')} — "
                        f"{str(row.get('notes', ''))[:200]}"
                    ),
                    "domain": "humanitarian",
                    "occurred_at": occurred_at,
                    "country_iso": str(row.get("iso", row.get("iso3", "")))[:3],
                    "region": str(row.get("admin1", "")),
                    "confidence": 0.85,
                    "causal_weight": causal_weight,
                    "source": "ACLED",
                    "source_dataset": dataset_key,
                    "risk_score": risk_score,
                    "fatalities": fatalities,
                    "affected_population": None,
                    "severity_phase": None,
                    "status": "ACTIVE",
                    "ingested_at": now,
                })
            except Exception as exc:
                logger.debug("Skipping conflict row: %s", exc)

        return events

    def _map_food_security_rows(
        self,
        df: "pd.DataFrame",
        dataset_key: str,
    ) -> List[Dict[str, Any]]:
        """Map IPC food security data to CausalEvent property dicts.

        Only ingests rows at IPC Phase 3+ (Crisis, Emergency, Famine).
        Expected columns: country, iso3, ipc_phase, population, analysis_date,
        area_name.
        """
        import pandas as pd

        now = datetime.now(timezone.utc).isoformat()
        events: List[Dict[str, Any]] = []

        # Normalize column names to lowercase
        df.columns = [c.lower().strip() for c in df.columns]

        phase_col = None
        for candidate in ("ipc_phase", "phase", "classification", "ipc_level"):
            if candidate in df.columns:
                phase_col = candidate
                break

        if phase_col is None:
            logger.warning("No IPC phase column found in food security data")
            return events

        for _, row in df.iterrows():
            try:
                phase = int(row.get(phase_col, 0) or 0)
                if phase < 3:
                    continue  # Only Crisis (3), Emergency (4), Famine (5)

                population = int(row.get("population", row.get("pop", 0)) or 0)
                country_iso = str(
                    row.get("iso3", row.get("country_iso", row.get("iso", "")))
                )[:3]

                event_id = self._make_event_id(
                    "ipc",
                    country_iso,
                    row.get("area_name", row.get("admin1", "")),
                    row.get("analysis_date", row.get("date", "")),
                    phase,
                )

                phase_label = {3: "Crisis", 4: "Emergency", 5: "Famine"}.get(
                    phase, f"Phase {phase}"
                )

                causal_weight = {3: 0.6, 4: 0.8, 5: 1.0}.get(phase, 0.5)
                risk_score = {3: 6.0, 4: 8.0, 5: 10.0}.get(phase, 5.0)

                analysis_date = str(
                    row.get("analysis_date", row.get("date", ""))
                )
                try:
                    dt = pd.to_datetime(analysis_date)
                    analysis_date = dt.isoformat()
                except Exception:
                    pass

                events.append({
                    "event_id": event_id,
                    "event_type": "FOOD_INSECURITY",
                    "description": (
                        f"IPC {phase_label} (Phase {phase}) in "
                        f"{row.get('area_name', row.get('admin1', country_iso))} — "
                        f"{population:,} affected"
                    ),
                    "domain": "humanitarian",
                    "occurred_at": analysis_date,
                    "country_iso": country_iso,
                    "region": str(row.get("area_name", row.get("admin1", ""))),
                    "confidence": 0.9,
                    "causal_weight": causal_weight,
                    "source": "IPC",
                    "source_dataset": dataset_key,
                    "risk_score": risk_score,
                    "fatalities": None,
                    "affected_population": population,
                    "severity_phase": phase,
                    "status": "ACTIVE",
                    "ingested_at": now,
                })
            except Exception as exc:
                logger.debug("Skipping food security row: %s", exc)

        return events

    def _map_displacement_rows(
        self,
        df: "pd.DataFrame",
        dataset_key: str,
    ) -> List[Dict[str, Any]]:
        """Map UNHCR / IDMC displacement data to CausalEvent property dicts.

        Expected columns: iso3, country_of_origin, refugees, idps, year,
        asylum_seekers.
        """
        import pandas as pd

        now = datetime.now(timezone.utc).isoformat()
        events: List[Dict[str, Any]] = []
        df.columns = [c.lower().strip() for c in df.columns]

        for _, row in df.iterrows():
            try:
                country_iso = str(
                    row.get("iso3", row.get("country_iso", row.get("iso", "")))
                )[:3]

                refugees = int(row.get("refugees", row.get("refugee", 0)) or 0)
                idps = int(row.get("idps", row.get("idp", 0)) or 0)
                asylum = int(row.get("asylum_seekers", row.get("asylum", 0)) or 0)
                total_displaced = refugees + idps + asylum

                if total_displaced < 1000:
                    continue  # Skip trivially small rows

                year = str(row.get("year", row.get("date", "")))
                origin = str(
                    row.get("country_of_origin", row.get("origin", country_iso))
                )

                event_id = self._make_event_id(
                    "displacement",
                    country_iso,
                    origin,
                    year,
                )

                # Severity by displaced population
                if total_displaced >= 1_000_000:
                    causal_weight = 0.95
                    risk_score = 9.5
                elif total_displaced >= 100_000:
                    causal_weight = 0.8
                    risk_score = 7.5
                elif total_displaced >= 10_000:
                    causal_weight = 0.6
                    risk_score = 5.5
                else:
                    causal_weight = 0.4
                    risk_score = 3.5

                events.append({
                    "event_id": event_id,
                    "event_type": "DISPLACEMENT",
                    "description": (
                        f"Displacement from {origin}: "
                        f"{refugees:,} refugees, {idps:,} IDPs, "
                        f"{asylum:,} asylum seekers"
                    ),
                    "domain": "humanitarian",
                    "occurred_at": year if len(year) == 4 else year,
                    "country_iso": country_iso,
                    "region": str(row.get("admin1", row.get("region", ""))),
                    "confidence": 0.88,
                    "causal_weight": causal_weight,
                    "source": "UNHCR",
                    "source_dataset": dataset_key,
                    "risk_score": risk_score,
                    "fatalities": None,
                    "affected_population": total_displaced,
                    "severity_phase": None,
                    "status": "ACTIVE",
                    "ingested_at": now,
                })
            except Exception as exc:
                logger.debug("Skipping displacement row: %s", exc)

        return events

    def _map_inform_rows(
        self,
        df: "pd.DataFrame",
        dataset_key: str,
    ) -> List[Dict[str, Any]]:
        """Map INFORM Risk Index data to CausalEvent property dicts.

        Only ingests rows where the INFORM score exceeds 5.0.
        Expected columns: iso3, country, inform_risk, inform_risk_score,
        hazard_exposure, vulnerability, lack_of_coping_capacity, year.
        """
        import pandas as pd

        now = datetime.now(timezone.utc).isoformat()
        events: List[Dict[str, Any]] = []
        df.columns = [c.lower().strip() for c in df.columns]

        score_col = None
        for candidate in (
            "inform_risk", "inform_risk_score", "inform_score",
            "risk_score", "overall_score",
        ):
            if candidate in df.columns:
                score_col = candidate
                break

        if score_col is None:
            logger.warning("No INFORM score column found in data")
            return events

        for _, row in df.iterrows():
            try:
                score = float(row.get(score_col, 0) or 0)
                if score <= 5.0:
                    continue

                country_iso = str(
                    row.get("iso3", row.get("country_iso", row.get("iso", "")))
                )[:3]
                country_name = str(row.get("country", row.get("country_name", "")))
                year = str(row.get("year", row.get("date", "")))

                event_id = self._make_event_id(
                    "inform", country_iso, year,
                )

                # Severity scaling
                if score >= 8.0:
                    causal_weight = 0.95
                    risk_mapped = 9.5
                elif score >= 6.5:
                    causal_weight = 0.75
                    risk_mapped = 7.5
                else:
                    causal_weight = 0.55
                    risk_mapped = 6.0

                hazard = float(row.get("hazard_exposure", row.get("hazard", 0)) or 0)
                vulnerability = float(
                    row.get("vulnerability", row.get("vuln", 0)) or 0
                )
                coping = float(
                    row.get(
                        "lack_of_coping_capacity",
                        row.get("coping_capacity", 0),
                    ) or 0
                )

                events.append({
                    "event_id": event_id,
                    "event_type": "HIGH_RISK_COUNTRY",
                    "description": (
                        f"INFORM Risk Index {score:.1f} for {country_name} "
                        f"(hazard={hazard:.1f}, vuln={vulnerability:.1f}, "
                        f"coping={coping:.1f})"
                    ),
                    "domain": "humanitarian",
                    "occurred_at": year,
                    "country_iso": country_iso,
                    "region": "",
                    "confidence": 0.92,
                    "causal_weight": causal_weight,
                    "source": "INFORM",
                    "source_dataset": dataset_key,
                    "risk_score": risk_mapped,
                    "fatalities": None,
                    "affected_population": None,
                    "severity_phase": None,
                    "status": "ACTIVE",
                    "ingested_at": now,
                })
            except Exception as exc:
                logger.debug("Skipping INFORM row: %s", exc)

        return events

    def _map_food_price_rows(
        self,
        df: "pd.DataFrame",
        dataset_key: str,
    ) -> List[Dict[str, Any]]:
        """Map WFP food price monitoring data to CausalEvent property dicts.

        Expected columns: adm0_name, adm0_id, cm_name, mp_month, mp_year,
        mp_price, mp_commoditysource.
        """
        import pandas as pd

        now = datetime.now(timezone.utc).isoformat()
        events: List[Dict[str, Any]] = []
        df.columns = [c.lower().strip() for c in df.columns]

        for _, row in df.iterrows():
            try:
                price = float(row.get("mp_price", row.get("price", 0)) or 0)
                if price <= 0:
                    continue

                country = str(row.get("adm0_name", row.get("country", "")))
                country_iso = str(row.get("adm0_id", row.get("iso3", "")))[:3]
                commodity = str(row.get("cm_name", row.get("commodity", "")))
                month = str(row.get("mp_month", row.get("month", "")))
                year = str(row.get("mp_year", row.get("year", "")))

                event_id = self._make_event_id(
                    "wfp_price", country_iso, commodity, year, month,
                )

                events.append({
                    "event_id": event_id,
                    "event_type": "FOOD_PRICE_ALERT",
                    "description": (
                        f"WFP price: {commodity} at {price:.2f} in {country} "
                        f"({year}-{month})"
                    ),
                    "domain": "humanitarian",
                    "occurred_at": f"{year}-{month.zfill(2)}-01" if year and month else "",
                    "country_iso": country_iso,
                    "region": str(row.get("adm1_name", row.get("admin1", ""))),
                    "confidence": 0.8,
                    "causal_weight": 0.5,
                    "source": "WFP",
                    "source_dataset": dataset_key,
                    "risk_score": 4.0,
                    "fatalities": None,
                    "affected_population": None,
                    "severity_phase": None,
                    "status": "ACTIVE",
                    "ingested_at": now,
                })
            except Exception as exc:
                logger.debug("Skipping food price row: %s", exc)

        return events
