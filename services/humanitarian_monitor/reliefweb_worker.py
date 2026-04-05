"""ReliefWeb ingestion worker — polls the ReliefWeb API for recent
humanitarian reports, extracts causal relations from report text,
and upserts CausalEvent nodes plus CAUSES edges into Memgraph.
"""

import asyncio
import hashlib
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp
from gqlalchemy import Memgraph

logger = logging.getLogger("mda.humanitarian_monitor.reliefweb")

MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))

RELIEFWEB_API_BASE = "https://api.reliefweb.int/v1"
RELIEFWEB_APPNAME = os.getenv("RELIEFWEB_APPNAME", "MDA_OSINT_Tool")

# Maximum number of reports per poll
POLL_LIMIT = int(os.getenv("RELIEFWEB_POLL_LIMIT", "50"))

# Polling interval in seconds (default 10 minutes)
POLL_INTERVAL = int(os.getenv("RELIEFWEB_POLL_INTERVAL", "600"))

# ── Causal keyword patterns ─────────────────────────────────
# Used to extract lightweight causal relations from report body text.

CAUSAL_PATTERNS: List[re.Pattern] = [
    re.compile(
        r"(?P<cause>[A-Z][^.]{10,80}?)\s+"
        r"(?:led to|caused|resulted in|triggered|contributed to|exacerbated)\s+"
        r"(?P<effect>[^.]{10,120})",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:due to|because of|as a result of|following|in the wake of)\s+"
        r"(?P<cause>[^,]{10,120}),\s*"
        r"(?P<effect>[^.]{10,120})",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?P<cause>[A-Z][^.]{10,80}?)\s+"
        r"(?:has increased|has worsened|has displaced|has disrupted)\s+"
        r"(?P<effect>[^.]{10,120})",
        re.IGNORECASE,
    ),
]

# Event type keywords for classification
EVENT_TYPE_KEYWORDS: Dict[str, List[str]] = {
    "ARMED_CONFLICT": ["conflict", "fighting", "military", "armed", "attack", "battle"],
    "FOOD_INSECURITY": ["food", "hunger", "famine", "malnutrition", "ipc", "food security"],
    "DISPLACEMENT": ["displaced", "refugee", "idp", "migration", "flee", "evacuation"],
    "FLOOD": ["flood", "inundation", "deluge", "overflow"],
    "DROUGHT": ["drought", "dry spell", "water scarcity", "desertification"],
    "EPIDEMIC": ["cholera", "ebola", "measles", "epidemic", "outbreak", "pandemic"],
    "ECONOMIC_CRISIS": ["inflation", "economic", "currency", "poverty", "unemployment"],
    "SANCTIONS_IMPACT": ["sanctions", "embargo", "trade restriction"],
}

# ── Data classes ────────────────────────────────────────────


@dataclass
class ReliefWebReport:
    """Parsed metadata for a single ReliefWeb report."""

    report_id: str
    title: str
    url: str
    source: str
    date_published: str
    countries: List[str]
    country_isos: List[str]
    themes: List[str]
    body_text: Optional[str] = None


@dataclass
class ExtractedCausalRelation:
    """A causal relation extracted from report text."""

    cause_text: str
    effect_text: str
    cause_event_type: str
    effect_event_type: str
    confidence: float
    source_report_id: str


@dataclass
class PollResult:
    """Summary of a single polling cycle."""

    reports_fetched: int
    reports_processed: int
    events_upserted: int
    edges_created: int
    errors: List[str] = field(default_factory=list)
    polled_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ── ReliefWebIngestionWorker ────────────────────────────────


class ReliefWebIngestionWorker:
    """Asynchronous worker that polls the ReliefWeb API for recent
    humanitarian reports, extracts causal signals, and upserts them
    into Memgraph."""

    def __init__(
        self,
        mg: Optional[Memgraph] = None,
        poll_limit: int = POLL_LIMIT,
        poll_interval: int = POLL_INTERVAL,
    ):
        self.mg = mg or Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
        self.poll_limit = poll_limit
        self.poll_interval = poll_interval
        self._last_poll_time: Optional[str] = None

    # ── Deterministic IDs ────────────────────────────────────

    @staticmethod
    def _make_event_id(source: str, *parts: Any) -> str:
        payload = "|".join(str(p) for p in [source, *parts])
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]

    # ── API interaction ──────────────────────────────────────

    async def poll_recent_reports(
        self,
        session: aiohttp.ClientSession,
        since: Optional[str] = None,
    ) -> List[ReliefWebReport]:
        """Poll the ReliefWeb /reports endpoint for recent publications.

        Parameters
        ----------
        session : aiohttp.ClientSession
        since : str, optional
            ISO datetime string to filter reports after this date.

        Returns
        -------
        List[ReliefWebReport]
        """
        params: Dict[str, Any] = {
            "appname": RELIEFWEB_APPNAME,
            "limit": self.poll_limit,
            "fields[include][]": [
                "title", "url", "source", "date.original",
                "country", "theme", "body-html",
            ],
            "sort[]": "date.original:desc",
        }

        # Apply date filter
        filter_payload: Dict[str, Any] = {"operator": "AND", "conditions": []}
        if since:
            filter_payload["conditions"].append({
                "field": "date.original",
                "value": {"from": since},
            })

        url = f"{RELIEFWEB_API_BASE}/reports"

        post_body: Dict[str, Any] = {
            "limit": self.poll_limit,
            "fields": {
                "include": [
                    "title", "url", "source.name", "date.original",
                    "country.name", "country.iso3", "theme.name",
                ],
            },
            "sort": ["date.original:desc"],
        }
        if filter_payload["conditions"]:
            post_body["filter"] = filter_payload

        try:
            async with session.post(
                url,
                json=post_body,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError as exc:
            logger.error("ReliefWeb API poll failed: %s", exc)
            return []

        reports: List[ReliefWebReport] = []
        for item in data.get("data", []):
            fields = item.get("fields", {})
            report_id = str(item.get("id", ""))

            countries = []
            country_isos = []
            for c in fields.get("country", []):
                countries.append(c.get("name", ""))
                country_isos.append(c.get("iso3", ""))

            sources = []
            for s in fields.get("source", []):
                sources.append(s.get("name", ""))

            themes = [t.get("name", "") for t in fields.get("theme", [])]

            reports.append(
                ReliefWebReport(
                    report_id=report_id,
                    title=fields.get("title", ""),
                    url=fields.get("url", ""),
                    source=", ".join(sources) if sources else "ReliefWeb",
                    date_published=fields.get("date", {}).get("original", ""),
                    countries=countries,
                    country_isos=country_isos,
                    themes=themes,
                )
            )

        logger.info("Polled %d reports from ReliefWeb", len(reports))
        return reports

    async def fetch_report_body(
        self,
        session: aiohttp.ClientSession,
        report_id: str,
    ) -> Optional[str]:
        """Fetch the full body text for a single report.

        Parameters
        ----------
        session : aiohttp.ClientSession
        report_id : str

        Returns
        -------
        str or None
        """
        url = f"{RELIEFWEB_API_BASE}/reports/{report_id}"
        post_body = {
            "fields": {
                "include": ["body-html", "title"],
            },
        }

        try:
            async with session.post(
                url,
                json=post_body,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError as exc:
            logger.warning("Failed to fetch body for report %s: %s", report_id, exc)
            return None

        fields = data.get("data", [{}])
        if isinstance(fields, list) and fields:
            fields = fields[0].get("fields", {})
        elif isinstance(fields, dict):
            fields = fields.get("fields", {})

        body_html = fields.get("body-html", "")
        if not body_html:
            return None

        # Strip HTML tags for text extraction
        text = re.sub(r"<[^>]+>", " ", body_html)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    # ── Causal extraction from text ──────────────────────────

    def _classify_event_type(self, text: str) -> str:
        """Classify a text fragment into an event type based on keywords."""
        text_lower = text.lower()
        best_type = "UNKNOWN"
        best_count = 0

        for event_type, keywords in EVENT_TYPE_KEYWORDS.items():
            count = sum(1 for kw in keywords if kw in text_lower)
            if count > best_count:
                best_count = count
                best_type = event_type

        return best_type

    def process_report(
        self,
        report: ReliefWebReport,
    ) -> List[ExtractedCausalRelation]:
        """Extract causal relations from the body text of a report.

        Uses regex patterns to identify cause-effect pairs and classifies
        them by event type.
        """
        if not report.body_text:
            return []

        relations: List[ExtractedCausalRelation] = []
        seen_pairs: set = set()

        for pattern in CAUSAL_PATTERNS:
            for match in pattern.finditer(report.body_text):
                cause_text = match.group("cause").strip()
                effect_text = match.group("effect").strip()

                # Deduplicate
                pair_key = (cause_text[:50].lower(), effect_text[:50].lower())
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)

                cause_type = self._classify_event_type(cause_text)
                effect_type = self._classify_event_type(effect_text)

                # Confidence heuristic: higher if both sides are classified
                confidence = 0.5
                if cause_type != "UNKNOWN":
                    confidence += 0.15
                if effect_type != "UNKNOWN":
                    confidence += 0.15
                # Boost if report has known themes matching
                for theme in report.themes:
                    theme_lower = theme.lower()
                    if any(kw in theme_lower for kw in ("conflict", "food", "displaced")):
                        confidence = min(confidence + 0.05, 0.95)

                relations.append(
                    ExtractedCausalRelation(
                        cause_text=cause_text[:200],
                        effect_text=effect_text[:200],
                        cause_event_type=cause_type,
                        effect_event_type=effect_type,
                        confidence=round(confidence, 3),
                        source_report_id=report.report_id,
                    )
                )

        logger.debug(
            "Extracted %d causal relations from report %s",
            len(relations),
            report.report_id,
        )
        return relations

    # ── Memgraph persistence ─────────────────────────────────

    def _upsert_report_event(self, report: ReliefWebReport) -> str:
        """Upsert a CausalEvent node for the report itself, return its event_id."""
        event_id = self._make_event_id("reliefweb", report.report_id)

        primary_iso = report.country_isos[0] if report.country_isos else ""
        primary_country = report.countries[0] if report.countries else ""

        query = """
            MERGE (e:CausalEvent {event_id: $event_id})
            SET e.event_type           = $event_type,
                e.description          = $description,
                e.domain               = 'humanitarian',
                e.occurred_at          = $occurred_at,
                e.location_country_iso = $country_iso,
                e.location_region      = $region,
                e.confidence           = 0.7,
                e.causal_weight        = 0.5,
                e.source               = $source,
                e.source_dataset       = 'reliefweb',
                e.source_url           = $url,
                e.risk_score           = 5.0,
                e.status               = 'ACTIVE',
                e.ingested_at          = $ingested_at
        """
        params = {
            "event_id": event_id,
            "event_type": "HUMANITARIAN_REPORT",
            "description": report.title[:300],
            "occurred_at": report.date_published,
            "country_iso": primary_iso,
            "region": primary_country,
            "source": report.source,
            "url": report.url,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
        self.mg.execute_and_fetch(query, params)
        return event_id

    def _upsert_causal_edge(
        self,
        relation: ExtractedCausalRelation,
        report_event_id: str,
        report: ReliefWebReport,
    ) -> None:
        """Create cause and effect CausalEvent nodes plus a CAUSES edge."""
        primary_iso = report.country_isos[0] if report.country_isos else ""
        now = datetime.now(timezone.utc).isoformat()

        cause_id = self._make_event_id(
            "rw_cause",
            relation.source_report_id,
            relation.cause_text[:50],
        )
        effect_id = self._make_event_id(
            "rw_effect",
            relation.source_report_id,
            relation.effect_text[:50],
        )

        query = """
            MERGE (cause:CausalEvent {event_id: $cause_id})
            SET cause.event_type           = $cause_type,
                cause.description          = $cause_desc,
                cause.domain               = 'humanitarian',
                cause.location_country_iso = $country_iso,
                cause.confidence           = $confidence,
                cause.source               = 'ReliefWeb_extraction',
                cause.source_dataset       = 'reliefweb',
                cause.status               = 'INFERRED',
                cause.ingested_at          = $now

            MERGE (effect:CausalEvent {event_id: $effect_id})
            SET effect.event_type           = $effect_type,
                effect.description          = $effect_desc,
                effect.domain               = 'humanitarian',
                effect.location_country_iso = $country_iso,
                effect.confidence           = $confidence,
                effect.source               = 'ReliefWeb_extraction',
                effect.source_dataset       = 'reliefweb',
                effect.status               = 'INFERRED',
                effect.ingested_at          = $now

            MERGE (cause)-[r:CAUSES]->(effect)
            SET r.confidence        = $confidence,
                r.extraction_method = 'regex_nlp',
                r.source_report     = $report_id,
                r.updated_at        = $now

            WITH cause, effect
            MATCH (report:CausalEvent {event_id: $report_event_id})
            MERGE (report)-[:MENTIONS]->(cause)
            MERGE (report)-[:MENTIONS]->(effect)
        """

        params = {
            "cause_id": cause_id,
            "cause_type": relation.cause_event_type,
            "cause_desc": relation.cause_text,
            "effect_id": effect_id,
            "effect_type": relation.effect_event_type,
            "effect_desc": relation.effect_text,
            "country_iso": primary_iso,
            "confidence": relation.confidence,
            "report_id": relation.source_report_id,
            "report_event_id": report_event_id,
            "now": now,
        }
        self.mg.execute_and_fetch(query, params)

    # ── Main processing pipeline ─────────────────────────────

    async def _process_single_report(
        self,
        session: aiohttp.ClientSession,
        report: ReliefWebReport,
    ) -> Dict[str, int]:
        """Fetch body, extract relations, and persist for one report."""
        stats = {"events": 0, "edges": 0}

        # Fetch full body
        body = await self.fetch_report_body(session, report.report_id)
        report.body_text = body

        # Upsert report event node
        try:
            report_event_id = self._upsert_report_event(report)
            stats["events"] += 1
        except Exception as exc:
            logger.error("Failed to upsert report %s: %s", report.report_id, exc)
            return stats

        # Extract and persist causal relations
        if report.body_text:
            relations = self.process_report(report)
            for rel in relations:
                try:
                    self._upsert_causal_edge(rel, report_event_id, report)
                    stats["events"] += 2  # cause + effect nodes
                    stats["edges"] += 1
                except Exception as exc:
                    logger.warning(
                        "Failed to upsert causal edge from report %s: %s",
                        report.report_id,
                        exc,
                    )

        return stats

    async def _poll_cycle(self) -> PollResult:
        """Execute a single poll-and-process cycle."""
        errors: List[str] = []
        total_events = 0
        total_edges = 0

        async with aiohttp.ClientSession() as session:
            reports = await self.poll_recent_reports(
                session, since=self._last_poll_time
            )

            if reports:
                # Update last poll time to the newest report's date
                self._last_poll_time = reports[0].date_published

            processed = 0
            for report in reports:
                try:
                    stats = await self._process_single_report(session, report)
                    total_events += stats["events"]
                    total_edges += stats["edges"]
                    processed += 1
                except Exception as exc:
                    msg = f"Error processing report {report.report_id}: {exc}"
                    logger.error(msg)
                    errors.append(msg)

                # Small delay to be respectful to the API
                await asyncio.sleep(0.5)

        result = PollResult(
            reports_fetched=len(reports),
            reports_processed=processed,
            events_upserted=total_events,
            edges_created=total_edges,
            errors=errors,
        )

        logger.info(
            "Poll cycle: %d fetched, %d processed, %d events, %d edges, %d errors",
            result.reports_fetched,
            result.reports_processed,
            result.events_upserted,
            result.edges_created,
            len(result.errors),
        )
        return result

    # ── Continuous polling loop ───────────────────────────────

    async def run_continuous(self) -> None:
        """Run an indefinite polling loop, executing a poll cycle every
        ``poll_interval`` seconds.

        Catches all exceptions per cycle to ensure resilience.
        """
        logger.info(
            "Starting ReliefWeb continuous polling (interval=%ds, limit=%d)",
            self.poll_interval,
            self.poll_limit,
        )

        while True:
            try:
                result = await self._poll_cycle()
                if result.errors:
                    logger.warning(
                        "Poll cycle completed with %d errors", len(result.errors)
                    )
            except Exception as exc:
                logger.error("Unhandled error in poll cycle: %s", exc, exc_info=True)

            logger.debug("Sleeping %d seconds until next poll", self.poll_interval)
            await asyncio.sleep(self.poll_interval)
