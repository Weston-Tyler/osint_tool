"""OBI Seed Extractor for WorldFish simulation engine.

Queries a live Memgraph instance to extract the N-hop causal neighborhood
of a trigger event, producing a SimulationSeed that captures the local
subgraph needed to initialize a scenario simulation.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from gqlalchemy import Memgraph

logger = logging.getLogger("worldfish.seed_extractor")

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class SimulationSeed:
    """Complete causal neighbourhood snapshot used to initialise a simulation."""

    seed_id: str
    trigger_event_id: str
    trigger_event_type: str
    trigger_risk_score: float
    triggered_at: datetime
    trigger_mode: str  # "automatic" | "analyst_initiated"
    seed_events: list[dict[str, Any]] = field(default_factory=list)
    seed_entities: list[dict[str, Any]] = field(default_factory=list)
    seed_causal_edges: list[dict[str, Any]] = field(default_factory=list)
    seed_patterns: list[dict[str, Any]] = field(default_factory=list)
    recommended_domain: str = "maritime"
    recommended_agents: list[str] = field(default_factory=list)
    scenario_description: str = ""


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------


class OBISeedExtractor:
    """Extract causal neighbourhood from the OBI knowledge graph in Memgraph.

    The extractor issues Cypher queries that walk outward from a trigger event
    along ``DIRECTLY_CAUSES``, ``ENABLES``, and ``INHIBITS`` edges up to
    *n_hops* away, collecting every event, entity, edge, and pattern touched.

    Parameters
    ----------
    host : str
        Memgraph hostname.
    port : int
        Memgraph Bolt port (default 7687).
    n_hops : int
        Maximum traversal depth (default 3).
    """

    _CAUSAL_REL_TYPES = "DIRECTLY_CAUSES|ENABLES|INHIBITS"

    def __init__(
        self,
        host: str = "localhost",
        port: int = 7687,
        n_hops: int = 3,
    ) -> None:
        self._host = host
        self._port = port
        self._n_hops = n_hops
        self._db = Memgraph(host=host, port=port)
        logger.info(
            "OBISeedExtractor connected to Memgraph at %s:%s (n_hops=%d)",
            host,
            port,
            n_hops,
        )

    # ------------------------------------------------------------------
    # Causal neighbourhood query
    # ------------------------------------------------------------------

    def extract_causal_neighborhood(
        self,
        trigger_event_id: str,
        n_hops: int | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Return the N-hop causal neighbourhood of *trigger_event_id*.

        Returns a dict with keys ``events``, ``entities``, ``causal_edges``,
        and ``patterns``.
        """
        hops = n_hops if n_hops is not None else self._n_hops

        # ----- events reachable via causal edges (both directions) -----
        events_query = f"""
            MATCH path = (trigger:Event {{event_id: $trigger_id}})
                  -[:{self._CAUSAL_REL_TYPES} *1..{hops}]-
                  (related:Event)
            WITH DISTINCT related
            RETURN related {{
                .event_id,
                .event_type,
                .description,
                .risk_score,
                .occurred_at,
                .latitude,
                .longitude,
                .region,
                .source
            }} AS event
        """

        # ----- entities linked to those events -----
        entities_query = f"""
            MATCH (trigger:Event {{event_id: $trigger_id}})
                  -[:{self._CAUSAL_REL_TYPES} *0..{hops}]-
                  (evt:Event)
                  -[:INVOLVES|OBSERVED_AT|ATTRIBUTED_TO]-
                  (ent)
            WITH DISTINCT ent
            RETURN ent {{
                .object_id,
                .object_type,
                .name,
                .description,
                .risk_score,
                .country,
                .faction,
                .role
            }} AS entity
        """

        # ----- causal edges themselves -----
        edges_query = f"""
            MATCH (trigger:Event {{event_id: $trigger_id}})
                  -[:{self._CAUSAL_REL_TYPES} *0..{hops}]-
                  (a:Event)
                  -[r:{self._CAUSAL_REL_TYPES}]-
                  (b:Event)
            WITH DISTINCT r, a, b
            RETURN {{
                source_event_id: a.event_id,
                target_event_id: b.event_id,
                relationship_type: type(r),
                confidence: r.confidence,
                lag_hours: r.lag_hours,
                evidence: r.evidence
            }} AS edge
        """

        # ----- patterns (temporal / spatial clusters) -----
        patterns_query = f"""
            MATCH (trigger:Event {{event_id: $trigger_id}})
                  -[:{self._CAUSAL_REL_TYPES} *0..{hops}]-
                  (evt:Event)
                  -[:MEMBER_OF]-
                  (pat:Pattern)
            WITH DISTINCT pat
            RETURN pat {{
                .pattern_id,
                .pattern_type,
                .description,
                .confidence,
                .region,
                .first_seen,
                .last_seen
            }} AS pattern
        """

        params = {"trigger_id": trigger_event_id}

        events = self._execute_query(events_query, params, "event")
        entities = self._execute_query(entities_query, params, "entity")
        causal_edges = self._execute_query(edges_query, params, "edge")
        patterns = self._execute_query(patterns_query, params, "pattern")

        logger.info(
            "Neighbourhood for %s: %d events, %d entities, %d edges, %d patterns",
            trigger_event_id,
            len(events),
            len(entities),
            len(causal_edges),
            len(patterns),
        )

        return {
            "events": events,
            "entities": entities,
            "causal_edges": causal_edges,
            "patterns": patterns,
        }

    # ------------------------------------------------------------------
    # Build a SimulationSeed
    # ------------------------------------------------------------------

    def build_seed(
        self,
        trigger_event_id: str,
        trigger_mode: str = "automatic",
        n_hops: int | None = None,
    ) -> SimulationSeed:
        """Build a complete :class:`SimulationSeed` for the given trigger.

        This fetches the trigger event node itself, extracts its causal
        neighbourhood, infers domain and recommended agents, and assembles
        everything into a single seed object.
        """
        # Fetch trigger event metadata
        trigger_result = self._db.execute_and_fetch(
            """
            MATCH (e:Event {event_id: $trigger_id})
            RETURN e {
                .event_id,
                .event_type,
                .description,
                .risk_score,
                .occurred_at,
                .region,
                .source
            } AS event
            """,
            {"trigger_id": trigger_event_id},
        )

        trigger_rows = list(trigger_result)
        if not trigger_rows:
            raise ValueError(
                f"Trigger event '{trigger_event_id}' not found in Memgraph"
            )

        trigger_event = trigger_rows[0]["event"]

        # Extract neighbourhood
        neighbourhood = self.extract_causal_neighborhood(
            trigger_event_id, n_hops=n_hops
        )

        # Infer domain from event types
        domain = self._infer_domain(
            trigger_event, neighbourhood["events"]
        )

        # Infer recommended agent types
        recommended_agents = self._infer_agents(
            domain, neighbourhood["entities"]
        )

        # Build scenario description from trigger + patterns
        scenario_description = self._build_scenario_description(
            trigger_event, neighbourhood
        )

        occurred_at_raw = trigger_event.get("occurred_at")
        if isinstance(occurred_at_raw, datetime):
            triggered_at = occurred_at_raw
        elif isinstance(occurred_at_raw, str):
            triggered_at = datetime.fromisoformat(occurred_at_raw)
        else:
            triggered_at = datetime.utcnow()

        return SimulationSeed(
            seed_id=str(uuid.uuid4()),
            trigger_event_id=trigger_event_id,
            trigger_event_type=trigger_event.get("event_type", "UNKNOWN"),
            trigger_risk_score=float(trigger_event.get("risk_score", 0.0)),
            triggered_at=triggered_at,
            trigger_mode=trigger_mode,
            seed_events=neighbourhood["events"],
            seed_entities=neighbourhood["entities"],
            seed_causal_edges=neighbourhood["causal_edges"],
            seed_patterns=neighbourhood["patterns"],
            recommended_domain=domain,
            recommended_agents=recommended_agents,
            scenario_description=scenario_description,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _execute_query(
        self,
        query: str,
        params: dict[str, Any],
        result_key: str,
    ) -> list[dict[str, Any]]:
        """Execute a Cypher query and collect the named result column."""
        try:
            rows = self._db.execute_and_fetch(query, params)
            return [row[result_key] for row in rows if row.get(result_key)]
        except Exception:
            logger.exception("Cypher query failed (key=%s)", result_key)
            return []

    @staticmethod
    def _infer_domain(
        trigger_event: dict[str, Any],
        related_events: list[dict[str, Any]],
    ) -> str:
        """Heuristically choose a simulation domain."""
        maritime_keywords = {
            "AIS_DISABLE",
            "STS_TRANSFER",
            "DARK_VOYAGE",
            "PORT_CALL",
            "MARITIME_TRANSIT",
            "VESSEL_RENDEZVOUS",
            "FISHING_VIOLATION",
        }
        territorial_keywords = {
            "TERRITORIAL_EXPANSION",
            "CHECKPOINT_SEIZURE",
            "TERRITORY_DISPUTE",
            "ARMED_CLASH",
            "EXTORTION",
        }

        all_types = {trigger_event.get("event_type", "")}
        for evt in related_events:
            all_types.add(evt.get("event_type", ""))

        maritime_hits = len(all_types & maritime_keywords)
        territorial_hits = len(all_types & territorial_keywords)

        if maritime_hits >= territorial_hits:
            return "maritime"
        return "territorial"

    @staticmethod
    def _infer_agents(
        domain: str,
        entities: list[dict[str, Any]],
    ) -> list[str]:
        """Suggest agent types based on domain and entities present."""
        agent_types: set[str] = set()

        for ent in entities:
            obj_type = (ent.get("object_type") or "").upper()
            faction = (ent.get("faction") or "").upper()
            role = (ent.get("role") or "").upper()

            if obj_type in ("VESSEL", "FLEET"):
                agent_types.add("maritime_operator")
            if obj_type in ("ORGANIZATION", "CARTEL"):
                agent_types.add("cartel_faction")
            if "DEA" in faction or "DEA" in role:
                agent_types.add("enforcement_dea")
            if "SEDENA" in faction or "MILITARY" in role:
                agent_types.add("enforcement_sedena")
            if "USCG" in faction or "COAST_GUARD" in role:
                agent_types.add("enforcement_uscg")
            if obj_type == "PERSON" and "CIVILIAN" in role:
                agent_types.add("civilian")

        # Ensure at least one adversarial and one enforcement agent
        if domain == "maritime":
            agent_types.add("smuggling_network")
            agent_types.add("enforcement_uscg")
        elif domain == "territorial":
            agent_types.add("cartel_faction")
            agent_types.add("enforcement_sedena")

        return sorted(agent_types)

    @staticmethod
    def _build_scenario_description(
        trigger_event: dict[str, Any],
        neighbourhood: dict[str, list[dict[str, Any]]],
    ) -> str:
        """Generate a human-readable scenario description."""
        trigger_type = trigger_event.get("event_type", "UNKNOWN")
        trigger_desc = trigger_event.get("description", "No description.")
        region = trigger_event.get("region", "unspecified region")
        n_events = len(neighbourhood["events"])
        n_entities = len(neighbourhood["entities"])
        n_patterns = len(neighbourhood["patterns"])

        pattern_summaries = []
        for pat in neighbourhood["patterns"][:5]:
            pat_desc = pat.get("description", pat.get("pattern_type", ""))
            if pat_desc:
                pattern_summaries.append(pat_desc)

        pattern_text = ""
        if pattern_summaries:
            pattern_text = (
                " Identified patterns include: "
                + "; ".join(pattern_summaries)
                + "."
            )

        return (
            f"Trigger event [{trigger_type}] in {region}: {trigger_desc} "
            f"The causal neighbourhood contains {n_events} related events "
            f"involving {n_entities} entities across {n_patterns} recognized "
            f"patterns.{pattern_text}"
        )
