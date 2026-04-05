"""Butterfly-effect engine — runs named Cypher path-traversal queries
across the Memgraph causal graph to discover multi-hop cascades with
cumulative confidence products.  Optionally synthesizes narrative
explanations via a local Ollama LLM endpoint.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from gqlalchemy import Memgraph

logger = logging.getLogger("mda.market_causal.butterfly")

MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")

# ── Named Cypher queries ────────────────────────────────────
# Each query uses variable-length path traversal with a confidence
# product computed via reduce().

BUTTERFLY_QUERIES: Dict[str, str] = {
    # 1. Generic downstream cascade from a trigger event
    "downstream_cascade": """
        MATCH path = (trigger:CausalEvent {event_id: $event_id})
              -[:CAUSES*1..6]->(downstream:CausalEvent)
        WITH path,
             downstream,
             reduce(conf = 1.0, r IN relationships(path) |
                    conf * coalesce(r.confidence, 0.5)) AS cascade_confidence
        WHERE cascade_confidence > 0.01
        RETURN trigger.event_id        AS trigger_id,
               trigger.event_type      AS trigger_type,
               downstream.event_id     AS downstream_id,
               downstream.event_type   AS downstream_type,
               downstream.description  AS downstream_desc,
               length(path)            AS hops,
               cascade_confidence
        ORDER BY cascade_confidence DESC
        LIMIT 50
    """,

    # 2. Type-based cascade: all events of a given type downstream
    "type_cascade": """
        MATCH path = (trigger:CausalEvent {event_type: $trigger_type})
              -[:CAUSES*1..5]->(downstream:CausalEvent {event_type: $target_type})
        WITH trigger, downstream, path,
             reduce(conf = 1.0, r IN relationships(path) |
                    conf * coalesce(r.confidence, 0.5)) AS cascade_confidence
        WHERE cascade_confidence > 0.02
        RETURN trigger.event_id     AS trigger_id,
               trigger.description  AS trigger_desc,
               downstream.event_id  AS downstream_id,
               downstream.description AS downstream_desc,
               length(path)         AS hops,
               cascade_confidence
        ORDER BY cascade_confidence DESC
        LIMIT 30
    """,

    # 3. Market-to-humanitarian cross-domain cascade
    "market_to_humanitarian": """
        MATCH path = (mi:MarketInstrument)
              -[:GRANGER_CAUSES*1..3]->(other:MarketInstrument)
              <-[:AFFECTS]-(evt:CausalEvent)
              -[:CAUSES*1..4]->(crisis:CausalEvent)
        WHERE crisis.domain = 'humanitarian'
        WITH mi, crisis, path,
             reduce(conf = 1.0, r IN relationships(path) |
                    conf * coalesce(r.confidence, 0.5)) AS cascade_confidence
        WHERE cascade_confidence > 0.005
        RETURN mi.ticker              AS source_instrument,
               mi.name                AS source_name,
               crisis.event_id        AS crisis_id,
               crisis.event_type      AS crisis_type,
               crisis.description     AS crisis_desc,
               crisis.location_country_iso AS crisis_country,
               length(path)           AS hops,
               cascade_confidence
        ORDER BY cascade_confidence DESC
        LIMIT 30
    """,

    # 4. Cartel-to-maritime cascade
    "cartel_to_maritime": """
        MATCH path = (cartel:Organization {org_type: 'CARTEL'})
              -[:OPERATES|CONTROLS*1..2]->(evt:CausalEvent)
              -[:CAUSES*1..4]->(maritime:CausalEvent)
        WHERE maritime.domain = 'maritime'
        WITH cartel, maritime, path,
             reduce(conf = 1.0, r IN relationships(path) |
                    conf * coalesce(r.confidence, 0.5)) AS cascade_confidence
        WHERE cascade_confidence > 0.01
        RETURN cartel.name            AS cartel_name,
               cartel.entity_id       AS cartel_id,
               maritime.event_id      AS maritime_event_id,
               maritime.event_type    AS maritime_event_type,
               maritime.description   AS maritime_desc,
               length(path)           AS hops,
               cascade_confidence
        ORDER BY cascade_confidence DESC
        LIMIT 30
    """,

    # 5. Panama Canal closure ripple effects
    "panama_canal_closure": """
        MATCH path = (canal:CausalEvent)
              -[:CAUSES*1..6]->(downstream:CausalEvent)
        WHERE canal.event_type = 'CANAL_DISRUPTION'
          AND canal.description CONTAINS 'Panama'
        WITH canal, downstream, path,
             reduce(conf = 1.0, r IN relationships(path) |
                    conf * coalesce(r.confidence, 0.5)) AS cascade_confidence
        WHERE cascade_confidence > 0.005
        RETURN canal.event_id          AS canal_event_id,
               canal.occurred_at       AS canal_date,
               downstream.event_id     AS downstream_id,
               downstream.event_type   AS downstream_type,
               downstream.domain       AS downstream_domain,
               downstream.description  AS downstream_desc,
               downstream.location_country_iso AS country,
               length(path)            AS hops,
               cascade_confidence
        ORDER BY cascade_confidence DESC
        LIMIT 50
    """,

    # 6. Wheat price spike to humanitarian crisis
    "wheat_spike_humanitarian": """
        MATCH path = (wheat:MarketInstrument {ticker: 'ZW=F'})
              <-[:AFFECTS]-(price_evt:CausalEvent {event_type: 'PRICE_ANOMALY'})
              -[:CAUSES*1..5]->(crisis:CausalEvent)
        WHERE crisis.domain = 'humanitarian'
          AND crisis.event_type IN ['FOOD_INSECURITY', 'FAMINE', 'DISPLACEMENT']
        WITH wheat, price_evt, crisis, path,
             reduce(conf = 1.0, r IN relationships(path) |
                    conf * coalesce(r.confidence, 0.5)) AS cascade_confidence
        WHERE cascade_confidence > 0.005
        RETURN wheat.ticker             AS instrument,
               price_evt.event_id       AS price_event_id,
               price_evt.occurred_at    AS price_date,
               crisis.event_id          AS crisis_id,
               crisis.event_type        AS crisis_type,
               crisis.description       AS crisis_desc,
               crisis.location_country_iso AS country,
               length(path)             AS hops,
               cascade_confidence
        ORDER BY cascade_confidence DESC
        LIMIT 30
    """,

    # 7. Sanctions cascade — from OFAC designation to downstream effects
    "sanctions_cascade": """
        MATCH path = (sanction:CausalEvent {event_type: 'SANCTIONS_DESIGNATION'})
              -[:CAUSES*1..6]->(downstream:CausalEvent)
        WITH sanction, downstream, path,
             reduce(conf = 1.0, r IN relationships(path) |
                    conf * coalesce(r.confidence, 0.5)) AS cascade_confidence
        WHERE cascade_confidence > 0.01
        RETURN sanction.event_id       AS sanction_id,
               sanction.description    AS sanction_desc,
               downstream.event_id     AS downstream_id,
               downstream.event_type   AS downstream_type,
               downstream.domain       AS downstream_domain,
               downstream.description  AS downstream_desc,
               length(path)            AS hops,
               cascade_confidence
        ORDER BY cascade_confidence DESC
        LIMIT 40
    """,

    # 8. Social media signals leading to physical events
    "social_media_to_physical": """
        MATCH path = (social:CausalEvent {domain: 'social_media'})
              -[:CAUSES*1..4]->(physical:CausalEvent)
        WHERE physical.domain IN ['maritime', 'security', 'humanitarian']
        WITH social, physical, path,
             reduce(conf = 1.0, r IN relationships(path) |
                    conf * coalesce(r.confidence, 0.5)) AS cascade_confidence
        WHERE cascade_confidence > 0.01
        RETURN social.event_id        AS social_event_id,
               social.event_type      AS social_type,
               social.description     AS social_desc,
               physical.event_id      AS physical_event_id,
               physical.event_type    AS physical_type,
               physical.domain        AS physical_domain,
               physical.description   AS physical_desc,
               length(path)           AS hops,
               cascade_confidence
        ORDER BY cascade_confidence DESC
        LIMIT 30
    """,

    # 9. UAS drug delivery chain — from detection to interdiction
    "uas_drug_delivery_chain": """
        MATCH path = (uas:UAS_Detection_Event)
              -[:LINKED_TO|ASSOCIATED_WITH*1..2]->(evt:CausalEvent)
              -[:CAUSES*1..4]->(outcome:CausalEvent)
        WHERE outcome.event_type IN ['CARTEL_INTERDICTION', 'MARITIME_SEIZURE',
                                      'DRUG_SEIZURE', 'BORDER_INCIDENT']
        WITH uas, outcome, path,
             reduce(conf = 1.0, r IN relationships(path) |
                    conf * coalesce(r.confidence, 0.5)) AS cascade_confidence
        WHERE cascade_confidence > 0.005
        RETURN uas.drone_id            AS drone_id,
               uas.detection_timestamp AS detection_time,
               outcome.event_id        AS outcome_id,
               outcome.event_type      AS outcome_type,
               outcome.description     AS outcome_desc,
               length(path)            AS hops,
               cascade_confidence
        ORDER BY cascade_confidence DESC
        LIMIT 30
    """,

    # 10. Cartel revenue feedback — interdictions affecting finances
    "cartel_revenue_feedback": """
        MATCH path = (interdiction:CausalEvent {event_type: 'CARTEL_INTERDICTION'})
              -[:CAUSES*1..5]->(financial:CausalEvent)
              -[:AFFECTS]->(mi:MarketInstrument)
        WHERE financial.domain = 'financial'
        WITH interdiction, financial, mi, path,
             reduce(conf = 1.0, r IN relationships(path) |
                    conf * coalesce(r.confidence, 0.5)) AS cascade_confidence
        WHERE cascade_confidence > 0.005
        RETURN interdiction.event_id    AS interdiction_id,
               interdiction.description AS interdiction_desc,
               financial.event_id       AS financial_id,
               financial.event_type     AS financial_type,
               financial.description    AS financial_desc,
               mi.ticker                AS affected_instrument,
               mi.name                  AS instrument_name,
               length(path)             AS hops,
               cascade_confidence
        ORDER BY cascade_confidence DESC
        LIMIT 30
    """,
}

# ── Data classes ────────────────────────────────────────────


@dataclass
class ButterflyResult:
    """Result of a single butterfly-effect query execution."""

    query_name: str
    params: Dict[str, Any]
    rows: List[Dict[str, Any]]
    row_count: int
    executed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    synthesis: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "query_name": self.query_name,
            "params": self.params,
            "row_count": self.row_count,
            "executed_at": self.executed_at.isoformat(),
            "rows": self.rows,
            "synthesis": self.synthesis,
        }


# ── ButterflyEffectEngine ──────────────────────────────────


class ButterflyEffectEngine:
    """Executes named multi-hop Cypher cascade queries against the Memgraph
    causal graph, with optional LLM narrative synthesis via Ollama."""

    def __init__(
        self,
        mg: Optional[Memgraph] = None,
        ollama_url: Optional[str] = None,
        ollama_model: Optional[str] = None,
    ):
        self.mg = mg or Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
        self.ollama_url = ollama_url or OLLAMA_URL
        self.ollama_model = ollama_model or OLLAMA_MODEL

    @property
    def available_queries(self) -> List[str]:
        """Return names of all registered butterfly queries."""
        return list(BUTTERFLY_QUERIES.keys())

    def run_butterfly(
        self,
        query_name: str,
        params: Optional[Dict[str, Any]] = None,
        synthesize: bool = False,
    ) -> ButterflyResult:
        """Execute a named butterfly-effect query and optionally synthesize
        a narrative explanation via Ollama.

        Parameters
        ----------
        query_name : str
            One of the 10 named queries in BUTTERFLY_QUERIES.
        params : dict, optional
            Cypher query parameters (e.g. ``{"event_id": "abc123"}``).
        synthesize : bool
            If True, send the query results to Ollama for narrative synthesis.

        Returns
        -------
        ButterflyResult
        """
        if query_name not in BUTTERFLY_QUERIES:
            raise ValueError(
                f"Unknown query '{query_name}'. "
                f"Available: {', '.join(BUTTERFLY_QUERIES.keys())}"
            )

        cypher = BUTTERFLY_QUERIES[query_name]
        params = params or {}

        logger.info("Running butterfly query '%s' with params %s", query_name, params)

        try:
            raw_rows = list(self.mg.execute_and_fetch(cypher, params))
        except Exception as exc:
            logger.error("Butterfly query '%s' failed: %s", query_name, exc)
            raise

        # Convert Memgraph row dicts to plain dicts with serializable values
        rows: List[Dict[str, Any]] = []
        for raw in raw_rows:
            row: Dict[str, Any] = {}
            for key, val in raw.items():
                if isinstance(val, datetime):
                    row[key] = val.isoformat()
                elif isinstance(val, (int, float, str, bool, type(None))):
                    row[key] = val
                else:
                    row[key] = str(val)
            rows.append(row)

        logger.info(
            "Butterfly query '%s' returned %d rows", query_name, len(rows)
        )

        result = ButterflyResult(
            query_name=query_name,
            params=params,
            rows=rows,
            row_count=len(rows),
        )

        if synthesize and rows:
            result.synthesis = self._synthesize_narrative(query_name, params, rows)

        return result

    # ── Batch execution ──────────────────────────────────────

    def run_all_queries(
        self,
        params_map: Optional[Dict[str, Dict[str, Any]]] = None,
        synthesize: bool = False,
    ) -> List[ButterflyResult]:
        """Run all butterfly queries with optional per-query parameters.

        Parameters
        ----------
        params_map : dict, optional
            Mapping of query_name -> params dict.  Queries not in the map
            are run with empty params (and may return nothing if they need
            parameters).
        synthesize : bool
            Whether to synthesize narratives for each result set.
        """
        params_map = params_map or {}
        results: List[ButterflyResult] = []

        for query_name in BUTTERFLY_QUERIES:
            params = params_map.get(query_name, {})
            try:
                result = self.run_butterfly(query_name, params, synthesize=synthesize)
                results.append(result)
            except Exception as exc:
                logger.error("Skipping '%s' due to error: %s", query_name, exc)

        return results

    # ── Ollama synthesis ─────────────────────────────────────

    def _synthesize_narrative(
        self,
        query_name: str,
        params: Dict[str, Any],
        rows: List[Dict[str, Any]],
    ) -> Optional[str]:
        """Send cascade results to a local Ollama model for narrative
        synthesis.  Returns the generated text or None on failure."""
        # Truncate to top 15 rows for the prompt
        truncated = rows[:15]
        rows_json = json.dumps(truncated, indent=2, default=str)

        prompt = (
            f"You are an intelligence analyst examining causal cascade data.\n"
            f"Query: {query_name}\n"
            f"Parameters: {json.dumps(params, default=str)}\n"
            f"Results ({len(rows)} total rows, showing top {len(truncated)}):\n"
            f"```json\n{rows_json}\n```\n\n"
            f"Provide a concise 2-3 paragraph analyst narrative explaining:\n"
            f"1. The primary causal chain observed.\n"
            f"2. Key intermediate nodes and their confidence levels.\n"
            f"3. Operational implications and recommended watch items.\n"
            f"Keep it under 300 words.  Use intelligence-community style."
        )

        try:
            resp = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": self.ollama_model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.3,
                        "num_predict": 512,
                    },
                },
                timeout=120,
            )
            resp.raise_for_status()
            body = resp.json()
            narrative = body.get("response", "").strip()
            logger.info(
                "Ollama synthesis for '%s': %d chars", query_name, len(narrative)
            )
            return narrative
        except requests.RequestException as exc:
            logger.warning(
                "Ollama synthesis failed for '%s': %s", query_name, exc
            )
            return None
        except (ValueError, KeyError) as exc:
            logger.warning(
                "Ollama response parsing failed for '%s': %s", query_name, exc
            )
            return None
