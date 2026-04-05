"""Causal Chain Report — structured intelligence product.

Traces causal predecessors and successors of high-risk events,
producing structured reports for analyst consumption.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

import ollama
from gqlalchemy import Memgraph

logger = logging.getLogger("mda.intel_products.chain_report")


@dataclass
class DownstreamEffect:
    event_type: str
    region: str
    probability: float
    min_days: int
    max_days: int
    causal_distance: int


@dataclass
class CausalChainReport:
    report_id: str
    trigger_event_id: str
    trigger_event_type: str
    trigger_region: str
    trigger_occurred_at: str
    downstream_effects: list[DownstreamEffect]
    supporting_instances: int
    narrative_summary: str
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_obi_assertion(self) -> dict:
        return {
            "assertion_type": "CausalChainReport",
            "assertion_id": self.report_id,
            "confidence": max((e.probability for e in self.downstream_effects), default=0.0),
            "generated_at": self.generated_at,
            "payload": {
                "trigger": {
                    "event_id": self.trigger_event_id,
                    "event_type": self.trigger_event_type,
                    "region": self.trigger_region,
                    "occurred_at": self.trigger_occurred_at,
                },
                "downstream_effects": [asdict(e) for e in sorted(
                    self.downstream_effects, key=lambda x: x.probability, reverse=True
                )],
                "supporting_instances": self.supporting_instances,
                "narrative": self.narrative_summary,
            },
        }

    def to_markdown(self) -> str:
        lines = [
            f"# Causal Chain Report",
            f"**Generated:** {self.generated_at}",
            f"**Trigger:** {self.trigger_event_type} — {self.trigger_region} ({self.trigger_occurred_at[:10]})",
            f"**Historical Precedents:** {self.supporting_instances}",
            "",
            "## Predicted Downstream Effects",
            "",
            "| Event Type | Region | Probability | Timeframe | Hops |",
            "|---|---|---|---|---|",
        ]
        for e in sorted(self.downstream_effects, key=lambda x: x.probability, reverse=True):
            lines.append(
                f"| {e.event_type} | {e.region} | {e.probability:.0%} | "
                f"{e.min_days}-{e.max_days}d | {e.causal_distance} |"
            )
        lines.extend(["", "## Summary", "", self.narrative_summary])
        return "\n".join(lines)


class CausalChainReportGenerator:
    """Generates CausalChainReport from Memgraph causal graph."""

    def __init__(self):
        self.mg = Memgraph(
            host=os.getenv("MEMGRAPH_HOST", "localhost"),
            port=int(os.getenv("MEMGRAPH_PORT", "7687")),
        )
        self.ollama_client = ollama.Client(host=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"))
        self.model = os.getenv("OLLAMA_LLM_MODEL", "qwen2.5:7b")

    def generate_report(self, trigger_event_id: str) -> Optional[CausalChainReport]:
        """Generate a full causal chain report for a trigger event."""
        import uuid

        # Fetch trigger event
        trigger_results = list(self.mg.execute_and_fetch(
            "MATCH (e:CausalEvent {event_id: $eid}) RETURN e",
            {"eid": trigger_event_id},
        ))
        if not trigger_results:
            logger.warning("Trigger event %s not found", trigger_event_id)
            return None

        trigger = dict(trigger_results[0]["e"])

        # Trace downstream cascade (up to 4 hops)
        downstream = list(self.mg.execute_and_fetch("""
            MATCH path = (trigger:CausalEvent {event_id: $eid})
                         -[:DIRECTLY_CAUSES|ENABLES|GRANGER_CAUSES*1..4]->
                         (downstream:CausalEvent)
            RETURN downstream.event_type AS event_type,
                   downstream.location_region AS region,
                   length(path) AS hops,
                   reduce(c = 1.0, r IN relationships(path) | c * coalesce(r.confidence, 0.5)) AS chain_conf
            ORDER BY chain_conf DESC
            LIMIT 20
        """, {"eid": trigger_event_id}))

        effects = []
        for r in downstream:
            conf = float(r.get("chain_conf", 0.5))
            hops = int(r.get("hops", 1))
            effects.append(DownstreamEffect(
                event_type=r.get("event_type", "UNKNOWN"),
                region=r.get("region", ""),
                probability=round(conf, 3),
                min_days=hops * 7,
                max_days=hops * 45,
                causal_distance=hops,
            ))

        # Generate narrative with LLM
        narrative = self._synthesize_narrative(trigger, effects)

        return CausalChainReport(
            report_id=str(uuid.uuid4()),
            trigger_event_id=trigger_event_id,
            trigger_event_type=trigger.get("event_type", "UNKNOWN"),
            trigger_region=trigger.get("location_region", ""),
            trigger_occurred_at=str(trigger.get("occurred_at", "")),
            downstream_effects=effects,
            supporting_instances=sum(1 for e in effects if e.probability > 0.5),
            narrative_summary=narrative,
        )

    def _synthesize_narrative(self, trigger: dict, effects: list[DownstreamEffect]) -> str:
        effects_str = "\n".join(
            f"- {e.event_type} in {e.region} ({e.probability:.0%}, {e.min_days}-{e.max_days}d, {e.causal_distance} hops)"
            for e in effects[:10]
        )
        prompt = f"""You are a senior intelligence analyst. Write a 2-paragraph assessment of this causal chain.

TRIGGER: {trigger.get('event_type', '')} in {trigger.get('location_region', '')}
Description: {trigger.get('description', '')}

PREDICTED DOWNSTREAM EFFECTS:
{effects_str}

Paragraph 1: What happened and what it predicts.
Paragraph 2: Top 3 monitoring indicators analysts should watch."""

        try:
            resp = self.ollama_client.generate(
                model=self.model,
                prompt=prompt,
                options={"temperature": 0.3, "num_predict": 400},
            )
            return resp["response"].strip()
        except Exception as e:
            logger.error("LLM synthesis failed: %s", e)
            return f"Automated narrative generation failed. {len(effects)} downstream effects identified."
