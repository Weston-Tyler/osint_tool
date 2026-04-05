"""Causal Graph Learner — self-improving background process.

Continuously improves the causal model through:
1. Backward validation: boost edges whose predictions came true
2. Contradiction detection: penalize edges when expected effects don't occur
3. Pattern aging: decay confidence of stale patterns
4. Novel pattern emergence: detect new recurring sequences

Runs on a configurable interval (default 6 hours).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import ollama
from gqlalchemy import Memgraph

logger = logging.getLogger("mda.causal_graph_learner")

MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))
OLLAMA_HOST = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_LLM_MODEL", "llama3.2:3b")

VALIDATION_BOOST = 0.05
CONTRADICTION_PENALTY = 0.08
CONFIDENCE_DECAY_RECENT = 0.99   # < 30 days
CONFIDENCE_DECAY_MODERATE = 0.97 # 30-90 days
CONFIDENCE_DECAY_STALE = 0.94   # > 180 days


@dataclass
class ContradictionRecord:
    event_type_a: str
    event_type_b: str
    region: str
    instance_id: str
    contextual_variables: dict = field(default_factory=dict)


@dataclass
class EmergingPattern:
    pattern_id: str
    trigger_event_type: str
    chain_steps: list[str]
    observed_count: int
    first_seen: str
    last_seen: str
    avg_duration_days: float
    confidence: float
    proposed_name: str


class CausalGraphLearner:
    """Background process that continuously improves the causal model."""

    def __init__(self):
        self.mg = Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
        self.ollama_client = ollama.Client(host=OLLAMA_HOST)

    def run_backward_validation(self) -> int:
        """Find predictions confirmed by real events. Boost causal edges."""
        results = list(self.mg.execute_and_fetch("""
            MATCH (pred:CausalPrediction)
            WHERE pred.validated IS NULL OR pred.validated = false
            WITH pred
            MATCH (actual:CausalEvent)
            WHERE actual.event_type = pred.predicted_event_type
              AND actual.location_region = pred.predicted_location_region
            RETURN pred.prediction_id AS pred_id,
                   actual.event_id AS actual_id,
                   pred.trigger_event_id AS trigger_id
            LIMIT 100
        """))

        count = 0
        for r in results:
            # Boost the causal chain
            self.mg.execute("""
                MATCH (trigger:CausalEvent {event_id: $trigger_id})
                      -[r:DIRECTLY_CAUSES|ENABLES|GRANGER_CAUSES]-()
                SET r.confidence = CASE
                    WHEN r.confidence + $boost > 0.99 THEN 0.99
                    ELSE r.confidence + $boost
                END,
                r.supporting_instances = coalesce(r.supporting_instances, 0) + 1
            """, {"trigger_id": r["trigger_id"], "boost": VALIDATION_BOOST})

            # Mark prediction validated
            self.mg.execute("""
                MATCH (p:CausalPrediction {prediction_id: $pid})
                SET p.validated = true,
                    p.validated_at = localDateTime(),
                    p.validated_by_event = $actual_id
            """, {"pid": r["pred_id"], "actual_id": r["actual_id"]})
            count += 1
            logger.info("Validated prediction %s -> actual %s", r["pred_id"], r["actual_id"])

        return count

    def detect_contradictions(self) -> list[ContradictionRecord]:
        """Find A that occurred but expected B did not follow. Penalize edges."""
        results = list(self.mg.execute_and_fetch("""
            MATCH (src:CausalEventType)-[r:DIRECTLY_CAUSES]->(tgt:CausalEventType)
            WHERE r.supporting_instances >= 3 AND r.confidence > 0.40
            WITH src, tgt, r
            MATCH (actual_a:CausalEvent {event_type: src.event_type})
            WHERE NOT EXISTS {
                MATCH (actual_b:CausalEvent {event_type: tgt.event_type})
                WHERE actual_b.location_region = actual_a.location_region
            }
            RETURN src.event_type AS type_a, tgt.event_type AS type_b,
                   actual_a.event_id AS instance_id,
                   actual_a.location_region AS region,
                   r.confidence AS current_conf
            LIMIT 50
        """))

        contradictions = []
        for r in results:
            # Penalize the edge
            self.mg.execute("""
                MATCH (:CausalEventType {event_type: $ta})-[r:DIRECTLY_CAUSES]->
                      (:CausalEventType {event_type: $tb})
                SET r.confidence = CASE
                    WHEN r.confidence - $penalty < 0.01 THEN 0.01
                    ELSE r.confidence - $penalty
                END,
                r.contradiction_count = coalesce(r.contradiction_count, 0) + 1
            """, {"ta": r["type_a"], "tb": r["type_b"], "penalty": CONTRADICTION_PENALTY})

            contradictions.append(ContradictionRecord(
                event_type_a=r["type_a"],
                event_type_b=r["type_b"],
                region=r.get("region", ""),
                instance_id=r["instance_id"],
            ))

        return contradictions

    def apply_pattern_aging(self) -> int:
        """Decay confidence of CausalPatterns not recently observed."""
        result = list(self.mg.execute_and_fetch("""
            MATCH (p:CausalPattern)
            WHERE p.last_observed_instance IS NOT NULL
            SET p.confidence = CASE
                WHEN p.confidence * 0.97 < 0.05 THEN 0.05
                ELSE p.confidence * 0.97
            END,
            p.aged_at = localDateTime()
            RETURN count(p) AS aged
        """))
        return result[0]["aged"] if result else 0

    def detect_emerging_patterns(self, min_occurrences: int = 5) -> list[EmergingPattern]:
        """Detect new recurring event sequences not yet in the pattern library."""
        results = list(self.mg.execute_and_fetch("""
            MATCH (a:CausalEvent)-[:DIRECTLY_CAUSES|ENABLES]->(b:CausalEvent)
                  -[:DIRECTLY_CAUSES|ENABLES]->(c:CausalEvent)
            WHERE a.confidence >= 0.5 AND b.confidence >= 0.5 AND c.confidence >= 0.5
            WITH a.event_type AS step1, b.event_type AS step2, c.event_type AS step3,
                 count(*) AS frequency
            WHERE frequency >= $min_occ
            RETURN step1, step2, step3, frequency
            ORDER BY frequency DESC
            LIMIT 20
        """, {"min_occ": min_occurrences}))

        emerging = []
        for r in results:
            chain = [r["step1"], r["step2"], r["step3"]]
            pattern_id = "CRPAT_EMERGING_" + hashlib.sha256(
                json.dumps(chain).encode()
            ).hexdigest()[:8].upper()

            # Name it with LLM
            name = self._name_pattern(chain)

            emerging.append(EmergingPattern(
                pattern_id=pattern_id,
                trigger_event_type=chain[0],
                chain_steps=chain,
                observed_count=r["frequency"],
                first_seen=datetime.utcnow().isoformat(),
                last_seen=datetime.utcnow().isoformat(),
                avg_duration_days=30.0,
                confidence=min(0.90, r["frequency"] / (min_occurrences * 3)),
                proposed_name=name,
            ))

        return emerging

    def _name_pattern(self, chain: list[str]) -> str:
        try:
            resp = self.ollama_client.generate(
                model=OLLAMA_MODEL,
                prompt=(
                    f"Generate a short (4-7 word) name for this causal chain: "
                    f"{' -> '.join(chain)}. Respond with ONLY the name."
                ),
                options={"temperature": 0.4, "num_predict": 30},
            )
            return resp["response"].strip().replace('"', '')[:60]
        except Exception:
            return " -> ".join(chain[:2])

    def propose_emerging_patterns(self, patterns: list[EmergingPattern]):
        """Write proposed patterns to Memgraph for analyst review."""
        for p in patterns:
            self.mg.execute("""
                MERGE (pat:CausalPattern {pattern_id: $pid})
                SET pat.name = $name,
                    pat.status = 'proposed',
                    pat.trigger_event_type = $trigger,
                    pat.observed_count = $count,
                    pat.confidence = $conf,
                    pat.proposed_at = localDateTime()
            """, {
                "pid": p.pattern_id,
                "name": p.proposed_name,
                "trigger": p.trigger_event_type,
                "count": p.observed_count,
                "conf": p.confidence,
            })
            logger.info("Proposed pattern: %s (%s)", p.proposed_name, p.pattern_id)

    def run_cycle(self):
        """Run one complete learning cycle."""
        validated = self.run_backward_validation()
        logger.info("Validated %d predictions", validated)

        contradictions = self.detect_contradictions()
        logger.info("Found %d contradictions", len(contradictions))

        aged = self.apply_pattern_aging()
        logger.info("Aged %d patterns", aged)

        emerging = self.detect_emerging_patterns()
        if emerging:
            self.propose_emerging_patterns(emerging)
            logger.info("Proposed %d new patterns", len(emerging))


def run():
    """Main loop."""
    interval_hours = int(os.getenv("LEARNING_INTERVAL_HOURS", "6"))
    learner = CausalGraphLearner()
    logger.info("CausalGraphLearner started (interval=%dh)", interval_hours)

    import time
    while True:
        try:
            learner.run_cycle()
        except Exception as e:
            logger.error("Learning cycle error: %s", e, exc_info=True)
        time.sleep(interval_hours * 3600)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    run()
