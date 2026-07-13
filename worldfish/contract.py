"""Predictive-event output contract for WorldFish.

The versioned wire format for the forward (predicted) events WorldFish publishes
to Kafka. Every simulation prediction is wrapped in the envelope produced by
``build_prediction_event`` and sent to :data:`PREDICTION_TOPIC`, where the
graph-processor consumes it. See ``worldfish/PREDICTION_CONTRACT.md`` for the
field-level documentation and capsule ``202607-worldfish-buildout``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .prediction import CausalPrediction

# Bump the minor version for additive/back-compatible changes to the envelope,
# the major version for breaking changes. Consumers should tolerate unknown keys.
SCHEMA = "mda.prediction"
SCHEMA_VERSION = "1.0"

# mda.<domain>.<subtype> — predictions produced by the WorldFish simulation.
# The graph-processor subscribes to this topic (decision: capsule
# 202607-worldfish-buildout — events flow via Kafka, never direct DB writes).
PREDICTION_TOPIC = "mda.predictions.worldfish"


def build_prediction_event(pred: CausalPrediction, produced_at: str | None = None) -> dict[str, Any]:
    """Wrap a :class:`CausalPrediction` in the versioned Kafka envelope.

    ``produced_at`` may be supplied to keep the output deterministic (tests);
    otherwise it defaults to the current UTC time.
    """
    return {
        "schema": SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "event_type": "predicted_event",
        "produced_at": produced_at or datetime.now(timezone.utc).isoformat(),
        "producer": "worldfish",
        "prediction_id": pred.prediction_id,
        "simulation_run_id": pred.simulation_run_id,
        "domain": pred.domain,
        "confidence": pred.confidence,
        "confidence_label": pred.confidence_label,
        "predicted_event_type": pred.predicted_event_type,
        "payload": pred.to_obi_assertion_dict(),
    }
