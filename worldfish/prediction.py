"""CausalPrediction — structured output from WorldFish simulations."""

from __future__ import annotations

import json
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional


class PredictionConfidence(Enum):
    VERY_LOW = "very_low"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"

    @classmethod
    def from_float(cls, v: float) -> PredictionConfidence:
        if v < 0.20:
            return cls.VERY_LOW
        if v < 0.40:
            return cls.LOW
        if v < 0.60:
            return cls.MODERATE
        if v < 0.80:
            return cls.HIGH
        return cls.VERY_HIGH


@dataclass
class AlternativeOutcome:
    description: str
    probability: float
    predicted_event_type: str
    divergence_point: str


@dataclass
class CausalPrediction:
    prediction_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    simulation_run_id: str = ""
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    predicted_event_type: str = ""
    predicted_event_description: str = ""
    predicted_location_lat: Optional[float] = None
    predicted_location_lon: Optional[float] = None
    predicted_location_region: str = ""
    predicted_location_uncertainty_km: float = 50.0
    predicted_timeframe_min_days: int = 0
    predicted_timeframe_max_days: int = 90
    predicted_timeframe_median_days: int = 30
    confidence: float = 0.0
    confidence_label: str = ""
    confidence_decay_rate: float = 0.02
    trigger_event_id: str = ""
    causal_chain_path: list[str] = field(default_factory=list)
    causal_chain_summary: str = ""
    causal_chain_hop_count: int = 0
    n_agents: int = 0
    n_simulation_steps: int = 0
    domain: str = ""
    scenario_variables: dict = field(default_factory=dict)
    alternative_outcomes: list[AlternativeOutcome] = field(default_factory=list)
    atak_cot_uid: Optional[str] = None
    alert_sent: bool = False

    def __post_init__(self):
        self.confidence_label = PredictionConfidence.from_float(self.confidence).value

    def current_confidence(self, days_elapsed: float) -> float:
        return self.confidence * ((1 - self.confidence_decay_rate) ** days_elapsed)

    def to_obi_assertion_dict(self) -> dict:
        return {
            "assertion_type": "CausalPrediction",
            "assertion_id": self.prediction_id,
            "source": f"worldfish:{self.simulation_run_id}",
            "confidence": self.confidence,
            "generated_at": self.generated_at,
            "payload": {
                "predicted_event_type": self.predicted_event_type,
                "description": self.predicted_event_description,
                "location": {"lat": self.predicted_location_lat, "lon": self.predicted_location_lon,
                             "region": self.predicted_location_region},
                "timeframe": {"min_days": self.predicted_timeframe_min_days,
                              "max_days": self.predicted_timeframe_max_days},
                "causal_chain": {"trigger": self.trigger_event_id, "path": self.causal_chain_path,
                                 "summary": self.causal_chain_summary},
                "simulation": {"n_agents": self.n_agents, "n_steps": self.n_simulation_steps,
                               "domain": self.domain},
                "alternatives": [asdict(a) for a in self.alternative_outcomes],
            },
        }

    def to_atak_cot_xml(self) -> str:
        if not self.atak_cot_uid:
            self.atak_cot_uid = str(uuid.uuid4())
        domain_type = {"maritime": "a-h-G-U-C-I", "territorial": "a-h-G-E",
                       "market": "a-n-G", "humanitarian": "a-f-G"}.get(self.domain, "a-u-G")
        now = datetime.now(timezone.utc)
        start = now + timedelta(days=self.predicted_timeframe_min_days)
        stale = now + timedelta(days=self.predicted_timeframe_max_days)
        lat = self.predicted_location_lat or 0.0
        lon = self.predicted_location_lon or 0.0
        ce = self.predicted_location_uncertainty_km * 1000

        event = ET.Element("event", {
            "version": "2.0", "uid": self.atak_cot_uid, "type": domain_type,
            "time": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "start": start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "stale": stale.strftime("%Y-%m-%dT%H:%M:%S.000Z"), "how": "m-g",
        })
        ET.SubElement(event, "point", {"lat": f"{lat:.6f}", "lon": f"{lon:.6f}",
                                        "hae": "0.0", "ce": f"{ce:.0f}", "le": "9999999.0"})
        detail = ET.SubElement(event, "detail")
        ET.SubElement(detail, "contact", {"callsign": f"WF-{self.prediction_id[:8].upper()}"})
        ET.SubElement(detail, "remarks").text = (
            f"[WorldFish | Conf: {self.confidence:.0%} | {self.domain.upper()}]\n"
            f"{self.predicted_event_description}\n"
            f"Timeframe: {self.predicted_timeframe_min_days}-{self.predicted_timeframe_max_days}d"
        )
        return ET.tostring(event, encoding="unicode", xml_declaration=True)

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_obi_assertion_dict(), indent=indent, default=str)


# ---------------------------------------------------------------------------
# Prediction generation (deterministic, rule-based)
# ---------------------------------------------------------------------------

# Event-type names keyed by (signal, domain). "_" is the domain-agnostic default.
_EVENT_TYPES: dict[str, dict[str, str]] = {
    "violence": {"maritime": "MARITIME_VIOLENT_INCIDENT", "territorial": "ARMED_CLASH", "_": "VIOLENCE_ESCALATION"},
    "enforcement": {"maritime": "INTERDICTION_OPERATION", "territorial": "CHECKPOINT_OPERATION", "_": "ENFORCEMENT_ACTION"},
    "instability": {"maritime": "MARITIME_ROUTE_DISRUPTION", "territorial": "TERRITORIAL_INSTABILITY", "_": "INSTABILITY_EVENT"},
    "status_quo": {"_": "STATUS_QUO_CONTINUATION"},
}

# A signal must reach this strength before it becomes its own prediction.
_SIGNAL_THRESHOLD = 0.10


def _event_type(signal: str, domain: str) -> str:
    return _EVENT_TYPES[signal].get(domain, _EVENT_TYPES[signal]["_"])


def _confidence(strength: float, n_steps: int, n_agents: int) -> float:
    """Confidence from signal strength, damped by how much simulation backed it."""
    coverage = min(1.0, n_steps / 30.0) * min(1.0, n_agents / 15.0)
    raw = (0.15 + 0.65 * strength) * (0.6 + 0.4 * coverage)
    return round(max(0.0, min(1.0, raw)), 4)


def _top_actions(agent_action_summary: dict, limit: int = 3) -> list[str]:
    """Most frequent agent actions across the run (deterministic ordering)."""
    totals: dict[str, int] = {}
    for summ in agent_action_summary.values():
        for action, count in (summ.get("actions") or {}).items():
            totals[action] = totals.get(action, 0) + int(count)
    ranked = sorted(totals.items(), key=lambda kv: (-kv[1], kv[0]))
    return [name for name, _ in ranked[:limit]]


def generate_predictions(
    *,
    seed_id: str,
    simulation_run_id: str,
    trigger_event_id: str,
    trigger_event_type: str,
    domain: str,
    n_agents: int,
    state_history: list[dict],
    agent_action_summary: dict,
    region: str = "",
    lat: float | None = None,
    lon: float | None = None,
    step_days: float = 2.0,
) -> list[CausalPrediction]:
    """Derive forward-looking :class:`CausalPrediction` objects from a run.

    Pure and deterministic over its inputs (the ``prediction_id`` and
    ``generated_at`` fields are the only non-deterministic parts). Always returns
    at least one prediction so a completed simulation never emits nothing.
    """
    if not state_history:
        state_history = [{"violence": 0.0, "enforcement": 0.0, "stability": 1.0}]

    n_steps = len(state_history)
    first, last = state_history[0], state_history[-1]
    peak_violence = max(s.get("violence", 0.0) for s in state_history)
    peak_step = max(range(n_steps), key=lambda i: state_history[i].get("violence", 0.0))

    delta_violence = last.get("violence", 0.0) - first.get("violence", 0.0)
    final_enforcement = last.get("enforcement", 0.0)
    stability_drop = first.get("stability", 1.0) - last.get("stability", 1.0)

    # (signal-key, strength) — strength clamped to [0, 1].
    signals: list[tuple[str, float]] = [
        ("violence", max(delta_violence, peak_violence - 0.4)),
        ("enforcement", final_enforcement - 0.4),
        ("instability", stability_drop),
    ]
    signals = [(k, min(1.0, s)) for k, s in signals if s >= _SIGNAL_THRESHOLD]
    signals.sort(key=lambda ks: (-ks[1], ks[0]))

    horizon_days = max(1.0, n_steps * step_days)
    median_days = int(round(horizon_days))
    top_actions = _top_actions(agent_action_summary)

    def _make(signal: str, strength: float) -> CausalPrediction:
        etype = _event_type(signal, domain)
        conf = _confidence(strength, n_steps, n_agents)
        return CausalPrediction(
            simulation_run_id=simulation_run_id,
            predicted_event_type=etype,
            predicted_event_description=(
                f"Simulation from {trigger_event_type or 'trigger'} in "
                f"{region or 'the seeded region'} projects a {etype.replace('_', ' ').lower()} "
                f"within ~{median_days} days (peaked at step {peak_step})."
            ),
            predicted_location_lat=lat,
            predicted_location_lon=lon,
            predicted_location_region=region,
            predicted_timeframe_min_days=max(0, int(median_days * 0.5)),
            predicted_timeframe_median_days=median_days,
            predicted_timeframe_max_days=max(median_days, int(median_days * 1.5)),
            confidence=conf,
            trigger_event_id=trigger_event_id,
            causal_chain_path=[trigger_event_id, *top_actions],
            causal_chain_summary=(
                f"{trigger_event_type or 'trigger'} -> "
                + " -> ".join(top_actions) if top_actions else (trigger_event_type or "trigger")
            ),
            causal_chain_hop_count=1 + len(top_actions),
            n_agents=n_agents,
            n_simulation_steps=n_steps,
            domain=domain,
            scenario_variables={
                "peak_violence": round(peak_violence, 4),
                "delta_violence": round(delta_violence, 4),
                "final_enforcement": round(final_enforcement, 4),
                "stability_drop": round(stability_drop, 4),
            },
        )

    if not signals:
        # Flat trajectory — still emit one low-confidence baseline prediction.
        return [_make("status_quo", 0.0)]

    predictions = [_make(sig, strength) for sig, strength in signals]
    # Fold the weaker signals in as alternative outcomes on the strongest one.
    for sig, strength in signals[1:]:
        predictions[0].alternative_outcomes.append(
            AlternativeOutcome(
                description=f"Instead, a {_event_type(sig, domain).replace('_', ' ').lower()} develops.",
                probability=_confidence(strength, n_steps, n_agents),
                predicted_event_type=_event_type(sig, domain),
                divergence_point=f"step {peak_step}",
            )
        )
    return predictions
