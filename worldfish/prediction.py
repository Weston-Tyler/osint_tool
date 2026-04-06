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
