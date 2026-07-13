"""Unit tests for graph_consumer routing of mda.predictions.worldfish (DLQ-first).

Uses injected fakes for Memgraph and the DLQ producer, so no live infra is needed.
Builds envelopes with the real WorldFish producer to prove producer/consumer agree
on the v1.0 contract.
"""

from processors.graph.graph_consumer import GraphProcessor
from worldfish.contract import build_prediction_event
from worldfish.prediction import CausalPrediction


class FakeMemgraph:
    def __init__(self):
        self.calls = []

    def execute(self, cypher, params=None):
        self.calls.append((cypher, params))


class FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, value):
        self.sent.append((topic, value))


def _valid_envelope():
    pred = CausalPrediction(
        simulation_run_id="run-1", predicted_event_type="INTERDICTION_OPERATION",
        predicted_location_lat=12.5, predicted_location_lon=-90.3,
        predicted_location_region="Eastern Pacific", confidence=0.42, domain="maritime",
        trigger_event_id="evt-1", causal_chain_path=["evt-1", "ais_disable"],
    )
    env = build_prediction_event(pred, produced_at="2026-07-13T00:00:00Z")
    # to_obi_assertion_dict nests causal_chain.trigger from trigger_event_id
    return env


def test_valid_prediction_upserted():
    mg, dlq = FakeMemgraph(), FakeProducer()
    gp = GraphProcessor(memgraph=mg, dlq_producer=dlq)
    gp.process_message("mda.predictions.worldfish", _valid_envelope())
    assert mg.calls, "expected a Memgraph upsert"
    assert "PredictedEvent" in mg.calls[0][0]
    assert mg.calls[0][1]["pid"]
    assert not dlq.sent


def test_malformed_goes_to_dlq():
    mg, dlq = FakeMemgraph(), FakeProducer()
    gp = GraphProcessor(memgraph=mg, dlq_producer=dlq)
    gp.process_message("mda.predictions.worldfish", {"schema": "wrong", "prediction_id": "x"})
    assert dlq.sent and dlq.sent[0][0] == "mda.dlq"
    assert not mg.calls


def test_unknown_version_goes_to_dlq():
    mg, dlq = FakeMemgraph(), FakeProducer()
    gp = GraphProcessor(memgraph=mg, dlq_producer=dlq)
    env = _valid_envelope()
    env["schema_version"] = "2.0"
    gp.process_message("mda.predictions.worldfish", env)
    assert dlq.sent and not mg.calls
