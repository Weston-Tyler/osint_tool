"""Unit tests for WorldFish deterministic prediction generation and the output contract."""

from worldfish.contract import PREDICTION_TOPIC, SCHEMA, SCHEMA_VERSION, build_prediction_event
from worldfish.prediction import CausalPrediction, generate_predictions


def _history(enforcement_curve, violence=0.2, stability=0.7):
    return [
        {"step": i, "days": i * 2.0, "violence": violence, "enforcement": e, "stability": stability, "events": 1}
        for i, e in enumerate(enforcement_curve)
    ]


class TestGeneratePredictions:
    def test_enforcement_signal_produces_interdiction(self):
        preds = generate_predictions(
            seed_id="s", simulation_run_id="r", trigger_event_id="t",
            trigger_event_type="AIS_DISABLE", domain="maritime", n_agents=20,
            state_history=_history([0.3, 0.5, 0.7, 0.85]), agent_action_summary={},
            region="Eastern Pacific", lat=12.5, lon=-90.3, step_days=2.0,
        )
        assert len(preds) >= 1
        assert preds[0].predicted_event_type == "INTERDICTION_OPERATION"
        assert 0.0 <= preds[0].confidence <= 1.0
        assert preds[0].predicted_location_region == "Eastern Pacific"
        assert preds[0].trigger_event_id == "t"

    def test_flat_trajectory_still_emits_one_prediction(self):
        preds = generate_predictions(
            seed_id="s", simulation_run_id="r", trigger_event_id="t",
            trigger_event_type="X", domain="maritime", n_agents=5,
            state_history=_history([0.3, 0.3, 0.3]), agent_action_summary={},
        )
        assert len(preds) == 1
        assert preds[0].predicted_event_type == "STATUS_QUO_CONTINUATION"

    def test_deterministic(self):
        kw = dict(
            seed_id="s", simulation_run_id="r", trigger_event_id="t",
            trigger_event_type="X", domain="maritime", n_agents=10,
            state_history=_history([0.3, 0.6, 0.8]), agent_action_summary={},
        )
        a = generate_predictions(**kw)
        b = generate_predictions(**kw)
        assert [p.predicted_event_type for p in a] == [p.predicted_event_type for p in b]
        assert [p.confidence for p in a] == [p.confidence for p in b]

    def test_top_actions_in_causal_chain(self):
        summary = {
            "a1": {"actions": {"ais_disable": 5, "wait": 1}},
            "a2": {"actions": {"ais_disable": 2, "evade_patrol": 3}},
        }
        preds = generate_predictions(
            seed_id="s", simulation_run_id="r", trigger_event_id="t",
            trigger_event_type="AIS_DISABLE", domain="maritime", n_agents=10,
            state_history=_history([0.3, 0.6, 0.85]), agent_action_summary=summary,
        )
        assert "ais_disable" in preds[0].causal_chain_path

    def test_territorial_violence_signal(self):
        preds = generate_predictions(
            seed_id="s", simulation_run_id="r", trigger_event_id="t",
            trigger_event_type="ARMED_CLASH", domain="territorial", n_agents=15,
            state_history=_history([0.3, 0.3], violence=0.7), agent_action_summary={},
        )
        assert preds[0].predicted_event_type == "ARMED_CLASH"


class TestContractEnvelope:
    def test_envelope_shape(self):
        pred = CausalPrediction(
            simulation_run_id="run-1", predicted_event_type="INTERDICTION_OPERATION",
            confidence=0.42, domain="maritime",
        )
        env = build_prediction_event(pred, produced_at="2026-07-13T00:00:00Z")
        assert env["schema"] == SCHEMA
        assert env["schema_version"] == SCHEMA_VERSION
        assert env["event_type"] == "predicted_event"
        assert env["producer"] == "worldfish"
        assert env["produced_at"] == "2026-07-13T00:00:00Z"
        assert env["predicted_event_type"] == "INTERDICTION_OPERATION"
        assert env["payload"]["assertion_type"] == "CausalPrediction"

    def test_topic_follows_mda_convention(self):
        assert PREDICTION_TOPIC.startswith("mda.predictions.")
