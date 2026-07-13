"""Unit tests for the pure prediction->Memgraph transform (no live Memgraph/Kafka)."""

import pytest

from processors.graph.prediction_transform import (
    NODE_LABEL,
    PredictionContractError,
    build_predicted_event_cypher,
    build_trigger_link_cypher,
    has_location,
    validate_envelope,
)


def _env(**over):
    env = {
        "schema": "mda.prediction", "schema_version": "1.0", "prediction_id": "p1",
        "predicted_event_type": "INTERDICTION_OPERATION", "confidence": 0.42,
        "confidence_label": "moderate", "domain": "maritime", "simulation_run_id": "run-1",
        "payload": {"payload": {
            "description": "d",
            "location": {"lat": 12.5, "lon": -90.3, "region": "Eastern Pacific"},
            "timeframe": {"min_days": 10, "max_days": 40},
            "causal_chain": {"trigger": "evt-1", "path": ["evt-1", "ais_disable"], "summary": "s"},
        }},
    }
    env.update(over)
    return env


class TestValidate:
    def test_valid(self):
        validate_envelope(_env())

    def test_wrong_schema(self):
        with pytest.raises(PredictionContractError):
            validate_envelope(_env(schema="something.else"))

    def test_unknown_major_version(self):
        with pytest.raises(PredictionContractError):
            validate_envelope(_env(schema_version="2.0"))

    def test_missing_prediction_id(self):
        env = _env()
        del env["prediction_id"]
        with pytest.raises(PredictionContractError):
            validate_envelope(env)

    def test_minor_version_forward_compatible(self):
        validate_envelope(_env(schema_version="1.7"))


class TestBuildCypher:
    def test_cypher_and_params(self):
        cypher, params = build_predicted_event_cypher(_env())
        assert NODE_LABEL in cypher and "MERGE" in cypher
        assert params["pid"] == "p1"
        assert params["conf"] == 0.42
        assert params["lat"] == 12.5
        assert params["tmin"] == 10
        assert params["chain_path"] == ["evt-1", "ais_disable"]

    def test_malformed_raises(self):
        with pytest.raises(PredictionContractError):
            build_predicted_event_cypher(_env(schema="x"))

    def test_generated_at_captured(self):
        env = _env()
        env["payload"]["generated_at"] = "2026-07-13T00:00:00Z"
        cypher, params = build_predicted_event_cypher(env)
        assert params["gen_at"] == "2026-07-13T00:00:00Z"
        assert "p.generated_at = $gen_at" in cypher

    def test_trigger_link(self):
        link = build_trigger_link_cypher(_env())
        assert link is not None
        cypher, params = link
        assert "PREDICTED_FROM" in cypher
        assert params["trigger"] == "evt-1"

    def test_no_trigger_link_when_absent(self):
        env = _env()
        env["payload"]["payload"]["causal_chain"] = {}
        assert build_trigger_link_cypher(env) is None

    def test_has_location(self):
        assert has_location(_env())
        env = _env()
        env["payload"]["payload"]["location"] = {}
        assert not has_location(env)


class TestSchemaArtifacts:
    def test_prediction_schema_files_exist(self):
        import pathlib

        root = pathlib.Path(__file__).resolve().parents[2]
        assert (root / "schema" / "graph" / "predictions-schema.cypher").exists()
        assert (root / "schema" / "relational" / "predictions-schema.sql").exists()
