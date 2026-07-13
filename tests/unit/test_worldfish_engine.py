"""Unit tests for the offline, deterministic WorldFish end-to-end run."""

from worldfish.seed_extractor import build_demo_seed
from worldfish.simulation_engine import WorldFishSimulation


class TestEngineOffline:
    def test_end_to_end_produces_prediction(self):
        result = WorldFishSimulation(build_demo_seed(), n_agents=12, rng_seed=0).run(n_steps=20)
        assert result.n_steps_completed >= 1
        assert len(result.predictions) >= 1
        assert len(result.predicted_events) == len(result.predictions)
        env = result.predicted_events[0]
        assert env["schema"] == "mda.prediction"
        assert env["domain"] == "maritime"
        assert env["producer"] == "worldfish"

    def test_deterministic_run(self):
        r1 = WorldFishSimulation(build_demo_seed(), n_agents=12, rng_seed=7).run(n_steps=20)
        r2 = WorldFishSimulation(build_demo_seed(), n_agents=12, rng_seed=7).run(n_steps=20)
        assert [p.predicted_event_type for p in r1.predictions] == [p.predicted_event_type for p in r2.predictions]
        assert [p.confidence for p in r1.predictions] == [p.confidence for p in r2.predictions]
        assert r1.state_history == r2.state_history

    def test_build_creates_expected_agent_count(self):
        sim = WorldFishSimulation(build_demo_seed(), n_agents=8, rng_seed=0)
        sim.build()
        assert len(sim.personas) == 8
        assert len(sim.model.agents) == 8

    def test_territorial_domain_runs(self):
        result = WorldFishSimulation(build_demo_seed(domain="territorial"), n_agents=10, rng_seed=0).run(n_steps=15)
        assert result.domain == "territorial"
        assert len(result.predictions) >= 1
