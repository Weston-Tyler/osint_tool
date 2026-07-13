"""Unit tests for the offline demo seed (seed_extractor.build_demo_seed)."""

from worldfish.seed_extractor import SimulationSeed, build_demo_seed


class TestDemoSeed:
    def test_shape(self):
        seed = build_demo_seed()
        assert isinstance(seed, SimulationSeed)
        assert seed.recommended_domain == "maritime"
        assert seed.trigger_event_type == "AIS_DISABLE"
        assert seed.seed_events and seed.seed_entities
        trig = [e for e in seed.seed_events if e["event_id"] == seed.trigger_event_id]
        assert trig and trig[0]["latitude"] is not None
        assert isinstance(seed.recommended_agents, list) and seed.recommended_agents

    def test_deterministic(self):
        a, b = build_demo_seed(), build_demo_seed()
        assert a.seed_id == b.seed_id
        assert a.trigger_event_id == b.trigger_event_id
        assert a.seed_events == b.seed_events

    def test_territorial_domain(self):
        assert build_demo_seed(domain="territorial").recommended_domain == "territorial"
