"""Unit tests for deterministic synthetic persona generation (reproducibility)."""

from worldfish.persona_generator import AgentPersona, build_synthetic_personas


class TestSyntheticPersonas:
    def test_reproducible_same_seed(self):
        a = build_synthetic_personas(10, rng_seed=42)
        b = build_synthetic_personas(10, rng_seed=42)
        assert [p.persona_id for p in a] == [p.persona_id for p in b]
        assert [p.risk_tolerance for p in a] == [p.risk_tolerance for p in b]
        assert [p.agent_type for p in a] == [p.agent_type for p in b]
        assert [p.information_access for p in a] == [p.information_access for p in b]

    def test_different_seed_differs(self):
        a = build_synthetic_personas(10, rng_seed=1)
        b = build_synthetic_personas(10, rng_seed=2)
        assert [p.risk_tolerance for p in a] != [p.risk_tolerance for p in b]

    def test_count_and_validity(self):
        personas = build_synthetic_personas(7, rng_seed=0)
        assert len(personas) == 7
        for p in personas:
            assert isinstance(p, AgentPersona)
            assert 0.0 <= p.risk_tolerance <= 1.0
            assert 0.0 <= p.cooperation_propensity <= 1.0
            assert p.persona_id and p.name and p.system_prompt

    def test_agent_types_from_list_cycle(self):
        personas = build_synthetic_personas(4, agent_types=["civilian"], rng_seed=0)
        assert all(p.agent_type == "civilian" for p in personas)

    def test_unknown_types_fall_back_to_default_mix(self):
        personas = build_synthetic_personas(5, agent_types=["not-a-real-type"], rng_seed=0)
        assert len(personas) == 5
        assert all(p.agent_type != "not-a-real-type" for p in personas)

    def test_zero_agents(self):
        assert build_synthetic_personas(0) == []
