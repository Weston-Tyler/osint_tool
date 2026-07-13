"""Unit tests for bounded agent memory (deque maxlen)."""

from worldfish.persona_generator import build_synthetic_personas
from worldfish.simulation_engine import MEMORY_MAXLEN, WorldFishAgent


class TestBoundedMemory:
    def test_memory_bounded_to_maxlen(self):
        persona = build_synthetic_personas(1, rng_seed=0)[0]
        agent = WorldFishAgent(0, persona)
        for i in range(MEMORY_MAXLEN + 50):
            agent.memory.append(f"m{i}")
        assert len(agent.memory) == MEMORY_MAXLEN
        # newest retained, oldest evicted
        assert agent.memory[-1] == f"m{MEMORY_MAXLEN + 49}"
        assert "m0" not in agent.memory

    def test_initial_memories_respect_bound(self):
        persona = build_synthetic_personas(1, rng_seed=0)[0]
        persona.initial_memories = [f"init{i}" for i in range(MEMORY_MAXLEN + 5)]
        agent = WorldFishAgent(0, persona)
        assert len(agent.memory) == MEMORY_MAXLEN
