"""Integration test: run WorldFish against a contract-faithful fake Ollama.

Unlike the CI import-stub (a bare module that swallows every call), this fake
implements the *real* Ollama ``generate()`` contract — it returns
``{"response": <text>}`` containing a JSON action — so it exercises the actual
LLM decision path (:class:`OllamaDecisionPolicy`) end to end. Lives under
tests/integration/ so it is not part of the unit gate.
"""

import json

from worldfish.environments import ActionType
from worldfish.persona_generator import build_synthetic_personas
from worldfish.seed_extractor import build_demo_seed
from worldfish.simulation_engine import OllamaDecisionPolicy, WorldFishSimulation


class FakeOllamaClient:
    """Contract-faithful stand-in for ``ollama.Client``."""

    def __init__(self):
        self.calls = 0

    def generate(self, model, prompt, options=None):
        self.calls += 1
        # Parse the ACTIONS: [...] list out of the prompt and choose a real one.
        choice = "wait"
        try:
            frag = prompt.split("ACTIONS:")[1].split("[")[1].split("]")[0]
            actions = [a.strip().strip("'\"") for a in frag.split(",")]
            choice = next((a for a in actions if a and a != "wait"), "wait")
        except (IndexError, ValueError):
            pass
        return {"response": json.dumps({"action": choice, "params": {}})}


def test_engine_runs_with_contract_faithful_ollama():
    client = FakeOllamaClient()
    policy = OllamaDecisionPolicy(client, model="test-model")
    result = WorldFishSimulation(
        build_demo_seed(), n_agents=10, rng_seed=0, decision_policy=policy
    ).run(n_steps=15)

    assert client.calls > 0  # the LLM decision path was actually exercised
    assert len(result.predictions) >= 1
    assert result.predicted_events[0]["schema"] == "mda.prediction"


def test_policy_parses_action_json_and_respects_available():
    policy = OllamaDecisionPolicy(FakeOllamaClient(), model="test-model")
    persona = build_synthetic_personas(1, ["smuggling_network"], rng_seed=0)[0]
    available = [ActionType.WAIT, ActionType.MARITIME_TRANSIT, ActionType.EVADE_PATROL]
    action, params = policy(persona, {"enforcement_pressure": 0.3}, available)
    assert action in available
    assert isinstance(params, dict)
