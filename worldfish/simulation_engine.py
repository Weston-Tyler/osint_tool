"""WorldFish simulation engine.

A lightweight agent-based simulation that projects how a seeded intelligence
scenario evolves, then derives forward-looking predictive events from the
resulting trajectory.

Agent decisions come from a pluggable **decision policy**:

* the default deterministic, rule-based policy runs fully offline and is
  reproducible given a fixed ``rng_seed`` (used by the CLI and the test suite);
* an Ollama-backed policy (:class:`OllamaDecisionPolicy`) drives decisions with a
  local LLM when a client is supplied.

``mesa``/``ollama`` are intentionally *not* hard dependencies — the engine ships
its own random-activation scheduler so it runs and tests without them. This is a
deliberate change from the original mesa-subclassing version to make the
predictive-event foundation runnable and deterministically testable
(see capsule 202607-worldfish-buildout).
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .environments import (
    ActionType,
    MaritimeDomainEnvironment,
    SimulationEnvironment,
    TerritorialDomainEnvironment,
    WorldState,
)
from .persona_generator import AgentPersona, build_synthetic_personas
from .prediction import CausalPrediction, generate_predictions
from .seed_extractor import SimulationSeed

logger = logging.getLogger("mda.worldfish.engine")

OLLAMA_HOST = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_LLM_MODEL", "qwen2.5:7b")
DEFAULT_N_AGENTS = int(os.getenv("WORLDFISH_DEFAULT_N_AGENTS", "24"))
DEFAULT_STEPS = int(os.getenv("WORLDFISH_DEFAULT_STEPS", "40"))
MEMORY_MAXLEN = 20

# Actions that move the world state materially — chosen more often by
# higher-risk agents so an adversary-heavy scenario escalates.
_HIGH_IMPACT = {
    ActionType.AIS_DISABLE, ActionType.STS_TRANSFER, ActionType.EVADE_PATROL, ActionType.FLAG_CHANGE,
    ActionType.TERRITORIAL_EXPANSION, ActionType.ATTACK, ActionType.ALLIANCE_BREAK,
    ActionType.BRIBE_OFFICIAL, ActionType.RECRUIT,
}


# --------------------------------------------------------------------------
# Decision policies
# --------------------------------------------------------------------------

# A policy is any callable: (persona, observation, available actions) -> (action, params).


def make_deterministic_policy(rng: random.Random):
    """A rule-based policy: high-risk personas pick high-impact actions.

    Deterministic given ``rng`` — the whole simulation is reproducible when the
    engine is built from a fixed ``rng_seed``.
    """

    def policy(persona: AgentPersona, observation: dict, available: list[ActionType]):
        high = [a for a in available if a in _HIGH_IMPACT]
        low = [a for a in available if a not in _HIGH_IMPACT]
        if high and rng.random() < persona.risk_tolerance:
            return rng.choice(high), {}
        return rng.choice(low or available), {}

    return policy


class OllamaDecisionPolicy:
    """LLM-backed policy. ``client`` must expose ``generate(model, prompt, options)``
    returning ``{"response": <text>}`` — matching ``ollama.Client`` and the
    contract-faithful fake used in the integration test."""

    def __init__(self, client, model: str = OLLAMA_MODEL):
        self.client = client
        self.model = model

    def __call__(self, persona: AgentPersona, observation: dict, available: list[ActionType]):
        names = [a.value for a in available]
        prompt = (
            f"{persona.system_prompt}\n\n"
            f"SITUATION: {json.dumps(observation, default=str)[:300]}\n"
            f"ACTIONS: {names}\n\n"
            'Choose ONE action. JSON only: {"action": "<name>", "params": {}}'
        )
        try:
            resp = self.client.generate(
                model=self.model, prompt=prompt,
                options={"temperature": 0.4, "num_predict": 200},
            )
            match = re.search(r"\{.*?\}", resp.get("response", ""), re.DOTALL)
            if match:
                decision = json.loads(match.group())
                try:
                    action = ActionType(decision.get("action", "wait"))
                    if action in available:
                        return action, decision.get("params", {})
                except ValueError:
                    pass
        except Exception:
            logger.debug("LLM decision failed for %s; defaulting to WAIT", persona.persona_id)
        return ActionType.WAIT, {}


def build_ollama_policy(host: str = OLLAMA_HOST, model: str = OLLAMA_MODEL) -> OllamaDecisionPolicy:
    """Construct an Ollama-backed policy (imports ``ollama`` lazily)."""
    import ollama

    return OllamaDecisionPolicy(ollama.Client(host=host), model=model)


# --------------------------------------------------------------------------
# Agents and model
# --------------------------------------------------------------------------


class WorldFishAgent:
    """One simulation agent with a bounded rolling memory."""

    def __init__(self, unique_id: int, persona: AgentPersona):
        self.unique_id = unique_id
        self.persona = persona
        self.resources = list(persona.resources)
        # Bounded memory: only the most recent MEMORY_MAXLEN entries are kept.
        from collections import deque

        self.memory: deque = deque(persona.initial_memories, maxlen=MEMORY_MAXLEN)
        self.action_history: list[dict] = []

    def step(self, model: WorldFishModel) -> None:
        obs = model.environment.get_agent_observation(self.persona.persona_id, model.world_state)
        available = model.environment.get_available_actions(
            {"agent_type": self.persona.agent_type, "risk_tolerance": self.persona.risk_tolerance}
        )
        action, params = model.policy(self.persona, obs, available)
        new_state, result = model.environment.apply_action(
            self.persona.persona_id, action, params, model.world_state
        )
        model.world_state = new_state
        self.action_history.append({"step": model.steps, "action": action.value, "result": result})
        self.memory.append(f"Step {model.steps}: {action.value} -> {result.get('outcome', '')}")


class WorldFishModel:
    """The simulation model: agents, environment, world state, and a scheduler."""

    def __init__(
        self,
        personas: list[AgentPersona],
        environment: SimulationEnvironment,
        initial_state: WorldState,
        step_days: float,
        rng: random.Random,
        policy,
    ):
        self.environment = environment
        self.world_state = initial_state
        self.step_days = step_days
        self.rng = rng
        self.policy = policy
        self.steps = 0
        self.terminal_reason = ""
        self.agents = [WorldFishAgent(i, p) for i, p in enumerate(personas)]

    def step(self) -> bool:
        """Advance one tick (random activation). Returns False if terminal."""
        self.world_state.events_this_step = []
        self.world_state.time_elapsed_days += self.step_days
        order = list(self.agents)
        self.rng.shuffle(order)
        for agent in order:
            agent.step(self)
        self.steps += 1
        self.world_state.step = self.steps
        terminal, reason = self.environment.check_terminal(self.world_state)
        if terminal:
            self.terminal_reason = reason
            logger.info("Terminal: %s", reason)
            return False
        return True


# --------------------------------------------------------------------------
# Result + orchestrator
# --------------------------------------------------------------------------


@dataclass
class SimulationResult:
    simulation_id: str
    seed_id: str
    domain: str
    n_agents: int
    n_steps_completed: int
    total_simulated_days: float
    started_at: datetime
    completed_at: datetime
    state_history: list[dict]
    agent_action_summary: dict
    predictions: list[CausalPrediction] = field(default_factory=list)
    predicted_events: list[dict] = field(default_factory=list)


class WorldFishSimulation:
    """Top-level orchestrator: build a model from a seed, run it, predict."""

    def __init__(
        self,
        seed: SimulationSeed,
        domain: str | None = None,
        n_agents: int | None = None,
        step_days: float = 2.0,
        rng_seed: int = 0,
        decision_policy=None,
    ):
        self.seed = seed
        self.domain = domain or seed.recommended_domain
        # NOTE: seed.recommended_agents is a list of *type* names, not a count.
        self.n_agents = n_agents or DEFAULT_N_AGENTS
        self.step_days = step_days
        self.rng_seed = rng_seed
        self.decision_policy = decision_policy
        self.simulation_id = str(uuid.uuid4())
        self.model: WorldFishModel | None = None
        self.personas: list[AgentPersona] = []

    def build(self) -> None:
        self.personas = build_synthetic_personas(
            self.n_agents, self.seed.recommended_agents, rng_seed=self.rng_seed
        )
        risks = [float(e.get("risk_score", 5.0)) for e in self.seed.seed_events] or [5.0]
        avg_risk = sum(risks) / len(risks)
        initial_state = WorldState(
            global_stability_index=max(0.1, 1.0 - avg_risk / 10.0),
            enforcement_pressure=0.3, violence_level=0.2, economic_stress=0.3,
        )
        env_class = MaritimeDomainEnvironment if self.domain == "maritime" else TerritorialDomainEnvironment
        env = env_class({})
        rng = random.Random(self.rng_seed)
        policy = self.decision_policy or make_deterministic_policy(random.Random(self.rng_seed + 1))
        self.model = WorldFishModel(self.personas, env, initial_state, self.step_days, rng, policy)

    def _trigger_location(self) -> tuple[float | None, float | None, str]:
        for evt in self.seed.seed_events:
            if evt.get("event_id") == self.seed.trigger_event_id:
                return evt.get("latitude"), evt.get("longitude"), evt.get("region", "")
        first = self.seed.seed_events[0] if self.seed.seed_events else {}
        return first.get("latitude"), first.get("longitude"), first.get("region", "")

    def run(self, n_steps: int | None = None) -> SimulationResult:
        if not self.model:
            self.build()
        n_steps = n_steps or DEFAULT_STEPS
        started = datetime.now(timezone.utc)
        history: list[dict] = []
        for step in range(n_steps):
            cont = self.model.step()
            ws = self.model.world_state
            history.append({
                "step": step, "days": ws.time_elapsed_days,
                "violence": ws.violence_level, "enforcement": ws.enforcement_pressure,
                "stability": ws.global_stability_index, "economic_stress": ws.economic_stress,
                "events": len(ws.events_this_step),
            })
            if not cont:
                break

        summary: dict = {}
        for agent in self.model.agents:
            counts: dict[str, int] = {}
            for entry in agent.action_history:
                counts[entry["action"]] = counts.get(entry["action"], 0) + 1
            summary[agent.persona.persona_id] = {
                "name": agent.persona.name, "type": agent.persona.agent_type,
                "actions": counts, "total": len(agent.action_history),
            }

        lat, lon, region = self._trigger_location()
        predictions = generate_predictions(
            seed_id=self.seed.seed_id,
            simulation_run_id=self.simulation_id,
            trigger_event_id=self.seed.trigger_event_id,
            trigger_event_type=self.seed.trigger_event_type,
            domain=self.domain,
            n_agents=len(self.personas),
            state_history=history,
            agent_action_summary=summary,
            region=region, lat=lat, lon=lon, step_days=self.step_days,
        )
        # Envelopes are imported here to avoid a hard import cycle at module load.
        from .contract import build_prediction_event

        envelopes = [build_prediction_event(p) for p in predictions]

        return SimulationResult(
            simulation_id=self.simulation_id, seed_id=self.seed.seed_id,
            domain=self.domain, n_agents=len(self.personas),
            n_steps_completed=len(history),
            total_simulated_days=history[-1]["days"] if history else 0.0,
            started_at=started, completed_at=datetime.now(timezone.utc),
            state_history=history, agent_action_summary=summary,
            predictions=predictions, predicted_events=envelopes,
        )
