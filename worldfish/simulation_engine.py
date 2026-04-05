"""WorldFish simulation engine — Mesa-based agent simulation.

Runs 50-500 agents with LLM-driven decision-making via Ollama.
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

import mesa
import numpy as np
import ollama

from .environments import ActionType, MaritimeDomainEnvironment, SimulationEnvironment, TerritorialDomainEnvironment, WorldState
from .memory import AgentMemoryStore, MemoryItem
from .persona_generator import AgentPersona
from .prediction import CausalPrediction
from .seed_extractor import SimulationSeed

logger = logging.getLogger("mda.worldfish.engine")

OLLAMA_HOST = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_LLM_MODEL", "qwen2.5:7b")


class WorldFishAgent(mesa.Agent):
    """A WorldFish simulation agent with LLM-driven decisions."""

    def __init__(self, unique_id: int, model: WorldFishModel, persona: AgentPersona):
        super().__init__(unique_id, model)
        self.persona = persona
        self.resources = dict(persona.resources)
        self.memory: deque = deque(persona.initial_memories, maxlen=20)
        self.action_history: list[dict] = []

    def step(self):
        observation = self.model.environment.get_agent_observation(
            self.persona.persona_id, self.model.world_state
        )
        action, params = self._decide(observation)
        new_state, result = self.model.environment.apply_action(
            self.persona.persona_id, action, params, self.model.world_state
        )
        self.model.world_state = new_state
        self.action_history.append({
            "step": self.model.schedule.steps,
            "action": action.value,
            "result": result,
        })
        self.memory.append(f"Step {self.model.schedule.steps}: {action.value} -> {result.get('outcome', '')}")

    def _decide(self, observation: dict) -> tuple[ActionType, dict]:
        available = self.model.environment.get_available_actions({
            "agent_type": self.persona.agent_type,
            "risk_tolerance": self.persona.risk_tolerance,
        })
        action_names = [a.value for a in available]
        recent = list(self.memory)[-5:]

        prompt = f"""{self.persona.system_prompt}

SITUATION: {json.dumps(observation, default=str)[:300]}
RECENT: {'; '.join(recent[-3:])}
ACTIONS: {action_names}
RESOURCES: {self.resources}

Choose ONE action. JSON only: {{"action": "<name>", "params": {{}}, "reasoning": "<1 sentence>"}}"""

        try:
            resp = self.model.ollama_client.generate(
                model=OLLAMA_MODEL, prompt=prompt,
                options={"temperature": 0.4, "num_predict": 200},
            )
            match = re.search(r'\{.*?\}', resp["response"], re.DOTALL)
            if match:
                decision = json.loads(match.group())
                try:
                    return ActionType(decision.get("action", "wait")), decision.get("params", {})
                except ValueError:
                    pass
        except Exception:
            pass
        return ActionType.WAIT, {}


class WorldFishModel(mesa.Model):
    """The WorldFish Mesa model."""

    def __init__(self, personas: list[AgentPersona], environment: SimulationEnvironment,
                 initial_state: WorldState, step_days: float = 2.0):
        super().__init__()
        self.environment = environment
        self.world_state = initial_state
        self.step_days = step_days
        self.schedule = mesa.time.RandomActivation(self)
        self.ollama_client = ollama.Client(host=OLLAMA_HOST)

        for i, persona in enumerate(personas):
            self.schedule.add(WorldFishAgent(i, self, persona))

        self.datacollector = mesa.DataCollector(model_reporters={
            "violence": lambda m: m.world_state.violence_level,
            "enforcement": lambda m: m.world_state.enforcement_pressure,
            "stability": lambda m: m.world_state.global_stability_index,
            "stress": lambda m: m.world_state.economic_stress,
        })

    def step(self):
        self.world_state.events_this_step = []
        self.world_state.time_elapsed_days += self.step_days
        self.datacollector.collect(self)
        self.schedule.step()
        self.world_state.step += 1
        terminal, reason = self.environment.check_terminal(self.world_state)
        if terminal:
            logger.info("Terminal: %s", reason)
            return False
        return True


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
    predicted_events: list[dict] = field(default_factory=list)


class WorldFishSimulation:
    """Top-level simulation orchestrator."""

    def __init__(self, seed: SimulationSeed, domain: str | None = None,
                 n_agents: int | None = None, step_days: float = 2.0):
        self.seed = seed
        self.domain = domain or seed.recommended_domain
        self.n_agents = n_agents or seed.recommended_agents
        self.simulation_id = str(uuid.uuid4())
        self.step_days = step_days
        self.model: WorldFishModel | None = None
        self.personas: list[AgentPersona] = []

    def build(self):
        """Build world state, personas, environment, and model."""
        from .persona_generator import OBIAgentPersonaGenerator
        gen = OBIAgentPersonaGenerator()

        # Generate personas from seed entities
        for entity in self.seed.seed_entities[:self.n_agents // 2]:
            try:
                self.personas.append(gen.generate_from_obi_object(entity))
            except Exception:
                pass

        # Fill with synthetic
        remaining = self.n_agents - len(self.personas)
        for _ in range(remaining // 3):
            self.personas.append(gen.generate_adversarial_persona("cartel_faction", 7))
        for _ in range(remaining // 3):
            self.personas.append(gen.generate_enforcement_persona("DEA", "JIATF-South"))
        for _ in range(remaining - 2 * (remaining // 3)):
            self.personas.append(gen.generate_civilian_persona("Region", "urban_middle_class"))

        # Build environment
        avg_risk = np.mean([e.get("risk_score", 5.0) for e in self.seed.seed_events]) if self.seed.seed_events else 5.0
        initial_state = WorldState(
            global_stability_index=max(0.1, 1.0 - avg_risk / 10.0),
            enforcement_pressure=0.3, violence_level=0.2, economic_stress=0.3,
        )

        env_class = MaritimeDomainEnvironment if self.domain == "maritime" else TerritorialDomainEnvironment
        env = env_class({})
        self.model = WorldFishModel(self.personas, env, initial_state, self.step_days)

    def run(self, n_steps: int = 50) -> SimulationResult:
        if not self.model:
            self.build()

        started = datetime.now(timezone.utc)
        history = []
        for step in range(n_steps):
            cont = self.model.step()
            ws = self.model.world_state
            history.append({
                "step": step, "days": ws.time_elapsed_days,
                "violence": ws.violence_level, "enforcement": ws.enforcement_pressure,
                "stability": ws.global_stability_index, "events": len(ws.events_this_step),
            })
            if not cont:
                break

        # Compile agent summaries
        summary = {}
        for agent in self.model.schedule.agents:
            counts: dict[str, int] = {}
            for entry in agent.action_history:
                a = entry["action"]
                counts[a] = counts.get(a, 0) + 1
            summary[agent.persona.persona_id] = {
                "name": agent.persona.name, "type": agent.persona.agent_type,
                "actions": counts, "total": len(agent.action_history),
            }

        return SimulationResult(
            simulation_id=self.simulation_id, seed_id=self.seed.seed_id,
            domain=self.domain, n_agents=len(self.personas),
            n_steps_completed=len(history),
            total_simulated_days=history[-1]["days"] if history else 0,
            started_at=started, completed_at=datetime.now(timezone.utc),
            state_history=history, agent_action_summary=summary,
        )
