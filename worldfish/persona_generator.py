"""OBI Agent Persona Generator for WorldFish.

Synthesises rich agent personas from OBI entity objects using a local Ollama
LLM, so that each Mesa agent in the simulation has a coherent identity,
motivation structure, and behavioural profile.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger("worldfish.persona_generator")

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class AgentPersona:
    """Full behavioural profile for one simulation agent."""

    persona_id: str
    obi_object_id: str
    agent_type: str  # e.g. "cartel_faction", "enforcement_dea", "civilian"
    name: str
    description: str
    motivation: str
    decision_style: str  # "aggressive", "cautious", "opportunistic", ...
    risk_tolerance: float  # 0.0 (risk-averse) .. 1.0 (risk-seeking)
    cooperation_propensity: float  # 0.0 (lone wolf) .. 1.0 (highly cooperative)
    information_access: str  # "full", "partial", "minimal"
    domain_attributes: dict[str, Any] = field(default_factory=dict)
    resources: list[str] = field(default_factory=list)
    capabilities: list[str] = field(default_factory=list)
    allied_with: list[str] = field(default_factory=list)
    opposed_to: list[str] = field(default_factory=list)
    initial_memories: list[str] = field(default_factory=list)
    system_prompt: str = ""


# ---------------------------------------------------------------------------
# Ollama helper
# ---------------------------------------------------------------------------


class _OllamaClient:
    """Thin wrapper around the Ollama HTTP generate endpoint."""

    def __init__(self, base_url: str, model: str, timeout: int) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout

    def generate(self, prompt: str, system: str = "") -> str:
        """Send a generation request and return the full response text."""
        payload: dict[str, Any] = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.7, "num_predict": 2048},
        }
        if system:
            payload["system"] = system

        resp = requests.post(
            f"{self.base_url}/api/generate",
            json=payload,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json().get("response", "")

    def generate_json(self, prompt: str, system: str = "") -> dict[str, Any]:
        """Generate and attempt to parse as JSON."""
        raw = self.generate(prompt, system)
        # Ollama may wrap JSON in markdown fences
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[-1]
        if cleaned.endswith("```"):
            cleaned = cleaned.rsplit("```", 1)[0]
        cleaned = cleaned.strip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            logger.warning("Failed to parse LLM output as JSON; returning raw text")
            return {"raw": raw}


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

_PERSONA_SYSTEM_PROMPT = """\
You are an intelligence analyst AI that creates detailed agent personas for
conflict / narcotrafficking simulations. You output ONLY valid JSON with
these exact keys: name, description, motivation, decision_style,
risk_tolerance (float 0-1), cooperation_propensity (float 0-1),
information_access, domain_attributes (object), resources (list of strings),
capabilities (list of strings), initial_memories (list of short sentences),
system_prompt (string: the instruction prompt that will drive this agent
during simulation). Be creative, plausible, and grounded in real-world
intelligence patterns. Do NOT include any commentary outside the JSON."""


class OBIAgentPersonaGenerator:
    """Generate :class:`AgentPersona` instances from OBI entity objects.

    Parameters
    ----------
    ollama_url : str
        Ollama server base URL.
    model : str
        Model name available in Ollama (e.g. ``"llama3"``).
    timeout : int
        HTTP timeout in seconds for Ollama requests.
    """

    def __init__(
        self,
        ollama_url: str = "http://localhost:11434",
        model: str = "llama3",
        timeout: int = 120,
    ) -> None:
        self._llm = _OllamaClient(ollama_url, model, timeout)
        logger.info(
            "OBIAgentPersonaGenerator using Ollama model '%s' at %s",
            model,
            ollama_url,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_from_obi_object(
        self,
        obi_object: dict[str, Any],
        agent_type: str = "generic",
        additional_context: str = "",
    ) -> AgentPersona:
        """Create a persona from an arbitrary OBI entity/object dict.

        Parameters
        ----------
        obi_object : dict
            The OBI object properties (object_id, object_type, name, etc.).
        agent_type : str
            High-level agent category.
        additional_context : str
            Free-text context to feed the LLM for richer persona generation.
        """
        object_id = obi_object.get("object_id", str(uuid.uuid4()))
        obj_name = obi_object.get("name", "Unknown Entity")
        obj_type = obi_object.get("object_type", "UNKNOWN")
        obj_desc = obi_object.get("description", "")
        obj_faction = obi_object.get("faction", "")
        obj_role = obi_object.get("role", "")
        obj_country = obi_object.get("country", "")

        prompt = (
            f"Generate a detailed agent persona for the following OBI entity.\n\n"
            f"Entity name: {obj_name}\n"
            f"Entity type: {obj_type}\n"
            f"Description: {obj_desc}\n"
            f"Faction: {obj_faction}\n"
            f"Role: {obj_role}\n"
            f"Country: {obj_country}\n"
            f"Agent type for simulation: {agent_type}\n"
        )
        if additional_context:
            prompt += f"Additional context: {additional_context}\n"

        prompt += "\nReturn ONLY valid JSON."

        data = self._llm.generate_json(prompt, system=_PERSONA_SYSTEM_PROMPT)

        return self._build_persona(
            data=data,
            obi_object_id=object_id,
            agent_type=agent_type,
            fallback_name=obj_name,
        )

    def generate_adversarial_persona(
        self,
        faction_type: str,
        obi_object: dict[str, Any] | None = None,
        scenario_context: str = "",
    ) -> AgentPersona:
        """Generate an adversarial (cartel / smuggling) persona.

        Parameters
        ----------
        faction_type : str
            One of ``"cartel_faction"``, ``"smuggling_network"``, or any custom
            adversarial label.
        obi_object : dict, optional
            If available, OBI entity properties.
        scenario_context : str
            Description of the scenario for grounding.
        """
        obi_object = obi_object or {}
        object_id = obi_object.get("object_id", str(uuid.uuid4()))
        obj_name = obi_object.get("name", "")

        faction_descriptions = {
            "cartel_faction": (
                "a senior operational leader of a Mexican drug cartel. "
                "They control territory, logistics corridors, and engage in "
                "violence, bribery, and extortion to maintain dominance."
            ),
            "smuggling_network": (
                "a coordinator within a maritime smuggling network that "
                "moves narcotics and contraband via go-fast boats, fishing "
                "vessels, and semi-submersibles. They specialise in evasion "
                "of coast guard patrols and AIS manipulation."
            ),
        }

        desc = faction_descriptions.get(
            faction_type,
            f"a member of the adversarial faction '{faction_type}'.",
        )

        prompt = (
            f"Generate a detailed simulation persona for {desc}\n\n"
            f"Faction type: {faction_type}\n"
        )
        if obj_name:
            prompt += f"Entity name hint: {obj_name}\n"
        if scenario_context:
            prompt += f"Scenario context: {scenario_context}\n"
        prompt += "\nReturn ONLY valid JSON."

        data = self._llm.generate_json(prompt, system=_PERSONA_SYSTEM_PROMPT)

        return self._build_persona(
            data=data,
            obi_object_id=object_id,
            agent_type=faction_type,
            fallback_name=obj_name or faction_type.replace("_", " ").title(),
        )

    def generate_enforcement_persona(
        self,
        agency: str,
        obi_object: dict[str, Any] | None = None,
        scenario_context: str = "",
    ) -> AgentPersona:
        """Generate a law-enforcement / military persona.

        Parameters
        ----------
        agency : str
            One of ``"DEA"``, ``"USCG"``, ``"MEXICO_SEDENA"``, or custom.
        obi_object : dict, optional
            If available, OBI entity properties.
        scenario_context : str
            Scenario description.
        """
        obi_object = obi_object or {}
        object_id = obi_object.get("object_id", str(uuid.uuid4()))
        obj_name = obi_object.get("name", "")

        agency_descriptions = {
            "DEA": (
                "a US Drug Enforcement Administration special agent operating "
                "in the Eastern Pacific or Caribbean. They coordinate with "
                "foreign partners and use intelligence-driven interdiction."
            ),
            "USCG": (
                "a US Coast Guard tactical action officer responsible for "
                "maritime law enforcement patrols, boarding operations, and "
                "counter-narcotics interdiction in the transit zone."
            ),
            "MEXICO_SEDENA": (
                "a Mexican SEDENA (Secretaría de la Defensa Nacional) field "
                "commander conducting counter-narcotics and checkpoint "
                "operations in cartel-contested territory."
            ),
        }

        desc = agency_descriptions.get(
            agency.upper(),
            f"a law-enforcement officer from agency '{agency}'.",
        )

        prompt = (
            f"Generate a detailed simulation persona for {desc}\n\n"
            f"Agency: {agency}\n"
        )
        if obj_name:
            prompt += f"Entity name hint: {obj_name}\n"
        if scenario_context:
            prompt += f"Scenario context: {scenario_context}\n"
        prompt += "\nReturn ONLY valid JSON."

        data = self._llm.generate_json(prompt, system=_PERSONA_SYSTEM_PROMPT)

        return self._build_persona(
            data=data,
            obi_object_id=object_id,
            agent_type=f"enforcement_{agency.lower()}",
            fallback_name=obj_name or f"{agency} Agent",
        )

    def generate_civilian_persona(
        self,
        obi_object: dict[str, Any] | None = None,
        role_hint: str = "local civilian",
        scenario_context: str = "",
    ) -> AgentPersona:
        """Generate a civilian / non-combatant persona.

        Parameters
        ----------
        obi_object : dict, optional
            OBI entity properties.
        role_hint : str
            A short description of the civilian's role (e.g. "fisherman",
            "port worker", "local mayor").
        scenario_context : str
            Scenario description.
        """
        obi_object = obi_object or {}
        object_id = obi_object.get("object_id", str(uuid.uuid4()))
        obj_name = obi_object.get("name", "")

        prompt = (
            f"Generate a detailed simulation persona for a civilian who is a "
            f"{role_hint} living in an area affected by cartel violence and "
            f"narcotics trafficking. They are not a combatant but may be "
            f"coerced, bribed, or serve as an informant.\n\n"
            f"Role: {role_hint}\n"
        )
        if obj_name:
            prompt += f"Entity name hint: {obj_name}\n"
        if scenario_context:
            prompt += f"Scenario context: {scenario_context}\n"
        prompt += "\nReturn ONLY valid JSON."

        data = self._llm.generate_json(prompt, system=_PERSONA_SYSTEM_PROMPT)

        return self._build_persona(
            data=data,
            obi_object_id=object_id,
            agent_type="civilian",
            fallback_name=obj_name or role_hint.title(),
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _build_persona(
        data: dict[str, Any],
        obi_object_id: str,
        agent_type: str,
        fallback_name: str,
    ) -> AgentPersona:
        """Construct an :class:`AgentPersona` from LLM-generated JSON."""

        def _float_clamp(val: Any, default: float) -> float:
            try:
                return max(0.0, min(1.0, float(val)))
            except (TypeError, ValueError):
                return default

        return AgentPersona(
            persona_id=str(uuid.uuid4()),
            obi_object_id=obi_object_id,
            agent_type=agent_type,
            name=data.get("name", fallback_name),
            description=data.get("description", ""),
            motivation=data.get("motivation", ""),
            decision_style=data.get("decision_style", "cautious"),
            risk_tolerance=_float_clamp(data.get("risk_tolerance"), 0.5),
            cooperation_propensity=_float_clamp(
                data.get("cooperation_propensity"), 0.5
            ),
            information_access=data.get("information_access", "partial"),
            domain_attributes=data.get("domain_attributes", {}),
            resources=data.get("resources", []),
            capabilities=data.get("capabilities", []),
            allied_with=data.get("allied_with", []),
            opposed_to=data.get("opposed_to", []),
            initial_memories=data.get("initial_memories", []),
            system_prompt=data.get("system_prompt", ""),
        )
