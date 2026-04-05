"""Domain-specific simulation environments for WorldFish."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ActionType(str, Enum):
    MARITIME_TRANSIT = "maritime_transit"
    STS_TRANSFER = "sts_transfer"
    PORT_ENTRY = "port_entry"
    AIS_DISABLE = "ais_disable"
    FLAG_CHANGE = "flag_change"
    EVADE_PATROL = "evade_patrol"
    REPORT_ANOMALY = "report_anomaly"
    TERRITORIAL_EXPANSION = "territorial_expansion"
    ALLIANCE_FORMATION = "alliance_formation"
    ALLIANCE_BREAK = "alliance_break"
    ATTACK = "attack"
    RETREAT = "retreat"
    BRIBE_OFFICIAL = "bribe_official"
    RECRUIT = "recruit"
    BUY_COMMODITY = "buy_commodity"
    SELL_COMMODITY = "sell_commodity"
    ACTIVATE_SUPPLY_CHAIN = "activate_supply_chain"
    DISRUPT_SUPPLY_CHAIN = "disrupt_supply_chain"
    COMMUNICATE = "communicate"
    GATHER_INTELLIGENCE = "gather_intelligence"
    WAIT = "wait"
    RESPOND_TO_PRESSURE = "respond_to_pressure"


@dataclass
class WorldState:
    step: int = 0
    time_elapsed_days: float = 0.0
    maritime_state: dict = field(default_factory=dict)
    territorial_state: dict = field(default_factory=dict)
    market_state: dict = field(default_factory=dict)
    humanitarian_state: dict = field(default_factory=dict)
    global_stability_index: float = 0.5
    enforcement_pressure: float = 0.3
    violence_level: float = 0.2
    economic_stress: float = 0.3
    events_this_step: list[dict] = field(default_factory=list)


class SimulationEnvironment(ABC):
    @abstractmethod
    def get_available_actions(self, agent_state: dict) -> list[ActionType]:
        pass

    @abstractmethod
    def apply_action(self, agent_id: str, action: ActionType, params: dict, state: WorldState) -> tuple[WorldState, dict]:
        pass

    @abstractmethod
    def get_agent_observation(self, agent_id: str, state: WorldState) -> dict:
        pass

    @abstractmethod
    def check_terminal(self, state: WorldState) -> tuple[bool, str]:
        pass


class MaritimeDomainEnvironment(SimulationEnvironment):
    def __init__(self, seed_data: dict):
        self.ports = seed_data.get("ports", {})

    def get_available_actions(self, agent_state: dict) -> list[ActionType]:
        actions = [ActionType.WAIT, ActionType.COMMUNICATE, ActionType.GATHER_INTELLIGENCE]
        at = agent_state.get("agent_type", "")
        if "vessel" in at or "smuggling" in at:
            actions.extend([ActionType.MARITIME_TRANSIT, ActionType.PORT_ENTRY, ActionType.EVADE_PATROL])
            if agent_state.get("risk_tolerance", 0) > 0.5:
                actions.extend([ActionType.AIS_DISABLE, ActionType.STS_TRANSFER, ActionType.FLAG_CHANGE])
        if "enforcement" in at or "coast_guard" in at:
            actions.extend([ActionType.MARITIME_TRANSIT, ActionType.REPORT_ANOMALY])
        return actions

    def apply_action(self, agent_id: str, action: ActionType, params: dict, state: WorldState) -> tuple[WorldState, dict]:
        result = {"success": True, "outcome": action.value, "detection_risk": 0.0}
        if action == ActionType.AIS_DISABLE:
            result["detection_risk"] = 0.2 + 0.5 * state.enforcement_pressure
            state.maritime_state[f"ais_gap_{agent_id}"] = True
            state.enforcement_pressure = min(1.0, state.enforcement_pressure + 0.05)
        elif action == ActionType.STS_TRANSFER:
            loc = params.get("location", "unknown")
            enf = state.maritime_state.get(f"enforcement_in_{loc}", 0.0)
            result["success_probability"] = max(0.1, 0.8 - 0.5 * enf)
            result["detection_risk"] = enf * 0.7
        elif action == ActionType.MARITIME_TRANSIT:
            result["travel_time_hours"] = params.get("travel_hours", 24)
        state.events_this_step.append({"agent_id": agent_id, "action": action.value, "result": result})
        return state, result

    def get_agent_observation(self, agent_id: str, state: WorldState) -> dict:
        return {
            "enforcement_pressure": state.enforcement_pressure,
            "ais_gaps": [k for k, v in state.maritime_state.items() if "ais_gap" in k and v],
        }

    def check_terminal(self, state: WorldState) -> tuple[bool, str]:
        if state.enforcement_pressure >= 0.95:
            return True, "Maximum enforcement pressure"
        return False, ""


class TerritorialDomainEnvironment(SimulationEnvironment):
    def __init__(self, seed_data: dict):
        self.territories = seed_data.get("faction_territories", {})

    def get_available_actions(self, agent_state: dict) -> list[ActionType]:
        actions = [ActionType.WAIT, ActionType.COMMUNICATE, ActionType.GATHER_INTELLIGENCE]
        at = agent_state.get("agent_type", "")
        if "cartel" in at:
            actions.extend([ActionType.TERRITORIAL_EXPANSION, ActionType.ALLIANCE_FORMATION,
                           ActionType.ALLIANCE_BREAK, ActionType.ATTACK, ActionType.RETREAT,
                           ActionType.BRIBE_OFFICIAL, ActionType.RECRUIT])
        if "enforcement" in at or "military" in at:
            actions.extend([ActionType.ATTACK, ActionType.REPORT_ANOMALY])
        if "civilian" in at:
            actions.append(ActionType.RESPOND_TO_PRESSURE)
        return actions

    def apply_action(self, agent_id: str, action: ActionType, params: dict, state: WorldState) -> tuple[WorldState, dict]:
        result = {"success": True, "outcome": action.value}
        if action == ActionType.TERRITORIAL_EXPANSION:
            target = params.get("target_area", "unknown")
            military = state.territorial_state.get(f"military_{target}", 0.0)
            rival = state.territorial_state.get(f"rival_{target}", 0.0)
            success = (0.7 - 0.4 * military - 0.3 * rival) > 0.3
            result["success"] = success
            if success:
                state.territorial_state[f"controlled_{target}"] = agent_id
                state.violence_level = min(1.0, state.violence_level + 0.1)
        elif action == ActionType.ATTACK:
            state.violence_level = min(1.0, state.violence_level + 0.2)
            state.humanitarian_state["displacement_pressure"] = min(
                1.0, state.humanitarian_state.get("displacement_pressure", 0) + 0.05
            )
        elif action == ActionType.BRIBE_OFFICIAL:
            state.territorial_state[f"bribed_{params.get('official', '')}"] = True
        state.events_this_step.append({"agent_id": agent_id, "action": action.value, "result": result})
        return state, result

    def get_agent_observation(self, agent_id: str, state: WorldState) -> dict:
        return {
            "violence_level": state.violence_level,
            "enforcement_pressure": state.enforcement_pressure,
            "territories": [k for k, v in state.territorial_state.items() if "controlled" in k and v == agent_id],
        }

    def check_terminal(self, state: WorldState) -> tuple[bool, str]:
        if state.violence_level >= 0.9:
            return True, "Open warfare threshold"
        if state.enforcement_pressure >= 0.95:
            return True, "Military saturation"
        return False, ""
