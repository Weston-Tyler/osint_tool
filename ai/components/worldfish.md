---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Component/worldfish
type: Component
label: WorldFish simulation
summary: Mesa agent-based simulation (50–500 agents) with Ollama LLM-driven personas; seeds from real intelligence data to predict maritime/actor behavior.
state: active
kind: subsystem
paths: ["worldfish/**"]
stability: evolving
created_by: agent:claude-code
method: generated
created_at: 2026-07-13
---

Experimental predictive layer. `simulation_engine.py` runs a Mesa agent model where each agent's
decisions are driven by an Ollama LLM using generated personas (`persona_generator.py`) and an
agent memory store (`memory.py`). Seeds are extracted from real data (`seed_extractor.py`), and
outputs feed causal predictions (`prediction.py`). Depends on Ollama (`OLLAMA_BASE_URL`) and the
`mesa` package — both stubbed in CI, so this subsystem is not exercised by the unit suite.
