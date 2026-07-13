---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Component/worldfish
type: Component
label: WorldFish simulation
summary: The predictive-event foundation — a Mesa agent-based simulation (50–500 agents) with Ollama LLM-driven personas, seeded from real intelligence data to forecast maritime/actor behavior and generate forward events. WIP.
state: active
kind: subsystem
paths: ["worldfish/**"]
stability: evolving
created_by: agent:claude-code
method: generated
created_at: 2026-07-13
---

`simulation_engine.py` runs a Mesa agent model where each agent's decisions are driven by an
Ollama LLM using generated personas (`persona_generator.py`) and an agent memory store
(`memory.py`). Seeds are extracted from real data (`seed_extractor.py`), and outputs feed causal
predictions (`prediction.py`). Depends on Ollama (`OLLAMA_BASE_URL`) and the `mesa` package —
both stubbed in CI, so this subsystem is not exercised by the unit suite.

**Operator flag (2026-07-13): this is the system's predictive-event foundation — not a
throwaway spike.** The integration is not yet fully built out and must be completed before any
predictive-event feature is relied on; treat changes here as high-importance and strategically
central. The current lack of test coverage (CI-stubbed) is a gap to close as it matures, not a
sign it is peripheral. See candidate `worldfish-predictive-event-foundation`.
