---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Decision/worldfish-predictive-event-foundation
type: Decision
label: worldfish predictive event foundation
summary: WorldFish is the designated foundation for the system's predictive-event capability and must
  be fully built out before predictive-event features are relied on; it is not an experimental throwaway.
claim: WorldFish is the designated foundation for the system's predictive-event capability and must be
  fully built out before predictive-event features are relied on; it is not an experimental throwaway.
state: draft
confidence: asserted
created_by: actor/weston-tyler
method: generated
created_at: '2026-07-13'
expires: '2026-08-12'
derived_from: []
evidence: []
tags: []
---

Operator statement (2026-07-13, Weston-Tyler): the WorldFish integration (worldfish/**) is the predictive event foundation of the platform. By artifact alone it reads as experimental — Mesa + Ollama, CI-stubbed, zero test coverage — but that is a maturity gap, not its status. Implication for agents: treat worldfish as strategically central and high-importance; do NOT deprecate or treat it as peripheral; changes here can affect the whole predictive-event roadmap. The integration is incomplete and needs to be fully worked out (personas, memory, seed extraction, prediction wiring, and test coverage) before downstream predictive features depend on it. Corrects the initial onboarding inference that worldfish was a throwaway spike.
