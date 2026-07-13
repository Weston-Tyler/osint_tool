---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Lesson/offline-testable-optional-deps
type: Lesson
label: offline testable optional deps
summary: A subsystem that relies on heavy external services (LLM, message broker, DB, ABM framework) should
  treat them as optional, injectable dependencies so the pipeline runs and is deterministically testable
  offline; hard top-level imports of them make the code unimportable and untestable.
claim: A subsystem that relies on heavy external services (LLM, message broker, DB, ABM framework) should
  treat them as optional, injectable dependencies so the pipeline runs and is deterministically testable
  offline; hard top-level imports of them make the code unimportable and untestable.
state: draft
confidence: asserted
created_by: agent:claude-code
method: generated
created_at: '2026-07-13'
expires: '2026-08-12'
derived_from:
- aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Task/202607-worldfish-buildout
evidence: []
tags: []
learned_from: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Task/202607-worldfish-buildout
---

WorldFish (worldfish/) hard-imported mesa, ollama, and gqlalchemy at module top. None are installed in dev/CI (all three are stubbed in ci.yml), so simulation_engine could not even be imported, let alone unit-tested, and run() never produced output. The build-out replaced the hard mesa dependency with an internal random-activation scheduler, made ollama/gqlalchemy/kafka lazy, and injected the decision policy — yielding an offline, deterministic pipeline (fixed --rng-seed) with a real end-to-end unit test and a contract-faithful Ollama integration test (not the CI import stub). Lesson: keep the live dependency at the edge (injected client / lazy import / dry-run) and a deterministic default in the core. Violation shape: a new module that imports an LLM/DB/broker client at top level and can only be exercised against live infrastructure.
