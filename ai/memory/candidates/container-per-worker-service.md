---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Convention/container-per-worker-service
type: Convention
label: container per worker service
summary: Each worker and service is its own Docker image with its own pinned requirements.txt; per-service
  dependencies go there, not in the root pyproject.toml.
claim: Each worker and service is its own Docker image with its own pinned requirements.txt; per-service
  dependencies go there, not in the root pyproject.toml.
state: draft
confidence: asserted
created_by: agent:claude-code
method: generated
created_at: '2026-07-13'
expires: '2026-08-12'
derived_from: []
evidence: []
tags: []
---

30 Dockerfiles and 26 requirements.txt files live under workers/ and services/, one per ingester/service (e.g. workers/gdelt/, workers/ais/, services/corporate/tco_detector/). Deps are added at the service that needs them: 727f7b1 fix(worker-ais): add gqlalchemy dependency for enrichment worker; bb0aebb fix(api): add missing kafka-python dependency. Root pyproject.toml carries only the shared/dev toolchain. Violation shape: adding a runtime dependency to pyproject.toml for one worker, or a worker with no requirements.txt/Dockerfile of its own.
