---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Convention/compose-batch-profile-oneshot
type: Convention
label: compose batch profile oneshot
summary: One-shot ingesters run under the docker-compose 'batch' profile; only long-running daemons are
  always-on services.
claim: One-shot ingesters run under the docker-compose 'batch' profile; only long-running daemons are
  always-on services.
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

5a4ffe6 fix(compose): move worker-gdelt to batch profile (one-shot ingester); d2ee0b6 fix(workers): batch profile for one-shot ingesters + gdelt parser fix. Separating one-shot jobs from daemons stops them from being restarted forever by compose. Violation shape: wiring a one-shot ingester as an always-restart service (no batch profile), or putting a long-running consumer behind the batch profile so it never starts with the stack.
