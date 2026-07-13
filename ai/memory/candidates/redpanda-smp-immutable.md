---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Lesson/redpanda-smp-immutable
type: Lesson
label: redpanda smp immutable
summary: Redpanda smp (core count) cannot be decreased on an existing data volume without wiping it, so
  smp must stay at its provisioned value (4).
claim: Redpanda smp (core count) cannot be decreased on an existing data volume without wiping it, so
  smp must stay at its provisioned value (4).
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

969c917 fix(redpanda): keep smp=4 (cannot decrease without wiping volume). Related: c18d6e9 fix(redpanda): raise partition limit. This is a stateful-broker constraint: lowering smp against a populated volume corrupts/blocks startup. Violation shape: editing docker-compose to lower the redpanda smp value on a deployment that already has data volumes.
