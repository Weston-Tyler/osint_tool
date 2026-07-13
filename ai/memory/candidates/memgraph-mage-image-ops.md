---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Lesson/memgraph-mage-image-ops
type: Lesson
label: memgraph mage image ops
summary: The Memgraph MAGE image needs a real published tag, a long start_period (~180s) for module load,
  and has no nc — healthchecks must use bash /dev/tcp.
claim: The Memgraph MAGE image needs a real published tag, a long start_period (~180s) for module load,
  and has no nc — healthchecks must use bash /dev/tcp.
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

Operational fixes learned the hard way: be7e45b fix(memgraph): pin to real tag 3.9.0 (1.22-memgraph-2.18 does not exist); deda179 use stable mage tag + proper --memory-limit flag; 5075e57 fix(memgraph): bump start_period to 180s for mage module load; cd13796 fix(memgraph): healthcheck uses bash /dev/tcp (nc not in mage image). Also 375b8be set MEMGRAPH_HOST/PORT env vars for the processor. Violation shape: inventing a memgraph/mage tag, setting a short healthcheck start_period, or using nc/curl in a memgraph healthcheck.
