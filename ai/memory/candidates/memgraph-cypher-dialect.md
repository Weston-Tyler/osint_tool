---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Lesson/memgraph-cypher-dialect
type: Lesson
label: memgraph cypher dialect
summary: 'Memgraph Cypher is not identical to Neo4j: duration() uses singular unit keys (hour, not hours)
  and some values must be inlined rather than passed as parameters.'
claim: 'Memgraph Cypher is not identical to Neo4j: duration() uses singular unit keys (hour, not hours)
  and some values must be inlined rather than passed as parameters.'
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

API query fixes: e5b9d4b fix(api): memgraph duration() uses singular 'hour' key not 'hours'; c3d80f3 fix(api): inline hours literal in uas detections Cypher. These bit the vessels/uas routers. Violation shape: writing Neo4j-style Cypher (plural duration keys, or parameterizing a value Memgraph requires inlined) in api/ or processors/ and assuming it runs on Memgraph.
