---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Component/graph-processor
type: Component
label: Graph processor
summary: Kafka consumer that transforms events and upserts entities/relationships into Memgraph and PostGIS; includes event detection, correlation, and dead-letter handling.
state: active
kind: service
paths: ["processors/**"]
stability: evolving
created_by: agent:claude-code
method: generated
created_at: 2026-07-13
---

`processors/graph/` consumes the `mda.*` topics (AIS, sanctions, GDELT, UAS, GFW v3, …) and
writes typed nodes/relationships to Memgraph and spatial rows to PostGIS. Contains the
Kafka→Memgraph transform (`memgraph_transform.py`), cross-domain correlator, event/anomaly
detection (`event_processor.py`), and a DLQ processor. This is the write-path chokepoint between
the ingestion workers and the graph/spatial stores.
