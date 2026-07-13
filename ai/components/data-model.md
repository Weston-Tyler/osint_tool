---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Component/data-model
type: Component
label: Data model & schemas
summary: Canonical data model — Memgraph Cypher graph schema, PostgreSQL/PostGIS relational schema, Elasticsearch index mappings, and RDF/OWL ontologies.
state: active
kind: subsystem
paths: ["schema/**", "ontology/**"]
stability: stable
created_by: agent:claude-code
method: generated
created_at: 2026-07-13
---

Declarative definitions the running system is built against. `schema/graph/` holds the Memgraph
init and expansion Cypher (base, causal, corporate-ownership); `schema/relational/` the
PostgreSQL + PostGIS DDL; `schema/elasticsearch/` the index mappings; `ontology/` the RDF/OWL
turtle files (causal, corporate-ownership) used by the RDF/Stardog exporters. Changes here ripple
into the graph-processor, API, and export services — treat as an interface, not an
implementation detail.
