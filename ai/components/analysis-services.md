---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Component/analysis-services
type: Component
label: Analysis & export services
summary: Downstream services — causal extraction/learning, corporate/shell/TCO detection, entity resolution, NLP, market & humanitarian monitors, and exporters (CoT/TAK, RDF, STIX, Elasticsearch, GeoServer).
state: active
kind: subsystem
paths: ["services/**"]
stability: evolving
created_by: agent:claude-code
method: generated
created_at: 2026-07-13
---

Higher-order processing and interoperability layer that reads the graph/spatial/search stores
and produces derived intelligence or exports to external systems. Groups: causal analysis
(`causal_extractor`, `causal_graph_learner`, `market_causal`, `stardog_exporter`), corporate
network analysis (`corporate/*`: entity_resolver, ownership_api, shell_detector, tco_detector),
vessel entity resolution (Splink), NLP/NER, humanitarian monitoring, intel products, and
exporters (`cot-publisher`, `rdf-exporter`, `stix-export`, `es-sync`, `geoserver-sync`,
`atak_overlay`). Each service is independently containerized.
