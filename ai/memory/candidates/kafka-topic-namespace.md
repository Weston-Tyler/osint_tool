---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Convention/kafka-topic-namespace
type: Convention
label: kafka topic namespace
summary: Kafka/Redpanda topics are named mda.<domain>.<subtype> (e.g. mda.ais.positions.raw, mda.gfw.encounters);
  ingestion workers only produce and the graph-processor only consumes.
claim: Kafka/Redpanda topics are named mda.<domain>.<subtype> (e.g. mda.ais.positions.raw, mda.gfw.encounters);
  ingestion workers only produce and the graph-processor only consumes.
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

Topic list in processors/graph/graph_consumer.py TOPICS and Makefile gfw-topics shows the mda.<domain>.<subtype> scheme: mda.ais.positions.raw, mda.sanctions.updates, mda.uas.detections.raw, mda.events.gdelt.raw, mda.gfw.{encounters,loitering,port_visits,fishing,gaps,vessels,insights,sar_detections}. Components are decoupled through topics plus shared stores, not direct calls: no worker imports another worker, and only tests and api import across components. Violation shape: a topic name off-scheme, a worker consuming another worker output directly, or a worker writing to a store the graph-processor owns.
