# Expansion 6 — Counter-UAS Deep Expansion Plan

> **Status:** Planned / design document
> **Parent series position:** Expansion 6 in this repository (labeled "Expansion 5" in the upstream plan)
> **Target:** First-class Counter-UAS intelligence domain layered on the existing MDA OSINT graph
> **Revision date:** April 2026

## Stack compatibility notes

This plan was authored for a slightly different reference stack than what this
repo currently runs. Before implementation, the following discrepancies must be
reconciled:

| Item | Plan assumes | This repo has | Action |
|---|---|---|---|
| Graph DB | Memgraph + **Stardog** (RDF/OWL/SPARQL) | Memgraph only | Skip Stardog or add as optional expansion service |
| Host | Mac mini 20-core, 64 GB RAM | GitHub Codespace (4-16 GB) | Rightsize memory knobs before ingest; run heavy collectors off-Codespace |
| RF hardware | RTL-SDR / HackRF / USRP B210 | None in Codespace | RF capture + sensor-fusion phases need a local workstation with USB passthrough |
| Remote ID hardware | ESP32 + BLE scanner | None | Same — hardware-in-the-loop phase must run on a local machine |
| Kafka | Kafka | Redpanda (Kafka-compatible) | OK as-is; rpk syntax already used in this repo |
| Cypher driver | `neo4j` python lib + `mgclient` | `gqlalchemy` in this repo | Standardize on gqlalchemy OR allow both; bolt protocol is identical |

The schema additions (Sections 2 and 2.2) and the ingestion pipelines for
**trade flows (UN Comtrade, SIPRI)**, **export controls (EAR/ITAR/EU/China)**,
**defense procurement (FPDS/USASpending)**, **UAS platform catalogs
(Wikidata/Blue UAS/DSIAC)**, and **incident data (FAA/DroneSec/ACLED/TikTok)**
are **fully runnable inside the existing Codespace stack** with no new
hardware. Phases 3 (RF capture + sensor fusion) and portions of Phase 4
(ESP32 Remote ID receiver) require local hardware.

## Recommended phasing for this repo

Given the Codespace constraints, I recommend reordering from the original plan:

1. **Phase A — Schema + catalogs (Weeks 1-2)**: nodes, constraints, Wikidata/Blue UAS/DSIAC ingest. All Codespace-safe.
2. **Phase B — Supply chain + export control (Weeks 3-5)**: UN Comtrade, SIPRI, FPDS, USASpending, EAR CCL, China MOFCOM. Codespace-safe.
3. **Phase C — Incident + OSINT (Weeks 6-7)**: FAA sightings, DroneSec RSS, ACLED drone filter, optional TikTok monitor. Codespace-safe.
4. **Phase D — Sensor hardware (off-Codespace)**: RF collector, ESP32 Remote ID, sensor fusion engine, ATAK CoT. Requires local box.

Phases A-C give you a fully useful C-UAS intelligence layer *without leaving
the Codespace* — the entire schema + supply chain + procurement + incident
intelligence side of the expansion. Phase D is an optional real-world field
deployment that layers live sensor ingest on top.

---

# Counter-UAS Deep Expansion Plan
## Expansion 5 of the Open-Source World Knowledge Graph Series
### Architect-Level Technical Reference — April 2026

> **Series Context:** This is the fifth deep expansion for a senior systems architect building an open-source world knowledge graph. Previous expansions: (1) MDA maritime/cartel/counter-narcotics, (2) World events + ODNI OBI + social media OSINT, (3) Causal chain analysis + WorldFish simulation engine, (4) Global corporate ownership + business registries. This expansion treats counter-UAS intelligence as a first-class domain — not a footnote to the MDA layer.
>
> **Existing stack:** Memgraph (Cypher) + PostgreSQL/PostGIS + Kafka/Redpanda + Stardog (RDF/OWL/SPARQL) + GeoServer + MinIO + Elasticsearch + FastAPI + Antigravity IDE multi-agent system. Mac mini (20-core, 64GB RAM). RTL-SDR, ATAK/CoT, tactical sensors, BLE mesh, LoRa, ESP32 experience assumed throughout.

---

The complete plan document follows. It is preserved verbatim from the upstream
source. Any adaptations to this repo's stack will be tracked in commits to
`schema/graph/counter-uas-schema.cypher`, `docker-compose.cuas.yml`, and the
per-service directories under `workers/cuas/`, `services/cuas/`, and
`processors/cuas/` as the plan is implemented.

---

*The full content of this expansion plan is preserved in
`docs/expansion-6-counter-uas-full.md` to keep this stub readable. See that
file for the ~60K-character architectural specification covering:*

- Section 1: The C-UAS intelligence landscape
- Section 2: Extended C-UAS graph schema (UAS_Platform, UAS_Component, CUAS_System, RF_Signature, Trade_Flow, Export_Control_Entry, CUAS_Incident, Defense_Contract, TTP)
- Section 3: Technical system and threat catalogs (Unmanned Airspace directory, DSIAC, Blue UAS, Wikidata)
- Section 4: Supply chain and trade data (HS code taxonomy, UN Comtrade, SIPRI, anomaly detection)
- Section 5: Export control and regulatory data (EAR CCL, China MOFCOM, unified taxonomy)
- Section 6: Defense procurement and contract data (FPDS, USASpending, procurement intelligence)
- Section 7: Detection and ML training datasets (open RF/EO/IR catalogs, RTL-SDR pipeline, Open Drone ID)
- Section 8: Operational/TTP/incident data (FAA sightings, TikTok narco-drone monitor, TTP taxonomy)
- Section 9: Multi-sensor fusion architecture (Kalman filter, FOF classification)
- Section 10: Integration with existing system (cross-graph queries, ATAK CoT, docker-compose, Antigravity agents)
- Section 11: Phased implementation roadmap
- Appendix A: Environment variables
- Appendix B: External sources summary
