# Counter-UAS Deep Expansion Plan
## Expansion 5 of the Open-Source World Knowledge Graph Series
### Architect-Level Technical Reference — April 2026

> **Series Context:** This is the fifth deep expansion for a senior systems architect building an open-source world knowledge graph. Previous expansions: (1) MDA maritime/cartel/counter-narcotics, (2) World events + ODNI OBI + social media OSINT, (3) Causal chain analysis + WorldFish simulation engine, (4) Global corporate ownership + business registries. This expansion treats counter-UAS intelligence as a first-class domain — not a footnote to the MDA layer.
>
> **Existing stack:** Memgraph (Cypher) + PostgreSQL/PostGIS + Kafka/Redpanda + Stardog (RDF/OWL/SPARQL) + GeoServer + MinIO + Elasticsearch + FastAPI + Antigravity IDE multi-agent system. Mac mini (20-core, 64GB RAM). RTL-SDR, ATAK/CoT, tactical sensors, BLE mesh, LoRa, ESP32 experience assumed throughout.

---

# SECTION 1: THE C-UAS INTELLIGENCE LANDSCAPE

## 1.1 Why C-UAS Requires Its Own Deep Data Layer

Counter-UAS intelligence is not a subcategory of air defense or border security. It is a cross-domain problem that sits at the intersection of supply chain intelligence, signals intelligence, behavioral analytics, export control law, and kinetic threat assessment — all operating simultaneously across threat actors ranging from lone hobbyists to peer-state militaries. The previous expansions already laid basic C-UAS nodes (`UAS_Detection_Event`, `Drone`, `Flight_Path`, `Sensor_Node`, `Countermeasure_Event`). This expansion makes those nodes the leaves of a far larger structure.

**The COTS-to-Military Continuum**

The core analytical challenge is the dual-use nature of almost every drone component. A DJI Mavic 3 costs $2,200 and is used by wedding photographers, precision agriculture companies, and search-and-rescue teams. The same airframe — or its core components — has been found modified for payload delivery by Mexican cartels, adapted for battlefield ISR by Ukrainian volunteer brigades, and reverse-engineered by Iranian engineers into the Shahed-131/136 loitering munition family that has struck Ukrainian cities, Israeli territory, and Saudi infrastructure. The motor winding, the flight controller firmware (ArduPilot is open source), the IMU chip (InvenSense MPU-6000 or equivalent), the LiPo battery chemistry, and the 2.4 GHz radio link protocol are identical or near-identical across the $200 hobbyist quad and the $20M military platform. This means that supply chain monitoring, export control enforcement, and threat identification must all share the same underlying data model.

Price bracket defines neither threat level nor technical sophistication. The key differentiator is payload capacity, range, survivability, and operator intent — all of which must be inferred from behavioral data combined with technical platform specifications.

**Sensor Heterogeneity Creates a Data Integration Problem**

Traditional air defense domains (fixed-wing aircraft, ballistic missiles) are dominated by radar as the primary sensor. Counter-UAS must fuse at minimum five detection modalities, each producing structurally different data:

- **RF detection** produces: frequency, bandwidth, modulation, MAC address or protocol fingerprint, signal power, bearing (if antenna array). No position without triangulation.
- **Radar** (FMCW or pulse-Doppler) produces: range, bearing, elevation, radial velocity (Doppler), and a micro-Doppler signature correlated with rotor rotation. High false-alarm rate against birds.
- **EO/IR cameras** produce: pixel bounding boxes, thermal signatures, estimated range (if stereo or known target size). Very high resolution but limited field of view and weather-dependent.
- **Acoustic sensors** produce: sound spectral features, estimated bearing. Works in low-visibility conditions but range is limited to 200–500m and is noise-environment sensitive.
- **Remote ID** (BLE/WiFi broadcast) produces: serial number, operator location, UAS GPS position, altitude, speed, heading, timestamp. Reliable only when the operator is compliant with regulations.

No single modality reliably detects all threat classes. Small slow drones evade radar. Autonomous pre-programmed drones emit no RF after launch. EO/IR cameras can't see through fog. This mandates a fusion layer — which is architecturally distinct from any previous expansion and requires its own pipeline, data model, and confidence scoring framework.

**Threat Actor Heterogeneity Requires Graph Traversal, Not Just Lookup**

The threat actor population spans six orders of magnitude in organizational sophistication:

1. **Lone recreational pilots** — accidental airspace violations, no malicious intent, low consequence
2. **Criminal enterprises (cartels, smuggling networks)** — drug/contraband delivery, reconnaissance, intimidation; growing sophistication; CJNG now fields FPV kamikaze drones
3. **Domestic extremists** — infrastructure disruption (power grids, pipelines), surveillance of law enforcement
4. **Terrorist organizations** — ISIS Mosul-era IED drones; Houthi maritime drone strikes; Hezbollah ISR
5. **Proxy state actors** — Iran supplying Shahed-136 to Houthis and Russia; Chinese drone technology transfer to proxy forces
6. **Peer state militaries** — Russia (Lancet, KUB-BLA, Shahed derivative), China (Wing Loong, CH-series, TB-001), Turkey (Bayraktar TB2/Akıncı), USA (MQ-9, RQ-4, MQ-25)

A single knowledge graph can link a cartel drone incident in Sonora to its component manufacturer in Shenzhen, to a shell company import record in Laredo, to a TikTok video posted by a cartel operative showing the drone modification workshop — but only if the schema is designed to support that traversal from the start. That is what this expansion builds.

**The C-UAS Market Context**

The counter-UAS industry has grown from a niche defense curiosity to a $1.5B+ annual market with 550+ C-UAS companies and 1,000+ distinct systems cataloged by Unmanned Airspace. This proliferation itself creates intelligence value: procurement decisions reveal capability gaps, vendor consolidation reveals technology bets, and contract awards reveal which nations and agencies are prioritizing which defeat mechanisms. The defense procurement layer (Section 6) adds this market intelligence dimension.

**How This Connects the Previous Four Expansions**

| Previous Layer | C-UAS Connection |
|---|---|
| MDA maritime + cartel | Cartel drone delivery across maritime borders; vessel-carried drone component shipments |
| World events + social OSINT | Drone strikes as causal events; social media intelligence on UAS deployment |
| Causal chain + WorldFish | Drone attacks as causal triggers; C-UAS countermeasure effectiveness as simulation parameter |
| Corporate ownership + registries | Drone manufacturer ownership; component supplier corporate tree; procurement contractor vetting |

---

## 1.2 The Six Pillars of C-UAS Data

This expansion is structured around six distinct data domains, each requiring its own ingestion pipeline, schema extensions, and cross-domain linkage strategy.

### Pillar 1: Technical System/Threat Catalogs

The foundational layer — what drones and C-UAS systems exist, what can they do, and how do they relate to each other. Data sources include the Unmanned Airspace C-UAS Directory (550+ systems), DSIAC technical inquiry reports, the DIU Blue UAS cleared list, Wikidata UAS articles, and DroneSec threat intelligence. The output is `UAS_Platform` and `CUAS_System` nodes with full specification properties.

### Pillar 2: Supply Chain & Trade Data

Who manufactures drone components, where they ship, at what volume, and through which intermediaries. Data sources include UN Comtrade (bilateral trade flows by HS code), US Census foreign trade data (port-level US imports), OEC (macro trends), and SIPRI Arms Transfers Database (military platform transfers). The HS code taxonomy for drone-relevant goods is the analytical key — mapped in Section 4.1. Output is `Trade_Flow` nodes with anomaly-detection scoring.

### Pillar 3: Export Control & Regulatory Data

Which components are controlled, by which regimes, under what technical thresholds. US EAR Commerce Control List (ECCNs 9A012, 9A610, 7A003, 7A103, 6A003, 5A001), ITAR US Munitions List (Categories VIII, XI, XII), EU Dual-Use Regulation 2021/821, China MOFCOM July 2024 drone controls, and the Wassenaar Arrangement. Output is `Export_Control_Entry` nodes cross-referenced to component types, enabling automated compliance checking on every trade flow.

### Pillar 4: Defense Procurement & Contract Data

Government C-UAS purchases reveal real-world deployment decisions, capability requirements, and vendor market positions. Data sources include FPDS, USASpending.gov, SAM.gov, UK Defence Contracts Online, and EU TED. Output is `Defense_Contract` nodes linked to contractor `Company` nodes from the corporate ownership graph.

### Pillar 5: Detection Sensor & ML Training Data

The raw material for building detection models — RF signal datasets, EO/IR video, acoustic recordings, radar tracks, labeled for drone classification. Key open datasets: Anti-UAV benchmark, VisDrone, CUAS, DroneRF, Noisy Drone RF on Kaggle, UETT4K, DroneDetectionDataset. Stored in MinIO, indexed in Elasticsearch, metadata in Memgraph as `RF_Signature` nodes and dataset registry entries.

### Pillar 6: Operational/TTP/Incident Data

Observed real-world events — airport incursions, cartel drone deliveries, warzone strikes, prison drone drops. Data sources include FAA UAS sighting reports (20,000+ records), DroneSec weekly intelligence briefs via RSS, ACLED conflict event data filtered for drone events, LiveUAMap, and social OSINT (TikTok narco-drone content, YouTube, Telegram). Output is `CUAS_Incident` nodes with attribution, platform, and countermeasure links.

---

> **NOTE:** The remaining sections (2-11 plus appendices) — ~50K characters
> of Cypher schema, Python ingesters, Docker compose definitions, Kafka topic
> lists, and cross-graph intelligence queries — are preserved in the project
> chat history under the original author's architectural reference document.
> They will be reproduced into this file when implementation begins, section
> by section as each phase is executed. This avoids the overhead of shipping
> ~60K chars of unimplemented reference material into every repo clone while
> still preserving the full plan.
>
> **Sections to be reproduced when implemented:**
>
> - Section 2: Extended C-UAS graph schema (UAS_Platform, UAS_Component,
>   CUAS_System, RF_Signature, Trade_Flow, Export_Control_Entry,
>   CUAS_Incident, Defense_Contract, TTP) + complete relationship schema
> - Section 3: Technical system and threat catalogs
>   (Unmanned Airspace directory, DSIAC, Blue UAS, Wikidata)
> - Section 4: Supply chain and trade data (HS code taxonomy, UN Comtrade,
>   SIPRI, supply-chain anomaly detection with 6 rules)
> - Section 5: Export control and regulatory data (EAR CCL, China MOFCOM,
>   unified taxonomy, license matrix)
> - Section 6: Defense procurement and contract data (FPDS, USASpending,
>   cross-graph procurement intelligence queries)
> - Section 7: Detection and ML training datasets (open RF/EO/IR catalogs,
>   RTL-SDR pipeline, Open Drone ID receiver)
> - Section 8: Operational/TTP/incident data (FAA sightings, TikTok
>   narco-drone monitor, cartel TTP taxonomy)
> - Section 9: Multi-sensor fusion architecture (Kalman filter, FOF
>   classification, track management)
> - Section 10: Integration with existing system (cross-graph intelligence
>   thread queries, ATAK CoT, docker-compose.cuas.yml, Antigravity agent
>   roles)
> - Section 11: Phased implementation roadmap (Weeks 1-16 with benchmarks)
> - Appendix A: Environment variables reference
> - Appendix B: External sources summary (35+ data sources with URLs,
>   access tiers, update frequencies)
