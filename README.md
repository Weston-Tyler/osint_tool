# MDA OSINT Database

**Maritime Domain Awareness — Open-Source Intelligence Database**

A fully open-source, graph-native intelligence database for Maritime Domain Awareness (MDA) across four operational domains: illicit maritime activity, cartel/drug trafficking networks, counter-narcotics operations, and counter-UAS in maritime/border environments.

## Architecture

- **Graph DB**: Memgraph (in-memory, Cypher, native Kafka streaming)
- **Relational + Geo**: PostgreSQL 16 + PostGIS 3.4 + Apache AGE
- **Search**: Elasticsearch 8 (full-text, fuzzy name matching)
- **Message Broker**: Redpanda (Kafka-compatible, single binary)
- **Object Storage**: MinIO (S3-compatible, air-gappable)
- **GIS Server**: GeoServer (WMS/WFS/WCS for operational maps)
- **API**: FastAPI (async Python, REST + WebSocket)
- **Entity Resolution**: Splink (probabilistic record linkage)
- **Dashboards**: Grafana + Kepler.gl

## Open Data Sources

| Source | Coverage | Update Frequency |
|---|---|---|
| [NOAA AIS (MarineCadastre)](https://marinecadastre.gov/downloads/data/ais/) | US waters | Monthly bulk |
| [Global Fishing Watch](https://globalfishingwatch.org/our-apis/) | Global | 6-hourly to daily |
| [OFAC SDN](https://ofac.treasury.gov/sanctions-list-service) | Global sanctions | Daily |
| [OpenSanctions](https://www.opensanctions.org) | 40+ lists, 1453+ vessels | Daily |
| [GDELT](https://analysis.gdeltproject.org/module-event-exporter.html) | Global events | Every 15 min |
| [FAA UAS Sightings](https://commons.erau.edu/db-aeronautical-science/6/) | US airspace | FOIA releases |
| [Open Drone ID](https://mavlink.io/en/services/opendroneid.html) | Local sensor range | Real-time |

## Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/weston-tyler/osint_tool.git
cd osint_tool
cp .env.example .env
# Edit .env with strong passwords

# 2. Start all services
make setup

# 3. Verify
curl http://localhost:8000/health

# 4. Ingest initial data
make ingest-ofac
make ingest-gdelt
```

## Service Ports

| Service | Port | Purpose |
|---|---|---|
| Memgraph Lab | 3000 | Graph exploration UI |
| Memgraph Bolt | 7687 | Cypher queries |
| PostgreSQL | 5432 | SQL + PostGIS |
| Redpanda | 9092/19092 | Kafka API |
| Redpanda Console | 8080 | Kafka UI |
| MinIO | 9000/9001 | S3 API / Console |
| Elasticsearch | 9200 | Search API |
| GeoServer | 8085 | WMS/WFS/WCS |
| FastAPI | 8000 | REST + WebSocket |
| Grafana | 3001 | Dashboards |

## API Endpoints

```
GET  /health                           # System health check
GET  /v1/vessels                       # Search vessels (filters: name, flag, risk, sanctions)
GET  /v1/vessels/{imo_or_mmsi}         # Vessel detail + ownership + sanctions + gaps
GET  /v1/vessels/{id}/network          # N-hop network traversal
GET  /v1/vessels/{id}/track            # Vessel track from PostGIS
GET  /v1/uas/detections                # Recent UAS detection events
GET  /v1/uas/flight-paths              # UAS flight paths from PostGIS
GET  /v1/uas/sensors                   # Registered sensor nodes
GET  /v1/analytics/sanctioned-network  # Sanctioned entity network
GET  /v1/analytics/risk-summary        # High-risk vessel summary
GET  /v1/analytics/interdiction-heatmap # Clustered interdiction events
GET  /v1/analytics/ais-gaps-summary    # AIS gap statistics
```

## Project Structure

```
mda-system/
├── api/                    # FastAPI application
│   ├── main.py
│   ├── routers/            # vessels, uas, analytics endpoints
│   └── models/             # Pydantic validation models
├── workers/                # Data ingestion workers
│   ├── ais/                # NOAA AIS + Global Fishing Watch
│   ├── sanctions/          # OFAC SDN + OpenSanctions
│   ├── gdelt/              # GDELT event ingestion
│   └── uas/                # FAA UAS sightings
├── processors/graph/       # Kafka -> Memgraph consumer + event detection
├── services/
│   ├── entity-resolution/  # Splink-based vessel/person ER
│   ├── cot-publisher/      # ATAK/TAK CoT XML output
│   ├── rdf-exporter/       # Stardog/EUCISE-OWL RDF export
│   └── geoserver-sync/     # GeoServer layer management
├── schema/
│   ├── graph/              # Memgraph Cypher schema
│   ├── relational/         # PostgreSQL + PostGIS tables
│   └── elasticsearch/      # ES index mappings
├── configs/                # Service configurations
├── scripts/                # Setup and utility scripts
├── tests/                  # Unit and integration tests
├── data-governance/        # Retention policy, PII rules
├── docker-compose.yml      # Full stack deployment
├── Makefile                # Common operations
└── pyproject.toml          # Python project config
```

## Operational Domains

1. **Illicit Maritime Activity (IMA)** — AIS manipulation, STS transfers, flag hopping, sanctions evasion
2. **Cartel & Drug Trafficking Networks** — TCO topology, supply chains, financial flows, maritime logistics
3. **Counter-Narcotics Operations** — Interdictions, seizures, operations, intelligence reports
4. **Counter-UAS (C-UAS)** — Drone detections, flight paths, Remote ID, countermeasures

## Key Features

- **Bitemporal data model** — track both real-world validity and system recording time
- **Cross-domain correlation** — link AIS anomalies with GDELT events, UAS detections with STS transfers
- **Composite risk scoring** — weighted multi-factor vessel risk (0-10 scale)
- **AIS gap detection** — real-time detection with probable cause assessment
- **STS transfer detection** — proximity-based ship-to-ship encounter detection
- **TAK/ATAK integration** — CoT XML output for field operator displays
- **Stardog/RDF export** — EUCISE-OWL aligned semantic layer
- **Air-gap capable** — full Docker deployment, no external dependencies required

## License

Apache License 2.0 — see [LICENSE](LICENSE)
