---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Component/ingestion-workers
type: Component
label: Ingestion workers
summary: Data-source ingesters (AIS/GFW, sanctions, GDELT, UAS, corporate, humanitarian, economic, crypto, conflict, geospatial) that normalize feeds and publish to Kafka; plus community-submission validation.
state: active
kind: subsystem
paths: ["workers/**", "community/**"]
stability: evolving
created_by: agent:claude-code
method: generated
created_at: 2026-07-13
---

Each subdirectory under `workers/` is one data source packaged as its own container
(`Dockerfile` + `requirements.txt` + one or more `*_ingester.py`). Ingesters pull from an
external API/dataset, normalize records, and produce to `mda.<domain>.*` Kafka topics consumed
downstream by the graph-processor. `community/submission_validator.py` validates
community-submitted intelligence (PII confirmation, source URLs) before ingest. Rate limiting
and retry/backoff on external APIs are a recurring concern here (see git history for GDELT/GFW).
