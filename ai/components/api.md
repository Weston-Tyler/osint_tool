---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Component/api
type: Component
label: MDA API
summary: FastAPI REST + WebSocket API over the Memgraph/PostGIS/Elasticsearch layers — vessels, UAS, and analytics routers with auth and Prometheus metrics.
state: active
kind: service
paths: ["api/**"]
stability: evolving
created_by: agent:claude-code
method: generated
created_at: 2026-07-13
---

Async FastAPI app (`api/main.py`) exposing `/v1/*` query endpoints and a WebSocket alert
broadcaster. Reads Memgraph, PostgreSQL/PostGIS, and Elasticsearch; all connection targets come
from environment variables with `localhost` defaults. Routers: vessels, uas, analytics,
websocket; services: auth, metrics. Pydantic models in `api/models/`.
