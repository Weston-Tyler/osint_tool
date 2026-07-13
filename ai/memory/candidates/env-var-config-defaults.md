---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Convention/env-var-config-defaults
type: Convention
label: env var config defaults
summary: All runtime config (DB/broker hosts, ports, credentials, API keys) is read from environment variables
  with localhost dev defaults; infrastructure endpoints are never hardcoded.
claim: All runtime config (DB/broker hosts, ports, credentials, API keys) is read from environment variables
  with localhost dev defaults; infrastructure endpoints are never hardcoded.
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

os.getenv(...) with a localhost/default fallback appears in ~74 modules across api/, workers/, services/, processors/, worldfish/ (e.g. api/main.py MEMGRAPH_HOST/POSTGRES_*; processors/graph/graph_consumer.py KAFKA_BOOTSTRAP/MEMGRAPH_*; worldfish/simulation_engine.py OLLAMA_BASE_URL). Multiple fix commits exist purely to route a hardcoded value through an env var: 375b8be fix(graph-processor): set MEMGRAPH_HOST/PORT env vars; f98f936 fix(ownership-api): add MEMGRAPH_URI env var. .env is git-ignored; .env.example is the documented template (2b1484b expanded it with all data-source API keys). Violation shape: a new module hardcodes a service host/port or reads a secret from anywhere but the environment.
