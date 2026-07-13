# osint_tool — Repository Constitution

<!-- Hard cap ~1,500 tokens (enforced by `aios validate`). This file is the only
     unconditionally-loaded repo context: identity, commands, top invariants as
     pointers. Detail belongs in ai/knowledge/ records, not here. -->

## What this repository is

The **MDA OSINT Database** — a fully open-source, graph-native intelligence database for Maritime
Domain Awareness across four domains: illicit maritime activity, cartel/narcotics networks,
counter-narcotics, and counter-UAS. It is a **containerized, event-driven pipeline, not a
monolith**: independent ingestion workers publish to Kafka (Redpanda) topics named
`mda.<domain>.*`; a graph-processor consumes them into Memgraph (graph) + PostGIS (spatial) +
Elasticsearch (search); a FastAPI service and downstream analysis/export services read those
stores. Every worker/service is its own Docker image wired by `docker-compose*.yml`; there is no
compiled build artifact.

## Commands (validated by running them, not by believing docs)

- **Lint (CI gate):** `ruff check .` — passes clean today (line-length 120, py312 target).
- **Format (advisory, NOT a gate):** `ruff format --check .` — reports ~74 files; CI emits a
  warning only and never fails on it.
- **Test (unit, CI gate):** `python -m pytest tests/unit/` — **currently fails at collection**
  because `tests/unit/test_sanctions_ingester.py` imports the removed
  `workers.sanctions.ofac_sdn_ingester`. Excluding that file: 56 pass / 1 skip. Fixing the stale
  test restores the gate.
- **Compose config check:** `docker compose -f docker-compose.yml [-f docker-compose.corporate.yml] config -q` — valid.
- **Full setup** (needs Docker + a populated `.env`; not run in onboarding): bring the stack up
  with `docker compose … up -d`, then create Kafka topics, ES indices, and init the Memgraph
  schema — see `Makefile` targets `setup` / `topics-all` / `es-indices` / `init-db`. `make` is
  **not available on Windows**; run the underlying commands directly.

Python: repo targets 3.11/3.12; the unit suite also passes on 3.13.

## Top invariants and rules (candidates pending ratification)

- **Config from environment variables with localhost defaults — never hardcode hosts/creds**
  (candidate — ai/memory/candidates/env-var-config-defaults).
- **Never commit secrets:** `.env` is git-ignored; `.env.example` is the template; pre-commit runs
  gitleaks + detect-private-key (candidate — ai/memory/candidates/no-secrets-in-repo).
- **Components couple through Kafka topics + shared stores, not imports;** topics follow
  `mda.<domain>.*` (candidate — ai/memory/candidates/kafka-topic-namespace).
- **`ruff check .` is the lint gate and must stay green;** `ruff format` is advisory only
  (candidate — ai/memory/candidates/ruff-check-is-gate).
- **Tests import production modules directly** — removing/renaming a module breaks collection
  (candidate — ai/memory/candidates/tests-import-production-modules).

## Map

Nine components in `ai/components/`: **api, ingestion-workers, graph-processor,
analysis-services, worldfish, data-model, frontend, deployment, tests**. Query before exploring:
`aios status`, `aios query components`, `aios impact --paths <files>`. Derived dependencies are
**Python-import-only** (`api→graph-processor`, `tests→*`); the true runtime coupling runs through
Kafka topics and the Memgraph/PostGIS/Elasticsearch stores, which the import graph does not show.
