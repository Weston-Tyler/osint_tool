---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Component/deployment
type: Component
label: Deployment & ops
summary: Container orchestration, service/infra configs, Kubernetes manifests, setup/verify scripts, CI/CD workflows, data-governance policy, and project/tooling config.
state: active
kind: subsystem
paths: ["docker-compose*.yml", "docker/**", "k8s/**", "configs/**", "scripts/**", ".github/**", ".devcontainer/**", "Makefile", ".pre-commit-config.yaml", ".env.example", "data-governance/**", "pyproject.toml"]
stability: evolving
created_by: agent:claude-code
method: generated
created_at: 2026-07-13
---

How the system is assembled and run. Multiple `docker-compose*.yml` overlays compose the stack
(base + causal / corporate / datasets / gdelt / production); `configs/` holds per-service config
(Memgraph, Elasticsearch, Prometheus, Grafana, Traefik, Keycloak, antigravity-agents); `scripts/`
creates Kafka topics, ES indices, Memgraph streams and runs `verify-system.sh`; `k8s/` holds
production manifests; `.github/workflows/` the CI/CD. Also carries repo-wide tooling config
(`pyproject.toml` ruff/pytest, `.pre-commit-config.yaml`) and the `data-governance/`
retention policy. `make` is a convenience wrapper — its targets shell out to these files.
