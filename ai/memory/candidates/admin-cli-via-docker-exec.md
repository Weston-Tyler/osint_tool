---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Convention/admin-cli-via-docker-exec
type: Convention
label: admin cli via docker exec
summary: Broker/graph admin commands (rpk, mgconsole) are run inside the service container via docker
  exec, not from the host.
claim: Broker/graph admin commands (rpk, mgconsole) are run inside the service container via docker exec,
  not from the host.
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

63c3517 fix(topics): run rpk inside mda-redpanda container instead of host. Makefile confirms the pattern: 'docker exec mda-redpanda rpk topic create ...' and 'docker exec -i mda-memgraph mgconsole --host=localhost < schema/...cypher'. The host is not assumed to have rpk/mgconsole installed. Violation shape: a setup script or doc that calls rpk/mgconsole directly on the host instead of exec-ing into the container.
