---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Component/tests
type: Component
label: Test suite
summary: Pytest unit/integration/smoke suites plus a Locust load test, covering ingesters, processors, API models, and validators.
state: active
kind: module
paths: ["tests/**"]
stability: evolving
created_by: agent:claude-code
method: generated
created_at: 2026-07-13
---

`tests/unit/` is the CI gate (`python -m pytest tests/unit/`); `tests/integration/` hits live API
endpoints, `tests/smoke/` exercises all ingesters, `tests/load/` is a Locust file. Tests import
production modules directly (e.g. `from workers.sanctions... import ...`), so a deleted or renamed
source module breaks collection — currently `test_sanctions_ingester.py` references the removed
`workers.sanctions.ofac_sdn_ingester`. CI stubs heavy optional deps (gqlalchemy, ollama, mesa)
and installs spacy/statsmodels rather than depending on the full stack.
