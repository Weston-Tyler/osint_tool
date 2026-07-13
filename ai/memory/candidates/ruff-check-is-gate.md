---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Convention/ruff-check-is-gate
type: Convention
label: ruff check is gate
summary: ruff check . is the lint gate and must stay green; ruff format is advisory only (CI reports formatting
  as a warning and never fails on it).
claim: ruff check . is the lint gate and must stay green; ruff format is advisory only (CI reports formatting
  as a warning and never fails on it).
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

Validated by running both: 'ruff check .' exits 0 (All checks passed). 'ruff format --check .' reports ~74 files would be reformatted and exits 1. CI (.github/workflows/ci.yml) runs 'ruff check . --output-format=github' as a hard step but 'ruff format --check . || echo ::warning::Code would be reformatted' as a non-blocking warning. Config in pyproject.toml: line-length 120, target py312, select E/F/W with several ignores (E501, E402, F401, F841, W29x). Violation shape: introducing a ruff check (E/F/W non-ignored) error, or assuming ruff format is enforced when it is not.
