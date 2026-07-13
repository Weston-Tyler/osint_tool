---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Lesson/tests-import-production-modules
type: Lesson
label: tests import production modules
summary: Unit tests import production modules by path, so removing or renaming a source module breaks
  pytest collection unless its dependent tests, Makefile targets, and compose services are updated in
  the same change.
claim: Unit tests import production modules by path, so removing or renaming a source module breaks pytest
  collection unless its dependent tests, Makefile targets, and compose services are updated in the same
  change.
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

ORIGINATING BUG (fixed in the onboarding session — kept as the worked example): commit 6f2ca60 fix(sanctions): unify on OpenSanctions, retire dead OFAC URL deleted workers/sanctions/ofac_sdn_ingester.py (151 lines) but did NOT update tests/unit/test_sanctions_ingester.py, which still did 'from workers.sanctions.ofac_sdn_ingester import ...'. Result: 'python -m pytest tests/unit/' failed at COLLECTION (ModuleNotFoundError), taking down the whole unit gate; excluding that one file, 56 passed / 1 skip. The Makefile ingest-ofac target also still ran the deleted file.

RESOLUTION (this session, uncommitted at time of writing): test_sanctions_ingester.py was rewritten to exercise the OpenSanctions FTM converters (ftm_to_vessel/person/organization, stream_entities) that replaced the OFAC parsers; the dead ingest-ofac Makefile target and the README 'make ingest-ofac' reference were removed/repointed to ingest-opensanctions. Gate now: 63 passed / 1 skip, ruff clean.

General lesson (still binding-worthy): tests import production modules by path, so retiring or renaming a source leaves dangling references (tests, make targets, compose services, docs) that only surface at collection/run time — update them in the same change. Consider a CI guard that fails on an import of a non-existent module.
