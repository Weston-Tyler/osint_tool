---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Lesson/aios-windows-crlf-eol
type: Lesson
label: aios windows crlf eol
summary: On Windows (core.autocrlf=true), AIOS-managed files must be pinned to LF via .gitattributes or
  checkouts rewrite them to CRLF and aios validate reports the generated adapters as stale.
claim: On Windows (core.autocrlf=true), AIOS-managed files must be pinned to LF via .gitattributes or
  checkouts rewrite them to CRLF and aios validate reports the generated adapters as stale.
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

Observed this session: after committing the ai/ tree and switching branches, git re-materialized ai/constitution.md and the AGENTS.md/CLAUDE.md adapters as CRLF (git ls-files --eol showed i/lf w/crlf). aios content-hashes these files, so the adapter source:sha256 no longer matched the constitution and 'aios validate' failed with adapter.stale. Fix applied: .gitattributes pinning 'ai/** text eol=lf', 'AGENTS.md text eol=lf', 'CLAUDE.md text eol=lf', renormalize working copy to LF, then re-run aios adapt. Violation shape: onboarding an AIOS repo on Windows without a .gitattributes eol=lf rule, or committing CRLF into ai/**.
