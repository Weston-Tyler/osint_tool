---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Invariant/no-secrets-in-repo
type: Invariant
label: no secrets in repo
summary: 'Secrets are never committed: .env and *.key files are git-ignored, .env.example holds only placeholders,
  and pre-commit blocks private keys and leaked credentials.'
claim: 'Secrets are never committed: .env and *.key files are git-ignored, .env.example holds only placeholders,
  and pre-commit blocks private keys and leaked credentials.'
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

gitignore ignores .env, .env.local, .env.*.local, env/, keycloak-data/, and *.key. .pre-commit-config.yaml runs gitleaks (v8.18.0) and detect-private-key on every commit. The only checked-in env file is .env.example, which documents variable names/placeholders (2b1484b expanded it with all data-source API keys). Violation shape: committing a real credential, an .env file, a private key, or a config with an embedded secret. Note: this is a candidate invariant; a human must ratify before it is binding.
