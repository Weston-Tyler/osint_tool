---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Convention/external-api-ratelimit-backoff
type: Convention
label: external api ratelimit backoff
summary: Every ingester that calls an external OSINT API must apply rate limiting plus retry/backoff;
  unthrottled calls have repeatedly caused failures.
claim: Every ingester that calls an external OSINT API must apply rate limiting plus retry/backoff; unthrottled
  calls have repeatedly caused failures.
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

Recurring hard-won fixes: 9b13df5 feat(gfw): add rate limiting, retry/backoff, --max-pages cap; 16e2f24 fix(gdelt): rate limit + retry/backoff + daemon loop on doc/geo/tv; ad06dac fix(gdelt): bump rate limit to 5.5s, disable defunct GEO 2.0 endpoint; acb4d22 add startup cooldown to doc/tv. External endpoints also drift (71a7617 OFAC moved to sanctionslistservice; 6ba1772 ReliefWeb v1 decommissioned use v2; 644b4ed registered ReliefWeb appname). Violation shape: a new *_ingester.py that hammers a remote API in a tight loop with no delay, no backoff, and no handling of endpoint deprecation.
