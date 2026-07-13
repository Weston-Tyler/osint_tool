---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Lesson/postgres-extensions-before-ddl
type: Lesson
label: postgres extensions before ddl
summary: PostgreSQL extensions (PostGIS, pgvector, Apache AGE) must be created before any DDL that uses
  them, and the postgres image is custom-built to bundle AGE + PostGIS.
claim: PostgreSQL extensions (PostGIS, pgvector, Apache AGE) must be created before any DDL that uses
  them, and the postgres image is custom-built to bundle AGE + PostGIS.
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

e10654b fix(postgres): create vector + postgis extensions before tables use them; 576a0eb fix(postgres): build custom image with AGE + PostGIS (docker/postgres/Dockerfile). Init order matters: schema/relational/postgis-init.sql and postgres-init.sql assume the extensions already exist. Violation shape: adding a table/column using geometry/vector/AGE types ahead of the CREATE EXTENSION, or pulling the stock postgres image instead of the custom one.
