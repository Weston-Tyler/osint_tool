---
id: aios:r/01KXE5FNAHRNYRTP3BGYS72X6T/Component/frontend
type: Component
label: Kepler.gl frontend
summary: Vite + React Kepler.gl single-page web UI for geospatial exploration of MDA vessel/UAS/event data.
state: active
kind: app
paths: ["frontend/**"]
stability: evolving
created_by: agent:claude-code
method: generated
created_at: 2026-07-13
---

`frontend/kepler-gl/` is a small Vite + React app embedding Kepler.gl for map-based exploration.
The only JavaScript/Node component in the repo; everything else is Python or infra. Talks to the
API/GeoServer over HTTP.
