#!/bin/bash
# Codespaces / devcontainer post-create setup
set -euo pipefail

echo "=== MDA OSINT Database — Codespace Setup ==="

# 1. Install Python deps
echo "Installing Python dependencies..."
pip install -e ".[dev]" 2>/dev/null || pip install pytest ruff pydantic
pip install kafka-python requests gqlalchemy spacy pandas numpy \
    statsmodels yfinance psycopg2-binary haversine aiohttp \
    elasticsearch minio rdflib httpx ollama mesa 2>/dev/null || true

# 2. Create .env from template
if [ ! -f .env ]; then
    echo "Creating .env from template..."
    sed \
        -e 's/change_me_strong_password_32chars/mda_codespace_pass_2026!/' \
        -e 's/change_me_minio_password/mda_minio_pass_2026!/' \
        -e 's/change_me_grafana_password/mda_grafana_2026!/' \
        -e 's/change_me_geoserver_password/mda_geoserver_2026!/' \
        -e 's/change_me_elastic_password/mda_elastic_2026!/' \
        -e 's/change_me_jwt_secret_64chars/mda_jwt_secret_codespace_2026_very_long_key!/' \
        .env.example > .env
    echo "  .env created with default codespace passwords"
fi

# 3. Start core Docker services (lightweight subset for Codespaces)
echo "Starting Docker services..."
docker compose up -d postgres redpanda memgraph elasticsearch 2>/dev/null || true

echo "Waiting for services to initialize (45s)..."
sleep 45

# 4. Initialize schemas
echo "Initializing Memgraph schema..."
docker exec mda-memgraph mgconsole < schema/graph/memgraph-init.cypher 2>/dev/null || true
docker exec mda-memgraph mgconsole < schema/graph/causal-schema.cypher 2>/dev/null || true

# 5. Create Kafka topics
echo "Creating Kafka topics..."
docker exec mda-redpanda rpk topic create mda.ais.positions.raw -p 4 2>/dev/null || true
docker exec mda-redpanda rpk topic create mda.sanctions.updates -p 1 2>/dev/null || true
docker exec mda-redpanda rpk topic create mda.acled.events -p 4 2>/dev/null || true
docker exec mda-redpanda rpk topic create mda.causal.extractions.raw -p 4 2>/dev/null || true
docker exec mda-redpanda rpk topic create mda.alerts.composite -p 4 2>/dev/null || true

# 6. Start API
echo "Starting API..."
docker compose up -d api 2>/dev/null || true
sleep 10

# 7. Run quick verification
echo ""
echo "=== Quick Verification ==="
echo -n "  Memgraph:      "
docker exec mda-memgraph mgconsole --execute "RETURN 'OK';" 2>/dev/null | grep -q OK && echo "OK" || echo "FAIL"
echo -n "  PostgreSQL:    "
docker exec mda-postgres pg_isready -U mda 2>/dev/null && echo "" || echo "FAIL"
echo -n "  Redpanda:      "
docker exec mda-redpanda rpk cluster health 2>/dev/null | grep -q Healthy && echo "OK" || echo "FAIL"
echo -n "  API:           "
curl -sf http://localhost:8000/health 2>/dev/null | grep -q ok && echo "OK" || echo "STARTING..."

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Next steps:"
echo "  make ingest-ofac          # Load ~1000+ sanctioned vessels"
echo "  make ingest-gdelt         # Pull latest GDELT events"
echo "  curl localhost:8000/v1/vessels?sanctioned_only=true"
echo "  make verify-quick         # Run verification suite"
echo ""
echo "Dashboards:"
echo "  Memgraph Lab:     Port 3000"
echo "  Grafana:          Port 3001"
echo "  Redpanda Console: Port 8080"
echo "  API Docs:         http://localhost:8000/docs"
echo ""
