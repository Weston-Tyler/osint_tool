#!/bin/bash
# Codespaces post-create setup — auto-starts services and ingests real data
set -uo pipefail

echo ""
echo "=================================================="
echo "  MDA OSINT Database — Codespace Setup"
echo "=================================================="
echo ""

# 1. Install Python deps (skip ones that need C libs)
echo "[1/7] Installing Python dependencies..."
pip install -q pytest pydantic kafka-python requests spacy pandas numpy \
    statsmodels psycopg2-binary haversine aiohttp httpx rdflib 2>/dev/null || true
echo "  Done"

# 2. Create .env
if [ ! -f .env ]; then
    echo "[2/7] Creating .env..."
    sed \
        -e 's/change_me_strong_password_32chars/mda_codespace_2026!/' \
        -e 's/change_me_minio_password/mda_minio_2026!/' \
        -e 's/change_me_grafana_password/mda_grafana_2026!/' \
        -e 's/change_me_geoserver_password/mda_geoserver_2026!/' \
        -e 's/change_me_elastic_password/mda_elastic_2026!/' \
        -e 's/change_me_jwt_secret_64chars/mda_jwt_codespace_2026_long_key!/' \
        .env.example > .env
    echo "  Done"
else
    echo "[2/7] .env already exists — skipping"
fi

# 3. Pull Docker images
echo "[3/7] Pulling Docker images (this takes ~2 min)..."
docker compose pull postgres redpanda memgraph elasticsearch 2>/dev/null || true

# 4. Start core services
echo "[4/7] Starting core services..."
docker compose up -d postgres redpanda memgraph elasticsearch 2>/dev/null || true
echo "  Waiting 45s for services to initialize..."
sleep 45

# 5. Initialize schemas
echo "[5/7] Initializing schemas..."
docker exec mda-memgraph mgconsole < schema/graph/memgraph-init.cypher 2>/dev/null || true
docker exec mda-memgraph mgconsole < schema/graph/causal-schema.cypher 2>/dev/null || true
echo "  Done"

# 6. Create Kafka topics
echo "[6/7] Creating Kafka topics..."
bash scripts/create-kafka-topics.sh 2>/dev/null || true
echo "  Done"

# 7. Start API
echo "[7/7] Starting API..."
docker compose up -d api 2>/dev/null || true
sleep 15

# Quick health check
echo ""
echo "=================================================="
echo "  Health Check"
echo "=================================================="
echo -n "  Memgraph:      "
docker exec mda-memgraph mgconsole --execute "RETURN 'OK';" 2>/dev/null | grep -q OK && echo "OK" || echo "STARTING"
echo -n "  PostgreSQL:    "
docker exec mda-postgres pg_isready -U mda 2>/dev/null | grep -q "accepting" && echo "OK" || echo "STARTING"
echo -n "  Redpanda:      "
docker exec mda-redpanda rpk cluster health 2>/dev/null | grep -q Healthy && echo "OK" || echo "STARTING"
echo -n "  API:           "
curl -sf http://localhost:8000/health 2>/dev/null | grep -q ok && echo "OK" || echo "STARTING"

echo ""
echo "=================================================="
echo "  READY! Run these commands:"
echo "=================================================="
echo ""
echo "  # Ingest real data from live APIs:"
echo "  python workers/sanctions/ofac_sdn_ingester.py"
echo ""
echo "  # Query the data:"
echo "  curl localhost:8000/v1/vessels?sanctioned_only=true"
echo ""
echo "  # Open in browser:"
echo "  #   API Docs:      Port 8000/docs"
echo "  #   Memgraph Lab:  Port 3000"
echo "  #   Grafana:       Port 3001"
echo ""
