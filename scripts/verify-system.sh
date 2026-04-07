#!/usr/bin/env bash
# ============================================================
# MDA OSINT Database — Full System Verification
# ============================================================
#
# This script verifies the entire system end-to-end:
# 1. Docker services are running and healthy
# 2. Database schemas are initialized
# 3. Kafka topics exist
# 4. API responds correctly
# 5. Each data source ingester can fetch real data
# 6. Graph and PostGIS queries return valid results
# 7. Causal extraction pipeline functions
# 8. WorldFish can initialize
#
# Usage:
#   ./scripts/verify-system.sh          # Full verification
#   ./scripts/verify-system.sh --quick  # Skip live data fetches
#
# Prerequisites:
#   - Docker Compose stack running (make setup)
#   - Python environment with project deps installed
#   - .env configured with API keys

set -uo pipefail

QUICK=false
[[ "${1:-}" == "--quick" ]] && QUICK=true

PASS=0
FAIL=0
SKIP=0
ERRORS=""

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m'

mg_query() {
    # Run a Cypher query against Memgraph via stdin (mgconsole has no --execute flag)
    echo "$1" | docker exec -i mda-memgraph mgconsole --host=localhost --output-format=csv 2>/dev/null
}

run_test() {
    local name="$1"
    local cmd="$2"
    local skip_in_quick="${3:-false}"

    if $QUICK && [ "$skip_in_quick" = "true" ]; then
        printf "  ${YELLOW}%-55s [SKIP]${NC}\n" "$name"
        SKIP=$((SKIP + 1))
        return
    fi

    printf "  %-55s " "$name"
    if output=$(eval "$cmd" 2>&1); then
        printf "${GREEN}[PASS]${NC}\n"
        PASS=$((PASS + 1))
    else
        printf "${RED}[FAIL]${NC}\n"
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  - ${name}: $(echo "$output" | head -1)"
    fi
}

echo ""
echo -e "${CYAN}============================================================${NC}"
echo -e "${CYAN}  MDA OSINT Database — Full System Verification${NC}"
echo -e "${CYAN}============================================================${NC}"
echo ""

# ── 1. Docker Services ───────────────────────────────────────

echo -e "${CYAN}1. Docker Services${NC}"
echo "   ----------------"

run_test "Memgraph running" \
    "mg_query 'RETURN 1 AS n;' | grep -q '1'"

run_test "PostgreSQL running" \
    "docker exec mda-postgres pg_isready -U mda -d mda 2>/dev/null"

run_test "Redpanda running" \
    "docker exec mda-redpanda rpk cluster health 2>/dev/null | grep -q 'Healthy'"

run_test "Elasticsearch running" \
    "curl -sf http://localhost:9200/_cluster/health | grep -qE 'green|yellow'"

run_test "MinIO running" \
    "curl -sf http://localhost:9000/minio/health/live"

run_test "GeoServer running" \
    "curl -sf -o /dev/null -w '%{http_code}' http://localhost:8085/geoserver/web/ | grep -qE '200|302'"

run_test "Grafana running" \
    "curl -sf http://localhost:3001/api/health | grep -q ok"

run_test "FastAPI running" \
    "curl -sf http://localhost:8000/health | grep -q ok"

echo ""

# ── 2. Database Schemas ──────────────────────────────────────

echo -e "${CYAN}2. Database Schemas${NC}"
echo "   -----------------"

run_test "Memgraph: Vessel constraint exists" \
    "mg_query 'SHOW CONSTRAINT INFO;' | grep -q Vessel"

run_test "Memgraph: CausalEvent constraint exists" \
    "mg_query 'SHOW CONSTRAINT INFO;' | grep -q CausalEvent"

run_test "PostGIS: ais_positions table exists" \
    "docker exec mda-postgres psql -U mda -d mda -c '\dt ais_positions' 2>/dev/null | grep -q ais_positions"

run_test "PostGIS: maritime_zones table exists" \
    "docker exec mda-postgres psql -U mda -d mda -c '\dt maritime_zones' 2>/dev/null | grep -q maritime_zones"

run_test "PostGIS: causal_events table exists" \
    "docker exec mda-postgres psql -U mda -d mda -c '\dt causal_events' 2>/dev/null | grep -q causal_events"

run_test "PostGIS: worldfish_agent_memory table exists" \
    "docker exec mda-postgres psql -U mda -d mda -c '\dt worldfish_agent_memory' 2>/dev/null | grep -q worldfish_agent_memory"

run_test "PostGIS: pgvector extension" \
    "docker exec mda-postgres psql -U mda -d mda -c 'SELECT vector_dims(NULL::vector);' 2>/dev/null"

run_test "Elasticsearch: mda_vessels index" \
    "curl -sf http://localhost:9200/mda_vessels | grep -q mda_vessels"

echo ""

# ── 3. Kafka Topics ──────────────────────────────────────────

echo -e "${CYAN}3. Kafka Topics${NC}"
echo "   -------------"

run_test "Topic: mda.ais.positions.raw" \
    "docker exec mda-redpanda rpk topic describe mda.ais.positions.raw 2>/dev/null"

run_test "Topic: mda.sanctions.updates" \
    "docker exec mda-redpanda rpk topic describe mda.sanctions.updates 2>/dev/null"

run_test "Topic: mda.causal.extractions.raw" \
    "docker exec mda-redpanda rpk topic describe mda.causal.extractions.raw 2>/dev/null"

run_test "Topic: mda.acled.events" \
    "docker exec mda-redpanda rpk topic describe mda.acled.events 2>/dev/null"

run_test "Topic count >= 40" \
    "[ $(docker exec mda-redpanda rpk topic list 2>/dev/null | wc -l) -ge 40 ]"

echo ""

# ── 4. API Endpoints ─────────────────────────────────────────

echo -e "${CYAN}4. API Endpoints${NC}"
echo "   ---------------"

run_test "GET /health" \
    "curl -sf http://localhost:8000/health | python3 -c 'import sys,json; d=json.load(sys.stdin); assert d[\"status\"] in (\"ok\",\"degraded\")'"

run_test "GET /v1/vessels" \
    "curl -sf http://localhost:8000/v1/vessels | python3 -c 'import sys,json; d=json.load(sys.stdin); assert \"vessels\" in d'"

run_test "GET /v1/vessels?sanctioned_only=true" \
    "curl -sf 'http://localhost:8000/v1/vessels?sanctioned_only=true' | python3 -c 'import sys,json; d=json.load(sys.stdin); assert \"count\" in d'"

run_test "GET /v1/uas/detections" \
    "curl -sf http://localhost:8000/v1/uas/detections | python3 -c 'import sys,json; d=json.load(sys.stdin); assert \"detections\" in d'"

run_test "GET /v1/analytics/risk-summary" \
    "curl -sf http://localhost:8000/v1/analytics/risk-summary | python3 -c 'import sys,json; json.load(sys.stdin)'"

run_test "GET /metrics" \
    "curl -sf http://localhost:8000/metrics | grep -q mda"

run_test "GET /metrics/summary" \
    "curl -sf http://localhost:8000/metrics/summary | python3 -c 'import sys,json; json.load(sys.stdin)'"

echo ""

# ── 5. Live Data Fetch Tests ─────────────────────────────────

echo -e "${CYAN}5. Live Data Source Fetches${NC}"
echo "   -------------------------"

run_test "OFAC SDN: fetch SDN list" \
    "python3 -c \"
import requests
r = requests.get('https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML', timeout=60, allow_redirects=True)
assert r.status_code == 200, f'status={r.status_code}'
assert len(r.content) > 100000, f'too small: {len(r.content)}'
print('OK: SDN.XML', len(r.content), 'bytes')
\"" "true"

run_test "GDELT: fetch latest master file" \
    "python3 -c \"
import requests
r = requests.get('http://data.gdeltproject.org/gdeltv2/masterfilelist.txt', timeout=15)
lines = r.text.strip().split('\n')
assert len(lines) > 100
print(f'OK: {len(lines)} entries in master file')
\"" "true"

run_test "ReliefWeb: fetch 1 report" \
    "python3 -c \"
import requests
r = requests.post('https://api.reliefweb.int/v2/reports?appname=mda-osint-verify', json={'limit':1}, timeout=15)
assert r.status_code == 200
d = r.json()
assert len(d.get('data', [])) >= 1
print('OK: ReliefWeb API responding')
\"" "true"

run_test "CoinGecko: fetch BTC price" \
    "python3 -c \"
import requests
r = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd', timeout=10)
d = r.json()
assert 'bitcoin' in d and d['bitcoin']['usd'] > 0
print('OK: BTC =', int(d['bitcoin']['usd']))
\"" "true"

run_test "World Bank: fetch Mexico GDP" \
    "python3 -c \"
import requests
r = requests.get('https://api.worldbank.org/v2/country/MX/indicator/NY.GDP.MKTP.CD?format=json&per_page=1', timeout=15)
d = r.json()
assert len(d) >= 2 and len(d[1]) >= 1
print(f'OK: World Bank API responding')
\"" "true"

run_test "UNHCR: population endpoint" \
    "python3 -c \"
import requests
r = requests.get('https://api.unhcr.org/population/v1/population/?limit=1&year=2023', timeout=15)
assert r.status_code == 200
print('OK: UNHCR API responding')
\"" "true"

run_test "OpenSanctions: check availability" \
    "curl -sf -I https://data.opensanctions.org/datasets/latest/default/entities.ftm.json | head -1 | grep -q 200" "true"

echo ""

# ── 6. Data Ingestion Verification ───────────────────────────

echo -e "${CYAN}6. Data Ingestion (dry-run normalizers)${NC}"
echo "   --------------------------------------"

run_test "OFAC SDN parser: vessel extraction" \
    "python3 -c \"
from workers.sanctions.ofac_sdn_ingester import parse_ofac_vessel
result = parse_ofac_vessel({
    'uid': 1, 'sdnType': 'Vessel', 'lastName': 'TEST',
    'ids': [{'idType': 'IMO Number', 'idNumber': 'IMO 1234567'}],
    'programs': [{'program': 'IRAN'}], 'akas': [], 'remarks': ''
})
assert result is not None
assert result['imo'] == '1234567'
print('OK: OFAC vessel parser works')
\""

run_test "AIS normalizer: AISHub record" \
    "python3 -c \"
from workers.ais.aishub_ingester import normalize_aishub_record
r = normalize_aishub_record({'MMSI': '123456789', 'LATITUDE': 5.0, 'LONGITUDE': -76.0,
    'SOG': 120, 'COG': 1800, 'HEADING': 245, 'NAVSTAT': 0, 'NAME': 'TEST', 'TYPE': 70, 'IMO': '1234567', 'TIME': '2026-01-01'})
assert r['mmsi'] == '123456789' and r['speed_kts'] == 12.0
print('OK: AISHub normalizer works')
\""

run_test "EM-DAT normalizer: disaster event" \
    "python3 -c \"
from workers.humanitarian.emdat_ingester import normalize_disaster
r = normalize_disaster({'Dis No': '2024-001', 'Disaster Type': 'Flood', 'ISO': 'MX',
    'Country': 'Mexico', 'Start Year': '2024', 'Start Month': '6',
    'Total Deaths': '50', 'Total Affected': '100000', 'Total Damage (\\\"000 US\$)': '5000'})
assert r is not None and 'FLOOD' in r['event_type']
print('OK: EM-DAT normalizer works')
\""

run_test "Remote ID parser: BasicID message" \
    "python3 -c \"
from workers.uas.remote_id_receiver import RemoteIDParser
parser = RemoteIDParser()
# Build a BasicID message (type=0, proto=1)
import struct
header = bytes([(0x0 << 4) | 0x01])
type_byte = bytes([(2 << 4) | 2])  # CAA reg, multirotor
uas_id = b'FA-2024-001234\x00\x00\x00\x00\x00\x00'
raw = header + type_byte + uas_id
result = parser.parse_message(raw, '2026-01-01T00:00:00Z')
assert result is not None and result['uas_id'] == 'FA-2024-001234'
print('OK: Remote ID parser works')
\""

run_test "CoinGecko anomaly detector" \
    "python3 -c \"
from workers.crypto.coingecko_ingester import detect_anomaly
prices = [{'token_id': 'bitcoin', 'ticker': 'BTC', 'price_usd': 50000 + i*10, 'timestamp': f'2026-01-{i+1:02d}'} for i in range(40)]
prices[35]['price_usd'] = 80000  # inject spike
anomalies = detect_anomaly(prices)
assert len(anomalies) >= 1 and any(a['direction'] == 'spike' for a in anomalies)
print(f'OK: Detected {len(anomalies)} anomalies')
\""

run_test "Pydantic AIS model validation" \
    "python3 -c \"
from api.models.ais import AISPositionMessage
from datetime import datetime, timezone
pos = AISPositionMessage(source='test', mmsi='123456789', timestamp=datetime.now(timezone.utc), lat=5.0, lon=-76.0)
assert pos.mmsi == '123456789'
# Test IMO normalization
pos2 = AISPositionMessage(source='test', mmsi='987654321', imo='IMO 1234567', timestamp=datetime.now(timezone.utc), lat=0, lon=0)
assert pos2.imo == '1234567'
print('OK: Pydantic models validate correctly')
\""

run_test "CausalPrediction: CoT XML generation" \
    "python3 -c \"
from worldfish.prediction import CausalPrediction
pred = CausalPrediction(
    predicted_event_type='PORT_CLOSURE', predicted_event_description='Test',
    predicted_location_lat=19.05, predicted_location_lon=-104.32,
    confidence=0.75, domain='maritime', trigger_event_id='test_001')
xml = pred.to_atak_cot_xml()
assert '<?xml' in xml and 'WF-' in xml
print('OK: CoT XML generation works')
\""

run_test "WorldFish environments: action types" \
    "python3 -c \"
from worldfish.environments import ActionType, WorldState, MaritimeDomainEnvironment
assert len(ActionType) >= 15
ws = WorldState()
env = MaritimeDomainEnvironment({})
actions = env.get_available_actions({'agent_type': 'vessel', 'risk_tolerance': 0.8})
assert ActionType.AIS_DISABLE in actions
print(f'OK: {len(actions)} actions available for adversarial vessel')
\""

run_test "Event processor: risk scoring" \
    "python3 -c \"
from processors.graph.event_processor import CompositeRiskScorer
scorer = CompositeRiskScorer()
# Max risk vessel
score = scorer.compute_risk_score(
    {'sanctions_status': 'SANCTIONED', 'flag_state': 'KP', 'beneficial_owner': 'Unknown'},
    {'owner_sanctioned': True, 'ais_gap_recent_24h': True, 'dark_sts_transfer': True,
     'cartel_route_transit': True, 'near_sanctioned_vessel': True})
assert score == 10.0
# Clean vessel
score2 = scorer.compute_risk_score(
    {'sanctions_status': 'CLEAR', 'flag_state': 'US', 'beneficial_owner': 'Maersk'},
    {})
assert score2 < 1.0
print(f'OK: Risk scorer works (max={score}, clean={score2})')
\""

run_test "NER regex extraction" \
    'python3 -c "
from services.nlp.ner_pipeline import extract_regex_entities
entities = extract_regex_entities(\"The vessel IMO 9123456 (MMSI 123456789) was seized during Operation UNIFIED RESOLVE with 2,400 kilograms of cocaine worth USD 72 million.\")
labels = {e[\"label\"] for e in entities}
assert \"IMO_NUMBER\" in labels and \"MMSI_NUMBER\" in labels
print(f\"OK: Extracted {len(entities)} entities from test text\")
"'

echo ""

# ── 7. Graph Query Verification ──────────────────────────────

echo -e "${CYAN}7. Graph Queries${NC}"
echo "   ---------------"

run_test "Memgraph: count all nodes" \
    "mg_query 'MATCH (n) RETURN count(n) AS total;' | grep -qE '[0-9]'"

run_test "Memgraph: count Vessel nodes" \
    "mg_query 'MATCH (v:Vessel) RETURN count(v) AS cnt;' | grep -qE '[0-9]'"

run_test "Memgraph: count sanctioned entities" \
    "mg_query \"MATCH (n) WHERE n.sanctions_status = 'SANCTIONED' RETURN count(n) AS cnt;\" | grep -qE '[0-9]'"

run_test "Memgraph: SHOW STREAMS" \
    "mg_query 'SHOW STREAMS;' >/dev/null"

run_test "PostGIS: spatial query works" \
    "docker exec mda-postgres psql -U mda -d mda -c \"SELECT ST_AsText(ST_MakePoint(-76, 5));\" 2>/dev/null | grep -q POINT"

echo ""

# ── Summary ──────────────────────────────────────────────────

TOTAL=$((PASS + FAIL + SKIP))
echo -e "${CYAN}============================================================${NC}"
echo -e "  RESULTS: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}, ${YELLOW}${SKIP} skipped${NC} (${TOTAL} total)"
echo -e "${CYAN}============================================================${NC}"

if [ $FAIL -gt 0 ]; then
    echo ""
    echo -e "  ${RED}Failed tests:${NC}${ERRORS}"
    echo ""
    exit 1
else
    echo -e "  ${GREEN}All tests passed!${NC}"
    exit 0
fi
