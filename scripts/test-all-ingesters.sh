#!/usr/bin/env bash
# Comprehensive test runner — validates all data source ingesters,
# causal extraction, WorldFish, and integration points.
#
# Usage: ./scripts/test-all-ingesters.sh

set -euo pipefail

echo "============================================================"
echo "  MDA OSINT Database — Comprehensive Test Suite"
echo "============================================================"
echo ""

PASS=0
FAIL=0
ERRORS=""

run_test() {
    local name="$1"
    local cmd="$2"
    printf "  %-50s " "$name"
    if eval "$cmd" > /dev/null 2>&1; then
        echo "[PASS]"
        PASS=$((PASS + 1))
    else
        echo "[FAIL]"
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  - ${name}"
    fi
}

# ── 1. Python Import Tests ───────────────────────────────────

echo "1. Module Import Tests"
echo "   -------------------"

# AIS
run_test "workers.ais.noaa_ais_ingester" "python -c 'import workers.ais.noaa_ais_ingester'"
run_test "workers.ais.gfw_ingester" "python -c 'import workers.ais.gfw_ingester'"
run_test "workers.ais.aishub_ingester" "python -c 'import workers.ais.aishub_ingester'"

# Sanctions
run_test "workers.sanctions.ofac_sdn_ingester" "python -c 'import workers.sanctions.ofac_sdn_ingester'"
run_test "workers.sanctions.opensanctions_ingester" "python -c 'import workers.sanctions.opensanctions_ingester'"

# GDELT
run_test "workers.gdelt.gdelt_ingester" "python -c 'import workers.gdelt.gdelt_ingester'"
run_test "workers.gdelt.gkg_ingester" "python -c 'import workers.gdelt.gkg_ingester'"
run_test "workers.gdelt.doc_api" "python -c 'import workers.gdelt.doc_api'"
run_test "workers.gdelt.geo_api" "python -c 'import workers.gdelt.geo_api'"
run_test "workers.gdelt.tv_api" "python -c 'import workers.gdelt.tv_api'"

# UAS
run_test "workers.uas.faa_uas_ingester" "python -c 'import workers.uas.faa_uas_ingester'"
run_test "workers.uas.remote_id_receiver" "python -c 'import workers.uas.remote_id_receiver'"

# Humanitarian
run_test "workers.humanitarian.emdat_ingester" "python -c 'import workers.humanitarian.emdat_ingester'"
run_test "workers.humanitarian.iom_dtm_ingester" "python -c 'import workers.humanitarian.iom_dtm_ingester'"

# Geospatial
run_test "workers.geospatial.nasa_firms_ingester" "python -c 'import workers.geospatial.nasa_firms_ingester'"

# Maritime
run_test "workers.maritime.maritime_ingester" "python -c 'import workers.maritime.maritime_ingester'"

# Crypto
run_test "workers.crypto.coingecko_ingester" "python -c 'import workers.crypto.coingecko_ingester'"

# Causal Extraction
run_test "services.causal_extractor.rule_based" "python -c 'import services.causal_extractor.rule_based'"
run_test "services.causal_extractor.llm_extractor" "python -c 'import services.causal_extractor.llm_extractor'"

# WorldFish
run_test "worldfish.environments" "python -c 'import worldfish.environments'"
run_test "worldfish.prediction" "python -c 'import worldfish.prediction'"
run_test "worldfish.seed_extractor" "python -c 'import worldfish.seed_extractor'"
run_test "worldfish.persona_generator" "python -c 'import worldfish.persona_generator'"
run_test "worldfish.memory" "python -c 'import worldfish.memory'"

# API
run_test "api.main" "python -c 'import api.main'"
run_test "api.models.ais" "python -c 'import api.models.ais'"
run_test "api.models.uas" "python -c 'import api.models.uas'"
run_test "api.models.entities" "python -c 'import api.models.entities'"

echo ""

# ── 2. API Endpoint Reachability (if running) ────────────────

echo "2. External API Reachability"
echo "   -------------------------"

run_test "GDELT Master File List" "curl -sf -o /dev/null http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
run_test "OFAC SDN JSON" "curl -sf -o /dev/null -I https://www.treasury.gov/ofac/downloads/sanctions/1.0/sdn_advanced.json"
run_test "ReliefWeb API" "curl -sf -o /dev/null 'https://api.reliefweb.int/v1/reports?limit=1'"
run_test "CoinGecko API" "curl -sf -o /dev/null https://api.coingecko.com/api/v3/ping"
run_test "World Bank API" "curl -sf -o /dev/null 'https://api.worldbank.org/v2/country/MX/indicator/NY.GDP.MKTP.CD?format=json&per_page=1'"
run_test "UNHCR API" "curl -sf -o /dev/null 'https://api.unhcr.org/population/v1/population/?limit=1'"
run_test "UN Comtrade Preview" "curl -sf -o /dev/null -I https://comtradeapi.un.org/public/v1/preview/C/A/HS"

echo ""

# ── 3. Unit Tests ────────────────────────────────────────────

echo "3. Unit Tests (pytest)"
echo "   --------------------"

if command -v pytest &>/dev/null; then
    pytest tests/unit/ -v --tb=short 2>&1 | tail -20
    PYTEST_EXIT=$?
    if [ $PYTEST_EXIT -eq 0 ]; then
        echo "  Unit tests: [PASS]"
        PASS=$((PASS + 1))
    else
        echo "  Unit tests: [FAIL]"
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  - Unit test suite"
    fi
else
    echo "  pytest not installed — skipping"
fi

echo ""

# ── Summary ──────────────────────────────────────────────────

echo "============================================================"
echo "  RESULTS: ${PASS} passed, ${FAIL} failed"
echo "============================================================"

if [ $FAIL -gt 0 ]; then
    echo ""
    echo "  Failed tests:${ERRORS}"
    echo ""
    exit 1
else
    echo "  All tests passed!"
    exit 0
fi
