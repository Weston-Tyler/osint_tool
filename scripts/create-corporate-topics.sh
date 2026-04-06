#!/usr/bin/env bash
# Create Kafka topics for Corporate Ownership Graph
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:19092}"
RPK="rpk -X brokers=${BOOTSTRAP}"

echo "Creating corporate ownership Kafka topics..."

# Raw ingestion (high volume, 7-day retention)
$RPK topic create mda.corporate.gleif.raw -p 8 -r 1 -c retention.ms=604800000 2>/dev/null || true
$RPK topic create mda.corporate.companies_house.raw -p 8 -r 1 -c retention.ms=604800000 2>/dev/null || true
$RPK topic create mda.corporate.icij.raw -p 4 -r 1 -c retention.ms=604800000 2>/dev/null || true
$RPK topic create mda.corporate.opensanctions.raw -p 4 -r 1 -c retention.ms=604800000 2>/dev/null || true
$RPK topic create mda.corporate.sec_edgar.raw -p 4 -r 1 -c retention.ms=604800000 2>/dev/null || true
$RPK topic create mda.corporate.registry.raw -p 4 -r 1 -c retention.ms=604800000 2>/dev/null || true
$RPK topic create mda.corporate.property.raw -p 4 -r 1 -c retention.ms=604800000 2>/dev/null || true

# Processed (30-day retention)
$RPK topic create mda.corporate.entities.resolved -p 4 -r 1 -c retention.ms=2592000000 2>/dev/null || true
$RPK topic create mda.corporate.ownership.changes -p 4 -r 1 -c retention.ms=2592000000 2>/dev/null || true

# Alerts (90-day retention)
$RPK topic create mda.corporate.shell_company.alerts -p 2 -r 1 -c retention.ms=7776000000 2>/dev/null || true
$RPK topic create mda.corporate.tco.alerts -p 2 -r 1 -c retention.ms=7776000000 2>/dev/null || true

echo "Corporate ownership topics created."
$RPK topic list | grep corporate
