#!/usr/bin/env bash
# Create Kafka topics for expanded GDELT integration
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:19092}"
RPK="rpk -X brokers=${BOOTSTRAP}"

echo "Creating GDELT expansion Kafka topics..."

# GKG (Global Knowledge Graph) — high volume, 15-minute updates
$RPK topic create mda.gdelt.gkg.raw \
  -p 8 -r 1 -c retention.ms=604800000 \
  2>/dev/null || echo "  mda.gdelt.gkg.raw exists"

# GKG extracted entities (persons, orgs, locations)
$RPK topic create mda.gdelt.gkg.entities \
  -p 8 -r 1 -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.gdelt.gkg.entities exists"

# DOC API article search results
$RPK topic create mda.gdelt.doc.articles \
  -p 4 -r 1 -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.gdelt.doc.articles exists"

# GEO API spatial event results
$RPK topic create mda.gdelt.geo.events \
  -p 4 -r 1 -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.gdelt.geo.events exists"

# TV API clip metadata
$RPK topic create mda.gdelt.tv.clips \
  -p 4 -r 1 -c retention.ms=604800000 \
  2>/dev/null || echo "  mda.gdelt.tv.clips exists"

# TV API narrative shift alerts
$RPK topic create mda.gdelt.tv.narrative_shifts \
  -p 2 -r 1 -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.gdelt.tv.narrative_shifts exists"

echo ""
echo "GDELT topics created:"
$RPK topic list | grep gdelt
