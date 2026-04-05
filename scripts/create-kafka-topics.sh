#!/usr/bin/env bash
# Create all MDA Kafka topics in Redpanda
# Run: ./scripts/create-kafka-topics.sh

set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:19092}"
RPK="rpk -X brokers=${BOOTSTRAP}"

echo "Creating MDA Kafka topics on ${BOOTSTRAP}..."

# AIS positions: high-volume, 30-day retention, 12 partitions
$RPK topic create mda.ais.positions.raw \
  -p 12 -r 1 \
  -c retention.ms=2592000000 \
  -c compression.type=gzip \
  -c max.message.bytes=1048576 \
  2>/dev/null || echo "  mda.ais.positions.raw already exists"

$RPK topic create mda.ais.positions.enriched \
  -p 12 -r 1 \
  -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.ais.positions.enriched already exists"

# AIS event topics
$RPK topic create mda.ais.gaps.detected \
  -p 4 -r 1 \
  -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.ais.gaps.detected already exists"

$RPK topic create mda.ais.encounters.detected \
  -p 4 -r 1 \
  -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.ais.encounters.detected already exists"

# Sanctions updates: low-volume, 90-day retention
$RPK topic create mda.sanctions.updates \
  -p 1 -r 1 \
  -c retention.ms=7776000000 \
  2>/dev/null || echo "  mda.sanctions.updates already exists"

# GDELT events
$RPK topic create mda.events.gdelt.raw \
  -p 4 -r 1 \
  -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.events.gdelt.raw already exists"

$RPK topic create mda.events.gdelt.mda_filtered \
  -p 4 -r 1 \
  -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.events.gdelt.mda_filtered already exists"

# UAS detections
$RPK topic create mda.uas.detections.raw \
  -p 4 -r 1 \
  -c retention.ms=1209600000 \
  2>/dev/null || echo "  mda.uas.detections.raw already exists"

$RPK topic create mda.uas.remote_id \
  -p 4 -r 1 \
  -c retention.ms=1209600000 \
  2>/dev/null || echo "  mda.uas.remote_id already exists"

$RPK topic create mda.uas.flight_paths \
  -p 4 -r 1 \
  -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.uas.flight_paths already exists"

# Interdictions and intelligence
$RPK topic create mda.interdictions.new \
  -p 2 -r 1 \
  -c retention.ms=7776000000 \
  2>/dev/null || echo "  mda.interdictions.new already exists"

$RPK topic create mda.intelligence.reports \
  -p 2 -r 1 \
  -c retention.ms=7776000000 \
  2>/dev/null || echo "  mda.intelligence.reports already exists"

# NER extractions
$RPK topic create mda.ner.extractions \
  -p 4 -r 1 \
  -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.ner.extractions already exists"

# Alerts
$RPK topic create mda.alerts.composite \
  -p 4 -r 1 \
  -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.alerts.composite already exists"

# Dead letter queue: 7-day retention
$RPK topic create mda.dlq \
  -p 2 -r 1 \
  -c retention.ms=604800000 \
  2>/dev/null || echo "  mda.dlq already exists"

# GFW events
$RPK topic create mda.events.gfw.raw \
  -p 4 -r 1 \
  -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.events.gfw.raw already exists"

echo ""
echo "All topics created. Listing:"
$RPK topic list
