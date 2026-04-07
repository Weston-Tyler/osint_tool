#!/usr/bin/env bash
# Create Kafka topics for the Causal Chain Analysis Engine
set -euo pipefail

RPK="docker exec mda-redpanda rpk"

echo "Creating causal analysis Kafka topics via mda-redpanda container..."

# Articles for causal extraction
$RPK topic create mda.articles.raw \
  -p 8 -r 1 -c retention.ms=86400000 \
  2>/dev/null || echo "  mda.articles.raw exists"

# Raw causal extraction output
$RPK topic create mda.causal.extractions.raw \
  -p 8 -r 1 -c retention.ms=86400000 \
  2>/dev/null || echo "  mda.causal.extractions.raw exists"

# Validated causal triplets
$RPK topic create mda.causal.extractions.validated \
  -p 8 -r 1 -c retention.ms=604800000 \
  2>/dev/null || echo "  mda.causal.extractions.validated exists"

# Granger causality results
$RPK topic create mda.causal.granger.results \
  -p 4 -r 1 -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.causal.granger.results exists"

# Pattern trigger notifications
$RPK topic create mda.causal.patterns.triggered \
  -p 4 -r 1 -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.causal.patterns.triggered exists"

# New predictions from WorldFish
$RPK topic create mda.causal.predictions.new \
  -p 4 -r 1 -c retention.ms=7776000000 \
  2>/dev/null || echo "  mda.causal.predictions.new exists"

# WorldFish simulation commands
$RPK topic create mda.worldfish.simulations.triggered \
  -p 2 -r 1 -c retention.ms=86400000 \
  2>/dev/null || echo "  mda.worldfish.simulations.triggered exists"

# WorldFish simulation results
$RPK topic create mda.worldfish.simulations.completed \
  -p 2 -r 1 -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.worldfish.simulations.completed exists"

# Market anomalies
$RPK topic create mda.market.anomalies \
  -p 4 -r 1 -c retention.ms=604800000 \
  2>/dev/null || echo "  mda.market.anomalies exists"

# Humanitarian crisis scores
$RPK topic create mda.humanitarian.crisis_scores \
  -p 4 -r 1 -c retention.ms=7776000000 \
  2>/dev/null || echo "  mda.humanitarian.crisis_scores exists"

# Domain-specific alert topics
$RPK topic create mda.alerts.causal \
  -p 4 -r 1 -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.alerts.causal exists"
$RPK topic create mda.alerts.market \
  -p 4 -r 1 -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.alerts.market exists"
$RPK topic create mda.alerts.humanitarian \
  -p 4 -r 1 -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.alerts.humanitarian exists"

# ReliefWeb reports
$RPK topic create mda.reliefweb.reports \
  -p 4 -r 1 -c retention.ms=2592000000 \
  2>/dev/null || echo "  mda.reliefweb.reports exists"

echo ""
echo "Causal analysis topics created."
$RPK topic list | grep -E "causal|worldfish|market|humanitarian|reliefweb|articles"
