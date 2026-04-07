#!/usr/bin/env bash
# Create Kafka topics for all expanded data source ingesters
set -euo pipefail

RPK="docker exec mda-redpanda rpk"

echo "Creating dataset expansion Kafka topics..."

# ACLED
$RPK topic create mda.acled.events -p 8 -r 1 -c retention.ms=7776000000 2>/dev/null || true

# UNODC
$RPK topic create mda.unodc.crime_stats -p 4 -r 1 -c retention.ms=31536000000 2>/dev/null || true

# SIPRI
$RPK topic create mda.sipri.arms_transfers -p 2 -r 1 -c retention.ms=31536000000 2>/dev/null || true

# Stanford Militant Groups
$RPK topic create mda.conflict.militant_groups -p 2 -r 1 -c retention.ms=31536000000 2>/dev/null || true

# FRED Economic
$RPK topic create mda.economic.fred.series -p 4 -r 1 -c retention.ms=7776000000 2>/dev/null || true
$RPK topic create mda.economic.anomalies -p 4 -r 1 -c retention.ms=2592000000 2>/dev/null || true

# World Bank
$RPK topic create mda.economic.worldbank.indicators -p 4 -r 1 -c retention.ms=31536000000 2>/dev/null || true

# ICIJ Offshore Leaks
$RPK topic create mda.icij.entities -p 4 -r 1 -c retention.ms=31536000000 2>/dev/null || true
$RPK topic create mda.icij.relationships -p 4 -r 1 -c retention.ms=31536000000 2>/dev/null || true

# EU/UN Sanctions
$RPK topic create mda.sanctions.eu -p 2 -r 1 -c retention.ms=7776000000 2>/dev/null || true
$RPK topic create mda.sanctions.un -p 2 -r 1 -c retention.ms=7776000000 2>/dev/null || true

# WFP Food Prices
$RPK topic create mda.wfp.food_prices -p 4 -r 1 -c retention.ms=7776000000 2>/dev/null || true
$RPK topic create mda.wfp.ipc_phases -p 4 -r 1 -c retention.ms=7776000000 2>/dev/null || true

# FEWS NET
$RPK topic create mda.fewsnet.ipc -p 4 -r 1 -c retention.ms=7776000000 2>/dev/null || true

# UNHCR
$RPK topic create mda.unhcr.displacement -p 4 -r 1 -c retention.ms=31536000000 2>/dev/null || true

# EM-DAT
$RPK topic create mda.humanitarian.disasters -p 4 -r 1 -c retention.ms=31536000000 2>/dev/null || true

# IOM DTM
$RPK topic create mda.humanitarian.displacement -p 4 -r 1 -c retention.ms=7776000000 2>/dev/null || true

# NASA FIRMS
$RPK topic create mda.geospatial.fires -p 8 -r 1 -c retention.ms=604800000 2>/dev/null || true

# Sentinel SAR
$RPK topic create mda.sentinel.dark_vessels -p 4 -r 1 -c retention.ms=2592000000 2>/dev/null || true

# ADS-B
$RPK topic create mda.adsb.aircraft -p 8 -r 1 -c retention.ms=604800000 2>/dev/null || true

# Maritime
$RPK topic create mda.maritime.ports -p 2 -r 1 -c retention.ms=31536000000 2>/dev/null || true
$RPK topic create mda.maritime.trade_flows -p 4 -r 1 -c retention.ms=31536000000 2>/dev/null || true
$RPK topic create mda.maritime.connectivity -p 2 -r 1 -c retention.ms=31536000000 2>/dev/null || true

# Crypto
$RPK topic create mda.crypto.prices -p 4 -r 1 -c retention.ms=7776000000 2>/dev/null || true
$RPK topic create mda.crypto.anomalies -p 4 -r 1 -c retention.ms=2592000000 2>/dev/null || true

echo ""
echo "All dataset topics created. Total topics:"
$RPK topic list | wc -l
