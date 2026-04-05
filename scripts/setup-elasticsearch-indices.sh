#!/usr/bin/env bash
# Create Elasticsearch index mappings for MDA
# Run: ./scripts/setup-elasticsearch-indices.sh

set -euo pipefail

ES_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"

echo "Setting up Elasticsearch indices on ${ES_URL}..."

for mapping_file in schema/elasticsearch/index-mappings/*.json; do
    index_name=$(basename "$mapping_file" .json)
    echo "Creating index: ${index_name}"
    curl -s -X PUT "${ES_URL}/${index_name}" \
        -H "Content-Type: application/json" \
        -d @"${mapping_file}" | python3 -m json.tool 2>/dev/null || echo "  (may already exist)"
    echo ""
done

echo "Elasticsearch indices ready:"
curl -s "${ES_URL}/_cat/indices?v" 2>/dev/null || echo "  (could not list indices)"
