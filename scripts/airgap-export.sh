#!/usr/bin/env bash
# Air-gap deployment export script
#
# Creates a self-contained archive with all Docker images, configs,
# and data files needed to deploy MDA in an air-gapped environment.
#
# Usage: ./scripts/airgap-export.sh /path/to/export/dir

set -euo pipefail

EXPORT_DIR="${1:?Usage: $0 <export-dir>}"
IMAGES_DIR="${EXPORT_DIR}/images"
CONFIGS_DIR="${EXPORT_DIR}/configs"

echo "=== MDA Air-Gap Export ==="
echo "Export directory: ${EXPORT_DIR}"
echo ""

mkdir -p "${IMAGES_DIR}" "${CONFIGS_DIR}"

# ---- 1. Save Docker images ----
echo "Saving Docker images..."

IMAGES=(
    "memgraph/memgraph-platform:2.14.0"
    "apache/age:PG16"
    "docker.redpanda.com/redpandadata/redpanda:v23.3.7"
    "docker.redpanda.com/redpandadata/console:v2.4.3"
    "minio/minio:RELEASE.2024-03-26T22-10-45Z"
    "docker.elastic.co/elasticsearch/elasticsearch:8.13.0"
    "docker.osgeo.org/geoserver:2.25.1"
    "grafana/grafana:10.3.3"
    "quay.io/keycloak/keycloak:24.0.1"
    "traefik:v3.0"
    "prom/prometheus:v2.51.0"
)

for img in "${IMAGES[@]}"; do
    filename=$(echo "$img" | tr '/:' '_').tar.gz
    if [ -f "${IMAGES_DIR}/${filename}" ]; then
        echo "  [skip] ${img} (already exported)"
    else
        echo "  [save] ${img} -> ${filename}"
        docker pull "${img}" 2>/dev/null || true
        docker save "${img}" | gzip > "${IMAGES_DIR}/${filename}"
    fi
done

# Save locally built images
echo "Building and saving custom images..."
docker compose build 2>/dev/null || echo "  (some builds may fail without network)"

for svc in api worker-ais worker-sanctions worker-gdelt worker-uas graph-processor; do
    img_name="osint_tool-${svc}"
    if docker image inspect "${img_name}" &>/dev/null; then
        echo "  [save] ${img_name}"
        docker save "${img_name}" | gzip > "${IMAGES_DIR}/${img_name}.tar.gz"
    fi
done

# ---- 2. Copy project files ----
echo ""
echo "Copying project files..."

# Use rsync if available, otherwise cp
if command -v rsync &>/dev/null; then
    rsync -av --exclude='.git' --exclude='node_modules' --exclude='__pycache__' \
        --exclude='.env' --exclude='data/' --exclude='*.pyc' \
        ./ "${CONFIGS_DIR}/mda-system/"
else
    cp -r . "${CONFIGS_DIR}/mda-system/"
    rm -rf "${CONFIGS_DIR}/mda-system/.git"
fi

# ---- 3. Create import script ----
cat > "${EXPORT_DIR}/import.sh" << 'IMPORT_EOF'
#!/usr/bin/env bash
# Air-gap deployment import script
# Run this on the target air-gapped machine

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
IMAGES_DIR="${SCRIPT_DIR}/images"
CONFIGS_DIR="${SCRIPT_DIR}/configs"

echo "=== MDA Air-Gap Import ==="

# Load Docker images
echo "Loading Docker images..."
for img_file in "${IMAGES_DIR}"/*.tar.gz; do
    echo "  [load] $(basename "$img_file")"
    gunzip -c "$img_file" | docker load
done

# Copy project files
echo "Setting up project..."
cp -r "${CONFIGS_DIR}/mda-system" ~/mda-system
cd ~/mda-system

# Copy .env template
cp .env.example .env
echo ""
echo "IMPORTANT: Edit ~/mda-system/.env with strong passwords before starting!"
echo ""
echo "To start: cd ~/mda-system && make setup"
echo ""
echo "=== Import complete ==="
IMPORT_EOF

chmod +x "${EXPORT_DIR}/import.sh"

# ---- 4. Create manifest ----
echo ""
echo "Creating manifest..."
cat > "${EXPORT_DIR}/MANIFEST.txt" << EOF
MDA OSINT Database — Air-Gap Export
====================================
Created: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
Host: $(hostname)

Docker Images:
$(ls -lh "${IMAGES_DIR}" | tail -n +2)

Project Files:
$(find "${CONFIGS_DIR}/mda-system" -type f | wc -l) files

Instructions:
1. Transfer this directory to the air-gapped machine
2. Run: ./import.sh
3. Edit ~/mda-system/.env with strong passwords
4. Run: cd ~/mda-system && make setup
EOF

# ---- 5. Calculate total size ----
TOTAL_SIZE=$(du -sh "${EXPORT_DIR}" | cut -f1)
echo ""
echo "=== Export complete ==="
echo "Total size: ${TOTAL_SIZE}"
echo "Location: ${EXPORT_DIR}"
echo ""
echo "Transfer to air-gapped machine and run: ./import.sh"
