.PHONY: help up down status logs topics es-indices test lint clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---- Docker Compose ----

up: ## Start all services
	docker compose up -d

down: ## Stop all services
	docker compose down

status: ## Show service status
	docker compose ps

logs: ## Tail all logs
	docker compose logs -f --tail=50

# ---- Infrastructure Setup ----

topics: ## Create Kafka topics
	./scripts/create-kafka-topics.sh

es-indices: ## Create Elasticsearch index mappings
	./scripts/setup-elasticsearch-indices.sh

init-db: ## Initialize Memgraph schema
	docker exec mda-memgraph mgconsole < schema/graph/memgraph-init.cypher

setup: up ## Full setup: start services, create topics, ES indices, init DB
	@echo "Waiting for services to be healthy..."
	sleep 30
	$(MAKE) topics
	$(MAKE) es-indices
	$(MAKE) init-db
	@echo "MDA system ready!"

# ---- Development ----

test: ## Run unit tests
	python -m pytest tests/unit/ -v --tb=short

smoke-test: ## Run smoke tests (all ingesters)
	python -m pytest tests/smoke/ -v --tb=short

verify: ## Full system verification (requires running stack)
	./scripts/verify-system.sh

verify-quick: ## Quick verification (skip live data fetches)
	./scripts/verify-system.sh --quick

lint: ## Run linter
	ruff check .

format: ## Auto-format code
	ruff format .

# ---- Data Ingestion ----

ingest-ofac: ## Run OFAC SDN ingestion
	python workers/sanctions/ofac_sdn_ingester.py

ingest-opensanctions: ## Run OpenSanctions ingestion
	python workers/sanctions/opensanctions_ingester.py

ingest-gdelt: ## Run GDELT latest events ingestion
	python workers/gdelt/gdelt_ingester.py

# ---- Cleanup ----

clean: ## Remove all Docker volumes (DESTRUCTIVE)
	@echo "This will delete all data volumes. Press Ctrl+C to cancel."
	@sleep 5
	docker compose down -v
