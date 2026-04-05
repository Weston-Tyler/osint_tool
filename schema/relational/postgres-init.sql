-- ============================================================
-- MDA Intelligence Database — PostgreSQL Schema Initialization
-- Run on first container start via docker-entrypoint-initdb.d
-- ============================================================

-- Create extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS age;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- Load AGE
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

-- Create MDA graph in AGE (for hybrid Cypher-over-SQL queries)
SELECT create_graph('mda');

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE mda TO mda;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO mda;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO mda;
