-- ============================================================
-- MDA Corporate Ownership — PostgreSQL Schema
-- Staging tables for company registry records, GLEIF cross-
-- referencing, entity resolution results, property ownership,
-- and shell company detection flags.
-- ============================================================

-- ── Company Registry Records (staging) ─────────────────────

CREATE TABLE IF NOT EXISTS company_registry_records (
    id                      BIGSERIAL PRIMARY KEY,
    source_registry         VARCHAR(64) NOT NULL,
    company_number          VARCHAR(128),
    lei                     VARCHAR(20),
    legal_name              TEXT NOT NULL,
    legal_name_normalized   TEXT,
    trading_name            TEXT,
    jurisdiction            VARCHAR(8),
    country_iso             CHAR(2),
    company_status          VARCHAR(64),
    company_status_detail   VARCHAR(128),
    company_type            VARCHAR(128),
    legal_form              VARCHAR(128),
    incorporation_date      DATE,
    dissolution_date        DATE,
    sic_codes               TEXT[],
    registered_address_line1 TEXT,
    registered_address_line2 TEXT,
    registered_address_city  VARCHAR(256),
    registered_address_region VARCHAR(128),
    registered_address_postal_code VARCHAR(32),
    registered_address_country CHAR(2),
    accounts_next_due       DATE,
    confirmation_next_due   DATE,
    has_charges             BOOLEAN DEFAULT FALSE,
    has_insolvency          BOOLEAN DEFAULT FALSE,
    raw_payload             JSONB,
    ingested_at             TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    graph_synced            BOOLEAN DEFAULT FALSE,
    graph_synced_at         TIMESTAMPTZ,
    CONSTRAINT uq_registry_company UNIQUE (source_registry, company_number)
);

CREATE INDEX IF NOT EXISTS idx_crr_lei ON company_registry_records (lei);
CREATE INDEX IF NOT EXISTS idx_crr_legal_name ON company_registry_records USING gin (legal_name_normalized gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_crr_jurisdiction ON company_registry_records (jurisdiction, company_status);
CREATE INDEX IF NOT EXISTS idx_crr_country ON company_registry_records (country_iso);
CREATE INDEX IF NOT EXISTS idx_crr_status ON company_registry_records (company_status);
CREATE INDEX IF NOT EXISTS idx_crr_incorporation ON company_registry_records (incorporation_date);
CREATE INDEX IF NOT EXISTS idx_crr_sic ON company_registry_records USING gin (sic_codes);
CREATE INDEX IF NOT EXISTS idx_crr_ingested ON company_registry_records (ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_crr_graph_sync ON company_registry_records (graph_synced) WHERE graph_synced = FALSE;
CREATE INDEX IF NOT EXISTS idx_crr_source ON company_registry_records (source_registry, ingested_at DESC);

-- ── GLEIF Cross-Reference Mapping ──────────────────────────

CREATE TABLE IF NOT EXISTS gleif_crossref_mapping (
    id                      BIGSERIAL PRIMARY KEY,
    lei                     VARCHAR(20) NOT NULL,
    registration_authority_id VARCHAR(32),
    registration_authority_entity_id VARCHAR(256),
    source_registry         VARCHAR(64),
    company_number          VARCHAR(128),
    jurisdiction            VARCHAR(8),
    relationship_type       VARCHAR(64) NOT NULL DEFAULT 'IS_DIRECTLY_CONSOLIDATED_BY',
    relationship_status     VARCHAR(32),
    parent_lei              VARCHAR(20),
    parent_legal_name       TEXT,
    ultimate_parent_lei     VARCHAR(20),
    ultimate_parent_legal_name TEXT,
    corroboration_level     VARCHAR(32),
    validation_sources      TEXT[],
    relationship_start_date DATE,
    relationship_end_date   DATE,
    gleif_last_update       TIMESTAMPTZ,
    ingested_at             TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_gleif_crossref UNIQUE (lei, parent_lei, relationship_type)
);

CREATE INDEX IF NOT EXISTS idx_gcm_lei ON gleif_crossref_mapping (lei);
CREATE INDEX IF NOT EXISTS idx_gcm_parent_lei ON gleif_crossref_mapping (parent_lei);
CREATE INDEX IF NOT EXISTS idx_gcm_ultimate_parent ON gleif_crossref_mapping (ultimate_parent_lei);
CREATE INDEX IF NOT EXISTS idx_gcm_registry ON gleif_crossref_mapping (source_registry, company_number);
CREATE INDEX IF NOT EXISTS idx_gcm_authority ON gleif_crossref_mapping (registration_authority_id, registration_authority_entity_id);
CREATE INDEX IF NOT EXISTS idx_gcm_jurisdiction ON gleif_crossref_mapping (jurisdiction);
CREATE INDEX IF NOT EXISTS idx_gcm_relationship ON gleif_crossref_mapping (relationship_type, relationship_status);
CREATE INDEX IF NOT EXISTS idx_gcm_corroboration ON gleif_crossref_mapping (corroboration_level);
CREATE INDEX IF NOT EXISTS idx_gcm_ingested ON gleif_crossref_mapping (ingested_at DESC);

-- ── Splink Entity Resolution Results ───────────────────────

CREATE TABLE IF NOT EXISTS splink_resolution_results (
    id                      BIGSERIAL PRIMARY KEY,
    cluster_id              VARCHAR(128) NOT NULL,
    entity_id_left          VARCHAR(256) NOT NULL,
    source_left             VARCHAR(64) NOT NULL,
    entity_id_right         VARCHAR(256) NOT NULL,
    source_right            VARCHAR(64) NOT NULL,
    match_probability       DECIMAL(8,7) NOT NULL,
    match_weight            DECIMAL(8,4),
    blocking_rule           VARCHAR(256),
    comparison_vector       JSONB,
    name_similarity         DECIMAL(5,4),
    address_similarity      DECIMAL(5,4),
    dob_match               BOOLEAN,
    jurisdiction_match      BOOLEAN,
    resolution_status       VARCHAR(32) DEFAULT 'AUTO_MATCHED',
    human_reviewed          BOOLEAN DEFAULT FALSE,
    human_reviewer          VARCHAR(128),
    reviewed_at             TIMESTAMPTZ,
    model_version           VARCHAR(32),
    resolved_at             TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_splink_pair UNIQUE (entity_id_left, source_left, entity_id_right, source_right)
);

CREATE INDEX IF NOT EXISTS idx_srr_cluster ON splink_resolution_results (cluster_id);
CREATE INDEX IF NOT EXISTS idx_srr_left ON splink_resolution_results (entity_id_left, source_left);
CREATE INDEX IF NOT EXISTS idx_srr_right ON splink_resolution_results (entity_id_right, source_right);
CREATE INDEX IF NOT EXISTS idx_srr_probability ON splink_resolution_results (match_probability DESC);
CREATE INDEX IF NOT EXISTS idx_srr_status ON splink_resolution_results (resolution_status);
CREATE INDEX IF NOT EXISTS idx_srr_human_review ON splink_resolution_results (human_reviewed) WHERE human_reviewed = FALSE AND match_probability BETWEEN 0.7 AND 0.95;
CREATE INDEX IF NOT EXISTS idx_srr_model ON splink_resolution_results (model_version, resolved_at DESC);

-- ── Property Ownership Records ─────────────────────────────

CREATE TABLE IF NOT EXISTS property_ownership_records (
    id                      BIGSERIAL PRIMARY KEY,
    title_number            VARCHAR(64),
    property_address        TEXT,
    property_city           VARCHAR(256),
    property_region         VARCHAR(128),
    property_postal_code    VARCHAR(32),
    property_country_iso    CHAR(2),
    property_type           VARCHAR(64),
    tenure                  VARCHAR(32),
    owner_name              TEXT NOT NULL,
    owner_name_normalized   TEXT,
    owner_entity_type       VARCHAR(32),
    owner_country_iso       CHAR(2),
    owner_company_number    VARCHAR(128),
    owner_lei               VARCHAR(20),
    price_paid              BIGINT,
    price_currency          CHAR(3) DEFAULT 'GBP',
    transaction_date        DATE,
    registration_date       DATE,
    multiple_address_indicator BOOLEAN DEFAULT FALSE,
    source_registry         VARCHAR(64) NOT NULL,
    raw_payload             JSONB,
    ingested_at             TIMESTAMPTZ DEFAULT NOW(),
    graph_synced            BOOLEAN DEFAULT FALSE,
    graph_synced_at         TIMESTAMPTZ,
    CONSTRAINT uq_property_owner UNIQUE (title_number, owner_name, source_registry)
);

CREATE INDEX IF NOT EXISTS idx_por_title ON property_ownership_records (title_number);
CREATE INDEX IF NOT EXISTS idx_por_owner_name ON property_ownership_records USING gin (owner_name_normalized gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_por_owner_company ON property_ownership_records (owner_company_number);
CREATE INDEX IF NOT EXISTS idx_por_owner_lei ON property_ownership_records (owner_lei);
CREATE INDEX IF NOT EXISTS idx_por_owner_country ON property_ownership_records (owner_country_iso);
CREATE INDEX IF NOT EXISTS idx_por_property_country ON property_ownership_records (property_country_iso, property_city);
CREATE INDEX IF NOT EXISTS idx_por_transaction_date ON property_ownership_records (transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_por_price ON property_ownership_records (price_paid DESC) WHERE price_paid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_por_source ON property_ownership_records (source_registry, ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_por_graph_sync ON property_ownership_records (graph_synced) WHERE graph_synced = FALSE;
CREATE INDEX IF NOT EXISTS idx_por_tenure ON property_ownership_records (tenure);

-- ── Shell Company Detection Flags ──────────────────────────

CREATE TABLE IF NOT EXISTS shell_company_flags (
    id                      BIGSERIAL PRIMARY KEY,
    entity_id               VARCHAR(256) NOT NULL,
    source_registry         VARCHAR(64) NOT NULL,
    company_number          VARCHAR(128),
    lei                     VARCHAR(20),
    legal_name              TEXT,
    jurisdiction            VARCHAR(8),
    country_iso             CHAR(2),

    -- Individual risk signals (boolean flags)
    flag_registered_agent_address   BOOLEAN DEFAULT FALSE,
    flag_mass_registration_address  BOOLEAN DEFAULT FALSE,
    flag_nominee_directors          BOOLEAN DEFAULT FALSE,
    flag_circular_ownership         BOOLEAN DEFAULT FALSE,
    flag_bearer_shares              BOOLEAN DEFAULT FALSE,
    flag_no_employees               BOOLEAN DEFAULT FALSE,
    flag_no_revenue                 BOOLEAN DEFAULT FALSE,
    flag_dormant_with_assets        BOOLEAN DEFAULT FALSE,
    flag_rapid_ownership_changes    BOOLEAN DEFAULT FALSE,
    flag_secrecy_jurisdiction       BOOLEAN DEFAULT FALSE,
    flag_layered_ownership          BOOLEAN DEFAULT FALSE,
    flag_same_day_incorporation     BOOLEAN DEFAULT FALSE,
    flag_missing_financials         BOOLEAN DEFAULT FALSE,
    flag_psc_exempt                 BOOLEAN DEFAULT FALSE,

    -- Aggregate scores
    shell_score             DECIMAL(5,3) NOT NULL DEFAULT 0.000,
    signal_count            SMALLINT DEFAULT 0,
    risk_tier               VARCHAR(16) NOT NULL DEFAULT 'LOW',
    ownership_depth         SMALLINT,
    address_reuse_count     INTEGER,
    director_overlap_count  INTEGER,

    -- Metadata
    detection_model_version VARCHAR(32),
    detected_at             TIMESTAMPTZ DEFAULT NOW(),
    last_recalculated_at    TIMESTAMPTZ DEFAULT NOW(),
    human_reviewed          BOOLEAN DEFAULT FALSE,
    human_reviewer          VARCHAR(128),
    reviewed_at             TIMESTAMPTZ,
    review_outcome          VARCHAR(32),

    CONSTRAINT uq_shell_flag UNIQUE (entity_id, source_registry)
);

CREATE INDEX IF NOT EXISTS idx_scf_shell_score ON shell_company_flags (shell_score DESC);
CREATE INDEX IF NOT EXISTS idx_scf_risk_tier ON shell_company_flags (risk_tier);
CREATE INDEX IF NOT EXISTS idx_scf_jurisdiction ON shell_company_flags (jurisdiction);
CREATE INDEX IF NOT EXISTS idx_scf_country ON shell_company_flags (country_iso);
CREATE INDEX IF NOT EXISTS idx_scf_company_number ON shell_company_flags (company_number);
CREATE INDEX IF NOT EXISTS idx_scf_lei ON shell_company_flags (lei);
CREATE INDEX IF NOT EXISTS idx_scf_entity ON shell_company_flags (entity_id);
CREATE INDEX IF NOT EXISTS idx_scf_detected ON shell_company_flags (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_scf_human_review ON shell_company_flags (human_reviewed) WHERE human_reviewed = FALSE AND risk_tier IN ('HIGH', 'CRITICAL');
CREATE INDEX IF NOT EXISTS idx_scf_model ON shell_company_flags (detection_model_version);
CREATE INDEX IF NOT EXISTS idx_scf_signals ON shell_company_flags (signal_count DESC, shell_score DESC);
