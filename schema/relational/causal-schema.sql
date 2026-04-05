-- ============================================================
-- MDA Causal Chain Analysis — PostgreSQL Schema Extension
-- Temporal causal edge weight tracking + WorldFish memory
-- ============================================================

-- ── Causal Events (relational mirror for time-series queries) ──

CREATE TABLE IF NOT EXISTS causal_events (
    event_id            VARCHAR(64) PRIMARY KEY,
    event_type          VARCHAR(128) NOT NULL,
    domain              VARCHAR(32),
    occurred_at         TIMESTAMPTZ,
    location_lat        DOUBLE PRECISION,
    location_lon        DOUBLE PRECISION,
    location_region     VARCHAR(256),
    location_country_iso CHAR(2),
    confidence          DECIMAL(4,3),
    risk_score          DECIMAL(4,2),
    severity            DECIMAL(4,2),
    description         TEXT,
    source_ids          TEXT[],
    entities_involved   TEXT[],
    system_ingested_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ce_type_region ON causal_events (event_type, location_region);
CREATE INDEX IF NOT EXISTS idx_ce_occurred ON causal_events (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_ce_domain ON causal_events (domain, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_ce_country ON causal_events (location_country_iso, occurred_at DESC);

-- ── Temporal Causal Edge Weight History ─────────────────────

CREATE TABLE IF NOT EXISTS causal_edge_weight_history (
    id                      BIGSERIAL PRIMARY KEY,
    edge_id                 VARCHAR(64) NOT NULL,
    source_event_type       VARCHAR(128) NOT NULL,
    target_event_type       VARCHAR(128) NOT NULL,
    edge_type               VARCHAR(64) NOT NULL,
    domain_pair             VARCHAR(128),
    location_region         VARCHAR(256),
    snapshot_date           DATE NOT NULL,
    snapshot_period         VARCHAR(16) NOT NULL DEFAULT 'MONTHLY',
    confidence              DECIMAL(4,3) NOT NULL,
    strength                DECIMAL(4,3),
    supporting_instances    INTEGER DEFAULT 0,
    lag_median_days         SMALLINT,
    lag_std_days            DECIMAL(6,2),
    confidence_delta        DECIMAL(4,3),
    confidence_trend        VARCHAR(16),
    computed_by             VARCHAR(64) DEFAULT 'causal_graph_learner',
    computed_at             TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_edge_snapshot UNIQUE (edge_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_cewh_edge_date ON causal_edge_weight_history (edge_id, snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_cewh_types ON causal_edge_weight_history (source_event_type, target_event_type, snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_cewh_region ON causal_edge_weight_history (location_region, snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_cewh_domain ON causal_edge_weight_history (domain_pair, snapshot_date DESC);

-- ── Materialized View: Edge Confidence Trend ────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS causal_edge_trend AS
SELECT
    curr.edge_id,
    curr.source_event_type,
    curr.target_event_type,
    curr.edge_type,
    curr.domain_pair,
    curr.location_region,
    curr.confidence                             AS current_confidence,
    curr.supporting_instances                   AS current_instances,
    hist.confidence                             AS confidence_12mo_ago,
    hist.supporting_instances                   AS instances_12mo_ago,
    (curr.confidence - COALESCE(hist.confidence, curr.confidence)) AS confidence_change_12mo,
    CASE
        WHEN hist.confidence IS NULL THEN 'NEW'
        WHEN (curr.confidence - hist.confidence) > 0.05 THEN 'STRENGTHENING'
        WHEN (curr.confidence - hist.confidence) < -0.05 THEN 'WEAKENING'
        ELSE 'STABLE'
    END                                         AS trend_12mo,
    curr.snapshot_date                          AS latest_snapshot
FROM causal_edge_weight_history curr
LEFT JOIN causal_edge_weight_history hist
    ON curr.edge_id = hist.edge_id
    AND hist.snapshot_date = curr.snapshot_date - INTERVAL '12 months'
WHERE curr.snapshot_date = (
    SELECT MAX(snapshot_date) FROM causal_edge_weight_history
);

-- ── WorldFish Agent Memory (pgvector) ───────────────────────

CREATE TABLE IF NOT EXISTS worldfish_agent_memory (
    id                  BIGSERIAL PRIMARY KEY,
    simulation_id       VARCHAR(64) NOT NULL,
    agent_id            VARCHAR(64) NOT NULL,
    obi_object_id       VARCHAR(64),
    memory_text         TEXT NOT NULL,
    memory_type         VARCHAR(32) DEFAULT 'episodic',
    memory_embedding    vector(384),
    importance_score    DECIMAL(4,3) DEFAULT 0.500,
    access_count        INTEGER DEFAULT 0,
    last_accessed_at    TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    step_number         INTEGER,
    decay_factor        DECIMAL(5,4) DEFAULT 0.9950
);

CREATE INDEX IF NOT EXISTS idx_wf_mem_agent ON worldfish_agent_memory (simulation_id, agent_id, created_at DESC);

-- ── WorldFish Shared World Memory ───────────────────────────

CREATE TABLE IF NOT EXISTS worldfish_world_memory (
    id                  BIGSERIAL PRIMARY KEY,
    simulation_id       VARCHAR(64) NOT NULL,
    fact_text           TEXT NOT NULL,
    fact_embedding      vector(384),
    domain              VARCHAR(32),
    confidence          DECIMAL(4,3),
    source_agent_id     VARCHAR(64),
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ── WorldFish Simulation Results ────────────────────────────

CREATE TABLE IF NOT EXISTS worldfish_simulations (
    simulation_id       VARCHAR(64) PRIMARY KEY,
    seed_id             VARCHAR(64),
    trigger_event_id    VARCHAR(64),
    domain              VARCHAR(32),
    n_agents            INTEGER,
    n_steps_completed   INTEGER,
    total_simulated_days DOUBLE PRECISION,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    final_violence_level DECIMAL(4,3),
    final_enforcement_pressure DECIMAL(4,3),
    final_stability_index DECIMAL(4,3),
    state_history       JSONB,
    agent_action_summary JSONB,
    report_text         TEXT
);

CREATE INDEX IF NOT EXISTS idx_wf_sim_trigger ON worldfish_simulations (trigger_event_id);
CREATE INDEX IF NOT EXISTS idx_wf_sim_domain ON worldfish_simulations (domain, started_at DESC);

-- ── Market Causal Tracking ──────────────────────────────────

CREATE TABLE IF NOT EXISTS market_price_anomalies (
    id                  BIGSERIAL PRIMARY KEY,
    ticker              VARCHAR(16) NOT NULL,
    anomaly_date        DATE NOT NULL,
    z_score             DECIMAL(6,3),
    direction           VARCHAR(8),
    price_before        DECIMAL(12,4),
    price_after         DECIMAL(12,4),
    pct_change          DECIMAL(8,5),
    detected_at         TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_anomaly UNIQUE (ticker, anomaly_date)
);

CREATE INDEX IF NOT EXISTS idx_mpa_ticker ON market_price_anomalies (ticker, anomaly_date DESC);

-- ── Humanitarian Crisis Scores ──────────────────────────────

CREATE TABLE IF NOT EXISTS humanitarian_crisis_scores (
    id                  BIGSERIAL PRIMARY KEY,
    country             VARCHAR(64) NOT NULL,
    crisis_type         VARCHAR(32) NOT NULL,
    probability         DECIMAL(4,3) NOT NULL,
    alert_level         VARCHAR(16),
    timeframe_days      INTEGER,
    contributing_patterns TEXT[],
    inform_score        DECIMAL(4,2),
    acled_30d_events    INTEGER,
    wfp_ipc_phase       SMALLINT,
    computed_at         TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_crisis_score UNIQUE (country, crisis_type, computed_at::date)
);

CREATE INDEX IF NOT EXISTS idx_hcs_country ON humanitarian_crisis_scores (country, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_hcs_alert ON humanitarian_crisis_scores (alert_level, computed_at DESC);
