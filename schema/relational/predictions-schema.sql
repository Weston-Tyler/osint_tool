-- WorldFish predictive events — PostGIS spatial table.
-- Populated from mda.predictions.worldfish by processors/graph/graph_consumer.py
-- for predictions that carry a location. See worldfish/PREDICTION_CONTRACT.md.
--
-- Extensions must exist before the table that uses their types.
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS predicted_events (
    prediction_id        TEXT PRIMARY KEY,
    simulation_run_id    TEXT,
    predicted_event_type TEXT,
    domain               TEXT,
    confidence           DOUBLE PRECISION,
    confidence_label     TEXT,
    timeframe_min_days   INTEGER,
    timeframe_max_days   INTEGER,
    region               TEXT,
    location             GEOMETRY(Point, 4326),
    causal_trigger       TEXT,
    schema_version       TEXT,
    system_created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_predicted_events_location ON predicted_events USING GIST (location);
CREATE INDEX IF NOT EXISTS idx_predicted_events_type ON predicted_events (predicted_event_type);
CREATE INDEX IF NOT EXISTS idx_predicted_events_domain_conf ON predicted_events (domain, confidence);
