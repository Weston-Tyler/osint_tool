-- ============================================================
-- MDA Intelligence Database — PostGIS Schema
-- Geospatial tables and AIS time-series
-- ============================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS vector;

-- ============================================================
-- AIS POSITION HISTORY (high-volume time-series)
-- ============================================================
CREATE TABLE IF NOT EXISTS ais_positions (
    id              BIGSERIAL PRIMARY KEY,
    mmsi            VARCHAR(9)   NOT NULL,
    imo             VARCHAR(7),

    -- Valid time (when the vessel was at this position)
    valid_time      TIMESTAMPTZ  NOT NULL,

    -- Position
    position        GEOMETRY(POINT, 4326) NOT NULL,
    speed_kts       REAL,
    heading         SMALLINT,
    course          SMALLINT,
    nav_status      SMALLINT,

    -- Vessel metadata (denormalized for query efficiency)
    vessel_name     VARCHAR(128),
    vessel_type     SMALLINT,

    -- Source provenance
    source          VARCHAR(32),
    confidence      REAL DEFAULT 0.9
);

CREATE INDEX IF NOT EXISTS ais_positions_geom_idx ON ais_positions USING GIST(position);
CREATE INDEX IF NOT EXISTS ais_positions_valid_time_idx ON ais_positions(valid_time);
CREATE INDEX IF NOT EXISTS ais_positions_mmsi_time_idx ON ais_positions(mmsi, valid_time);
CREATE INDEX IF NOT EXISTS ais_positions_imo_idx ON ais_positions(imo);

-- ============================================================
-- MARITIME ZONES (EEZ, territorial waters, JIATF AOR, etc.)
-- ============================================================
CREATE TABLE IF NOT EXISTS maritime_zones (
    zone_id         VARCHAR(64) PRIMARY KEY,
    name            VARCHAR(256),
    zone_type       VARCHAR(32),
    country_iso     CHAR(2),
    geometry        GEOMETRY(MULTIPOLYGON, 4326),
    area_sq_km      DOUBLE PRECISION,
    risk_level      VARCHAR(16),
    source          VARCHAR(64),
    valid_from      TIMESTAMPTZ,
    valid_to        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS maritime_zones_geom_idx ON maritime_zones USING GIST(geometry);
CREATE INDEX IF NOT EXISTS maritime_zones_type_idx ON maritime_zones(zone_type);

-- ============================================================
-- VESSEL TRACKS (LineStrings built from AIS positions)
-- ============================================================
CREATE TABLE IF NOT EXISTS vessel_tracks (
    track_id        BIGSERIAL PRIMARY KEY,
    imo             VARCHAR(7),
    mmsi            VARCHAR(9),
    track_start     TIMESTAMPTZ NOT NULL,
    track_end       TIMESTAMPTZ NOT NULL,
    geometry        GEOMETRY(LINESTRING, 4326),
    point_count     INTEGER,
    total_distance_m DOUBLE PRECISION,
    source          VARCHAR(32)
);

CREATE INDEX IF NOT EXISTS vessel_tracks_geom_idx ON vessel_tracks USING GIST(geometry);
CREATE INDEX IF NOT EXISTS vessel_tracks_time_idx ON vessel_tracks(track_start, track_end);
CREATE INDEX IF NOT EXISTS vessel_tracks_imo_idx ON vessel_tracks(imo);

-- ============================================================
-- KNOWN STS ZONES (hotspots for ship-to-ship transfers)
-- ============================================================
CREATE TABLE IF NOT EXISTS sts_zones (
    zone_id         VARCHAR(64) PRIMARY KEY,
    name            VARCHAR(256),
    geometry        GEOMETRY(POLYGON, 4326),
    country_iso     CHAR(2),
    risk_level      VARCHAR(16),
    last_incident   TIMESTAMPTZ,
    incident_count  INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS sts_zones_geom_idx ON sts_zones USING GIST(geometry);

-- ============================================================
-- DRUG TRAFFICKING ROUTES
-- ============================================================
CREATE TABLE IF NOT EXISTS trafficking_routes (
    route_id        VARCHAR(64) PRIMARY KEY,
    name            VARCHAR(256),
    route_type      VARCHAR(32),
    geometry        GEOMETRY(LINESTRING, 4326),
    method          VARCHAR(64),
    risk_level      VARCHAR(16),
    first_observed  TIMESTAMPTZ,
    last_observed   TIMESTAMPTZ,
    confidence      REAL
);

CREATE INDEX IF NOT EXISTS trafficking_routes_geom_idx ON trafficking_routes USING GIST(geometry);

-- ============================================================
-- UAS FLIGHT PATHS (with Z dimension for altitude)
-- ============================================================
CREATE TABLE IF NOT EXISTS uas_flight_paths (
    path_id         VARCHAR(64) PRIMARY KEY,
    drone_id        VARCHAR(64),
    start_time      TIMESTAMPTZ,
    end_time        TIMESTAMPTZ,
    geometry        GEOMETRY(LINESTRINGZ, 4326),
    max_alt_m       REAL,
    crossed_border  BOOLEAN,
    country_a       CHAR(2),
    country_b       CHAR(2)
);

CREATE INDEX IF NOT EXISTS uas_flight_paths_geom_idx ON uas_flight_paths USING GIST(geometry);

-- ============================================================
-- INTERDICTION EVENT LOCATIONS (Points)
-- ============================================================
CREATE TABLE IF NOT EXISTS interdiction_locations (
    event_id        VARCHAR(64) PRIMARY KEY,
    event_time      TIMESTAMPTZ,
    location        GEOMETRY(POINT, 4326),
    country_eez     CHAR(2),
    in_territorial  BOOLEAN
);

CREATE INDEX IF NOT EXISTS interdiction_locations_geom_idx ON interdiction_locations USING GIST(location);

-- ============================================================
-- STAGING TABLES (for entity resolution)
-- ============================================================
CREATE TABLE IF NOT EXISTS ofac_vessels_staging (
    entity_id       VARCHAR(64) PRIMARY KEY,
    ofac_uid        INTEGER,
    name            VARCHAR(256),
    aliases         TEXT[],
    imo             VARCHAR(7),
    mmsi            VARCHAR(9),
    flag_state      VARCHAR(4),
    vessel_type     VARCHAR(64),
    sanctions_status VARCHAR(32),
    ofac_programs   TEXT[],
    remarks         TEXT,
    ingest_time     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS opensanctions_vessels_staging (
    entity_id       VARCHAR(128) PRIMARY KEY,
    name            VARCHAR(256),
    aliases         TEXT[],
    imo             VARCHAR(7),
    mmsi            VARCHAR(9),
    flag_state      VARCHAR(4),
    call_sign       VARCHAR(16),
    vessel_type     VARCHAR(64),
    gross_tonnage   REAL,
    sanctions_datasets TEXT[],
    ingest_time     TIMESTAMPTZ DEFAULT NOW()
);
