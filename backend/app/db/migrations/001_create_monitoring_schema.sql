-- ==============================================================================
-- ModelPulse v2 — Phase 4 Database Migration: Monitoring Runs & Drift Results
-- Compatible with PostgreSQL 15+ and TimescaleDB hypertables.
-- ==============================================================================

-- 1. Monitoring Runs Table
CREATE TABLE IF NOT EXISTS monitoring_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    model_name VARCHAR(64) NOT NULL,
    model_version VARCHAR(32) NOT NULL,
    run_type VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    reference_start TIMESTAMPTZ NOT NULL,
    reference_end TIMESTAMPTZ NOT NULL,
    reference_sample_count INT NOT NULL,
    current_start TIMESTAMPTZ NOT NULL,
    current_end TIMESTAMPTZ NOT NULL,
    current_sample_count INT NOT NULL,
    sample_count INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    dataset_drift_detected BOOLEAN NOT NULL,
    prediction_drift_psi DOUBLE PRECISION,
    prediction_drift_detected BOOLEAN,
    severity_score INT NOT NULL CHECK (severity_score >= 0 AND severity_score <= 100),
    severity_band VARCHAR(16) NOT NULL,
    severity_confidence VARCHAR(16) NOT NULL,
    component_d DOUBLE PRECISION,
    component_p DOUBLE PRECISION,
    component_q DOUBLE PRECISION,
    component_m DOUBLE PRECISION,
    weights_used JSONB NOT NULL,
    top_contributors JSONB,
    recommendations JSONB,
    raw_payload JSONB NOT NULL
);

-- 2. Feature-Level Drift Results Table
CREATE TABLE IF NOT EXISTS feature_drift_results (
    id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL REFERENCES monitoring_runs(run_id) ON DELETE CASCADE,
    feature_name VARCHAR(64) NOT NULL,
    feature_type VARCHAR(16) NOT NULL,
    votes VARCHAR(16) NOT NULL,
    votes_count INT NOT NULL,
    total_tests INT NOT NULL,
    drift_detected BOOLEAN NOT NULL,
    drift_contribution DOUBLE PRECISION NOT NULL,
    unseen_categories JSONB,
    reference_mean DOUBLE PRECISION,
    current_mean DOUBLE PRECISION,
    test_details JSONB
);

-- 3. Operational Indexes
CREATE INDEX IF NOT EXISTS idx_monitoring_runs_created_at ON monitoring_runs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_monitoring_runs_model ON monitoring_runs (model_name, model_version);
CREATE INDEX IF NOT EXISTS idx_feature_drift_run_id ON feature_drift_results (run_id);
CREATE INDEX IF NOT EXISTS idx_feature_drift_feature_name ON feature_drift_results (feature_name);

-- 5. Prediction Events Table
CREATE TABLE IF NOT EXISTS prediction_events (
    event_id VARCHAR(128) PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    model_name VARCHAR(64),
    model_version VARCHAR(32),
    prediction TEXT,
    score DOUBLE PRECISION,
    validation_status VARCHAR(32) NOT NULL,
    error_reason TEXT,
    features_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- 6. Prediction Event Indexes
CREATE INDEX IF NOT EXISTS idx_prediction_events_timestamp
    ON prediction_events (timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_prediction_events_model
    ON prediction_events (model_name, model_version);

CREATE INDEX IF NOT EXISTS idx_prediction_events_validation
    ON prediction_events (validation_status);

    
-- 4. TimescaleDB Hypertable Setup (executed when TimescaleDB extension is active)
-- Uncomment on production TimescaleDB instance:
-- SELECT create_hypertable('monitoring_runs', 'created_at', if_not_exists => TRUE);
