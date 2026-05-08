-- Fivetran Monitor — PostgreSQL schema
-- Run: psql $DATABASE_URL < scripts/seed_db.sql

CREATE TABLE IF NOT EXISTS feature_store (
    id              BIGSERIAL PRIMARY KEY,
    connector_id    TEXT        NOT NULL,
    connector_name  TEXT,
    table_name      TEXT        NOT NULL,
    sync_time       TIMESTAMPTZ NOT NULL,
    rows_updated    BIGINT      DEFAULT 0,
    rows_pct_change DOUBLE PRECISION,
    z_score         DOUBLE PRECISION,
    rolling_mean_7d DOUBLE PRECISION,
    rolling_std_7d  DOUBLE PRECISION,
    rolling_mean_30d DOUBLE PRECISION,
    rolling_std_30d DOUBLE PRECISION,
    rows_lag1       BIGINT,
    rows_lag2       BIGINT,
    rows_lag3       BIGINT,
    day_of_week     SMALLINT,
    hour_of_day     SMALLINT,
    is_weekend      SMALLINT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (connector_id, table_name, sync_time)
);

CREATE TABLE IF NOT EXISTS ml_predictions (
    id              BIGSERIAL PRIMARY KEY,
    connector_id    TEXT        NOT NULL,
    table_name      TEXT        NOT NULL,
    sync_time       TIMESTAMPTZ NOT NULL,
    scored_at       TIMESTAMPTZ NOT NULL,
    rows_updated    BIGINT,
    anomaly_score   DOUBLE PRECISION,
    is_anomaly      BOOLEAN     DEFAULT FALSE,
    forecast_value  DOUBLE PRECISION,
    forecast_lower  DOUBLE PRECISION,
    forecast_upper  DOUBLE PRECISION,
    alert_reasons   TEXT,
    alerted         BOOLEAN     DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (connector_id, table_name, sync_time)
);

CREATE TABLE IF NOT EXISTS alert_log (
    id              BIGSERIAL PRIMARY KEY,
    connector_id    TEXT        NOT NULL,
    connector_name  TEXT,
    alert_type      TEXT        NOT NULL,
    issue           TEXT        NOT NULL,
    severity        TEXT        NOT NULL,
    reason          TEXT,
    details         TEXT,
    fired_at        TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS connector_metadata (
    connector_id    TEXT        PRIMARY KEY,
    connector_name  TEXT,
    service         TEXT,
    sync_frequency  INTEGER,
    paused          BOOLEAN     DEFAULT FALSE,
    last_seen_at    TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_feature_store_connector_table_time
    ON feature_store (connector_id, table_name, sync_time DESC);

CREATE INDEX IF NOT EXISTS idx_ml_predictions_anomaly
    ON ml_predictions (is_anomaly, alerted, scored_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_log_connector_issue
    ON alert_log (connector_id, issue, fired_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_log_fired_at
    ON alert_log (fired_at DESC);
