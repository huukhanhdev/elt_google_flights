-- sql/init.sql
-- Run automatically by Postgres on first container start.
-- Creates schemas and the raw flights table.

-- ─────────────────────────────────────────────
-- Schemas
-- ─────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

-- ─────────────────────────────────────────────
-- Raw layer
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.flight_searches (
    id              SERIAL PRIMARY KEY,
    search_id       VARCHAR(32)  NOT NULL,           -- MD5 hex: (origin+dest+dep_date+run_date)
    origin          VARCHAR(10)  NOT NULL,
    destination     VARCHAR(10)  NOT NULL,
    departure_date  DATE         NOT NULL,
    run_date        DATE         NOT NULL,            -- date this pipeline run occurred
    raw_payload     JSONB        NOT NULL,            -- full SerpApi JSON response
    pulled_at       TIMESTAMPTZ  DEFAULT NOW(),
    CONSTRAINT uq_flight_searches_search_id UNIQUE (search_id)
);

CREATE INDEX IF NOT EXISTS idx_flight_searches_run_date
    ON raw.flight_searches (run_date);

CREATE INDEX IF NOT EXISTS idx_flight_searches_route
    ON raw.flight_searches (origin, destination, departure_date);
