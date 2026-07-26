-- ─────────────────────────────────────────────────────────────────────────────
-- FlowState PostgreSQL Initialization Script
-- Runs once on first container start via docker-entrypoint-initdb.d/
-- ─────────────────────────────────────────────────────────────────────────────

-- Enable UUID generation (pgcrypto provides gen_random_uuid())
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ─── Flagged Transactions Table ───────────────────────────────────────────────
-- Stores every transaction that exceeded the fraud risk threshold (riskScore > 75)
CREATE TABLE IF NOT EXISTS flagged_transactions (
  id               UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
  transaction_id   VARCHAR(64)   NOT NULL UNIQUE,   -- deduplicated via Kafka message key
  user_id          VARCHAR(64)   NOT NULL,
  amount           NUMERIC(12,2) NOT NULL,
  timestamp        TIMESTAMPTZ   NOT NULL,           -- original transaction timestamp
  location         VARCHAR(128),
  risk_score       SMALLINT      NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
  velocity         SMALLINT      NOT NULL CHECK (velocity >= 0),
  created_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- ─── Indexes ─────────────────────────────────────────────────────────────────
-- Look up all flagged transactions for a specific user
CREATE INDEX IF NOT EXISTS idx_flagged_user_id
  ON flagged_transactions (user_id);

-- Quickly surface highest-risk transactions
CREATE INDEX IF NOT EXISTS idx_flagged_risk_score
  ON flagged_transactions (risk_score DESC);

-- Time-based queries (e.g., last hour of fraud activity)
CREATE INDEX IF NOT EXISTS idx_flagged_created_at
  ON flagged_transactions (created_at DESC);

-- ─── Seed Comment ────────────────────────────────────────────────────────────
-- No seed data needed; the consumer will populate this table at runtime.
