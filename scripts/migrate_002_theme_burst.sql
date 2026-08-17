-- migrate_002_theme_burst.sql — Spec 002 V1.5 (Anomaly-First Theme + Burst Detector)
-- Apply: psql -h 127.0.0.1 -U xanalyst -d xanalyst -f scripts/migrate_002_theme_burst.sql
-- Idempotent. Reverse: scripts/migrate_002_theme_burst_revert.sql
--
-- G4 compliance:
--   (a) Read-path NULL — telegram_topic_id pre-rev3 rows = NULL; signal queries fall back
--       to channel_name string match via `(telegram_topic_id=$2 OR (telegram_topic_id IS NULL
--       AND channel_name=$bucket_label))`.
--   (b) Backfill — infeasible for existing rows without re-fetching Telethon msg.reply_to.
--       Going forward (post Task 2 ingester patch), all new rows populate correctly.
--   (c) Reverse — soft-revert keeps telegram_topic_id column (data preserved), drops alpha_* only.

BEGIN;

-- =============================================================================
-- 1. ALTER channel_messages — add telegram_topic_id for V1.5 bucket-key joins.
-- =============================================================================
ALTER TABLE channel_messages ADD COLUMN IF NOT EXISTS telegram_topic_id INT;

-- Non-CONCURRENT index — table is <50k rows at deploy time.
-- For CONCURRENTLY version see migrate_002c_concurrent.sql (apply separately если non-CC fails).
CREATE INDEX IF NOT EXISTS idx_msgs_chat_topic
  ON channel_messages(channel_id, telegram_topic_id)
  WHERE source_account='private_mirror';

-- =============================================================================
-- 2. alpha_signal_scores — per-cycle per-bucket audit trail (30d retention via cron).
-- =============================================================================
CREATE TABLE IF NOT EXISTS alpha_signal_scores (
    id BIGSERIAL PRIMARY KEY,
    cycle_ts TIMESTAMP NOT NULL,
    -- Floor expression для UNIQUE — rev2 fix: raw cycle_ts allowed sub-second double-inserts
    cycle_ts_5min TIMESTAMP GENERATED ALWAYS AS (
        date_trunc('hour', cycle_ts) +
        (EXTRACT(MINUTE FROM cycle_ts)::int / 5) * INTERVAL '5 min'
    ) STORED,
    bucket_chat_id BIGINT NOT NULL,
    bucket_topic_id INT,
    bucket_label TEXT NOT NULL,
    -- 8 signals (S7 dropped rev 2: Telegram forum reply_to API collision)
    s1_msg_rate_z NUMERIC,
    s2_rate_ratio NUMERIC,
    s3_unique_author_z NUMERIC,
    s4_new_ticker_count INT,
    s5_new_ca_count INT,
    s6_rare_authors INT,
    s8_url_domain_burst INT,
    composite_score NUMERIC,
    n_distinct_signals INT,
    hard_fire BOOLEAN DEFAULT FALSE,
    soft_fire BOOLEAN DEFAULT FALSE,
    fired BOOLEAN DEFAULT FALSE,
    z_active BOOLEAN DEFAULT FALSE
);

-- Race protection — UNIQUE on (cycle_ts_5min, bucket) prevents double-insert
-- even если systemd-timer fires race condition somehow.
CREATE UNIQUE INDEX IF NOT EXISTS uq_signal_scores_5min_bucket
    ON alpha_signal_scores(cycle_ts_5min, bucket_chat_id, bucket_topic_id);

CREATE INDEX IF NOT EXISTS idx_signal_scores_bucket_ts
    ON alpha_signal_scores(bucket_chat_id, bucket_topic_id, cycle_ts DESC);

CREATE INDEX IF NOT EXISTS idx_signal_scores_fired
    ON alpha_signal_scores(fired, cycle_ts DESC) WHERE fired = TRUE;

-- =============================================================================
-- 3. alpha_events — fired events с LLM judgment + user feedback.
-- =============================================================================
CREATE TABLE IF NOT EXISTS alpha_events (
    id BIGSERIAL PRIMARY KEY,
    bucket_chat_id BIGINT NOT NULL,
    bucket_topic_id INT,
    bucket_label TEXT NOT NULL,
    fired_at TIMESTAMP NOT NULL,
    signals_json JSONB NOT NULL,
    raw_msg_ids BIGINT[] NOT NULL,
    -- LLM judge output (NULL если raw-signal fallback path)
    llm_topic TEXT,
    llm_summary TEXT,
    llm_tickers TEXT[],
    llm_cas TEXT[],
    llm_stance TEXT,
    llm_urgency TEXT,
    llm_noise BOOLEAN,
    llm_provider TEXT,
    llm_raw_response TEXT,                  -- forensics — prompt-injection detection
    -- Telegram emission
    tg_msg_id BIGINT,                       -- NULL если noise=true OR LLM-fail OR shadow_mode silent
    tg_topic_id INT,
    cooldown_until TIMESTAMP NOT NULL,
    -- Mode tracking
    shadow_mode BOOLEAN DEFAULT TRUE,
    was_raw_signal BOOLEAN DEFAULT FALSE,   -- TRUE если LLM all-providers-fail fallback
    -- User feedback (rev2 fix C3 — machine-verifiable validation gate)
    user_tag TEXT DEFAULT 'unreviewed' CHECK (user_tag IN ('unreviewed','plausible','real','noise','late_alpha')),
    user_tagged_at TIMESTAMP,
    user_tagged_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_alpha_events_bucket_fired
    ON alpha_events(bucket_chat_id, bucket_topic_id, fired_at DESC);

CREATE INDEX IF NOT EXISTS idx_alpha_events_shadow
    ON alpha_events(shadow_mode, fired_at DESC);

CREATE INDEX IF NOT EXISTS idx_alpha_events_user_tag
    ON alpha_events(user_tag, fired_at DESC) WHERE user_tag != 'unreviewed';

CREATE INDEX IF NOT EXISTS idx_alpha_events_raw_signal
    ON alpha_events(was_raw_signal, fired_at DESC) WHERE was_raw_signal = TRUE;

COMMIT;
