-- =============================================================================
-- Migration 001: Private Mirror Monitor V1
-- =============================================================================
-- Adds:
--   - 3 columns to channel_messages: source_account, extracted_tickers, extracted_cas
--   - new table mirror_merged_signals (dedup output)
--   - new table mirror_digests (digest history + LLM-failure logging)
--
-- Ordered steps (G4 partial-state safety):
--   1. ALTER TABLE channel_messages (DEFAULT 'main' makes new INSERTs safe)
--   2. Run backfill block (sets source_account='main' for existing rows in 10k batches)
--   3. CREATE TABLE mirror_merged_signals + mirror_digests
--   4. Create indexes
--
-- Apply: psql -U xanalyst -d xanalyst -f scripts/migrate_001_mirror_v1.sql
-- Revert: psql -U xanalyst -d xanalyst -f scripts/migrate_001_mirror_v1_revert.sql
-- =============================================================================


-- Step 1: ALTER channel_messages ---------------------------------------------
ALTER TABLE channel_messages
    ADD COLUMN IF NOT EXISTS source_account VARCHAR(30) NOT NULL DEFAULT 'main',
    ADD COLUMN IF NOT EXISTS extracted_tickers TEXT[],
    ADD COLUMN IF NOT EXISTS extracted_cas TEXT[];

-- Index for filtering by account (used by daily_summary, digest, etc.)
CREATE INDEX IF NOT EXISTS idx_messages_source_account ON channel_messages(source_account, message_date);


-- Step 2: Backfill existing rows in batches ----------------------------------
-- DEFAULT 'main' covers new INSERTs; backfill ensures existing rows are also set.
-- Batched to avoid long table locks on large tables (currently small but future-proof).
DO $$
DECLARE
    updated INT;
    total INT := 0;
BEGIN
    LOOP
        UPDATE channel_messages SET source_account = 'main'
        WHERE id IN (
            SELECT id FROM channel_messages
            WHERE source_account IS NULL OR source_account = ''
            LIMIT 10000
        );
        GET DIAGNOSTICS updated = ROW_COUNT;
        total := total + updated;
        EXIT WHEN updated = 0;
    END LOOP;
    RAISE NOTICE 'Backfill complete: % rows updated', total;
END $$;


-- Step 3: mirror_merged_signals -----------------------------------------------
-- Dedup output. Single row per fingerprint per active window.
-- Single source of truth for Redis crash recovery.
CREATE TABLE IF NOT EXISTS mirror_merged_signals (
    id BIGSERIAL PRIMARY KEY,
    category VARCHAR(30) NOT NULL,           -- 'arb' | 'pump_dump' | 'meme' | 'whale'
    fingerprint VARCHAR(200) NOT NULL,       -- per-category dedup key
    first_seen TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL,
    window_closed_at TIMESTAMP,              -- NULL пока окно открыто
    n_sources INT NOT NULL DEFAULT 1,
    sources JSONB,                           -- [{"source": "Example Source/Low", "ts": "..."}, ...]
    leader_source VARCHAR(200),              -- первый источник
    time_to_consensus_sec INT,               -- ts(threshold reached) - ts(first)
    value_min DECIMAL(20,8),                 -- для arb: min spread; для pd: min %change
    value_max DECIMAL(20,8),
    asset VARCHAR(100),                      -- тикер или CA
    asset_chain VARCHAR(20),                 -- 'sol' | 'eth' | 'bsc' | 'cex'
    telegram_msg_id BIGINT,                  -- ID сообщения для edit'ов (NULL пока не запостили)
    telegram_topic_id INT,
    emitted_locked BOOLEAN DEFAULT FALSE,    -- TRUE после lock на window close
    late_echo_count INT DEFAULT 0,
    edit_failed_count INT DEFAULT 0,         -- инкрементится при fail'е edit_message_in_topic
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_merged_fingerprint ON mirror_merged_signals(fingerprint, window_closed_at);
CREATE INDEX IF NOT EXISTS idx_merged_category_ts ON mirror_merged_signals(category, first_seen);
CREATE INDEX IF NOT EXISTS idx_merged_asset ON mirror_merged_signals(asset);


-- Step 4: mirror_digests ------------------------------------------------------
-- LLM-generated digests history (2x/day) + failure log.
-- Used by future /digest YYYY-MM-DD command (V1.5).
CREATE TABLE IF NOT EXISTS mirror_digests (
    id BIGSERIAL PRIMARY KEY,
    period_start TIMESTAMP NOT NULL,
    period_end TIMESTAMP NOT NULL,
    slot VARCHAR(10),                        -- 'morning' (09:00) | 'evening' (21:00)
    provider VARCHAR(20),                    -- 'perplexity' | 'gemini' | 'groq' | NULL on failure
    status VARCHAR(20) NOT NULL DEFAULT 'ok',-- 'ok' | 'failed'
    error TEXT,
    content TEXT,
    telegram_msg_id BIGINT,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_digests_period ON mirror_digests(period_start, period_end);
CREATE INDEX IF NOT EXISTS idx_digests_status ON mirror_digests(status, created_at);
