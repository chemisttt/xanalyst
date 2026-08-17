-- spec 004: CT Alpha Digest V1 (NFT slice) — migration
-- Создаёт 5 таблиц для CT Alpha Digest: items, promises, dev_credibility, feedback, ticks.
-- Apply: psql -U xanalyst -d xanalyst -f scripts/migrate_004_ct_digest.sql
-- Revert: scripts/migrate_004_ct_digest_revert.sql
-- Round-trip check: scripts/test_migration_round_trip.sh

BEGIN;

-- ============================================================
-- ct_digest_ticks — каждый cron-tick metadata
-- ============================================================
CREATE TABLE IF NOT EXISTS ct_digest_ticks (
    tick_id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP NULL,
    items_classified INT DEFAULT 0,
    llm_cost_cents REAL DEFAULT 0,
    digest_msg_id BIGINT NULL,                       -- TG msg_id для feedback lookup
    status VARCHAR(20) DEFAULT 'running',            -- 'running' | 'completed' | 'failed' | 'paused'
    manually_triggered_by BIGINT NULL                -- Telegram user_id для /digest on-demand; NULL = cron tick
);

CREATE INDEX IF NOT EXISTS idx_ct_ticks_manual ON ct_digest_ticks(manually_triggered_by, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_ct_ticks_digest_msg ON ct_digest_ticks(digest_msg_id);

-- ============================================================
-- ct_digest_items — все классифицированные tweet'ы
-- ============================================================
CREATE TABLE IF NOT EXISTS ct_digest_items (
    id BIGSERIAL PRIMARY KEY,
    tick_id BIGINT NOT NULL,
    tweet_id VARCHAR(40) NOT NULL,
    tweet_url TEXT,
    author_handle VARCHAR(64),
    author_metadata JSONB DEFAULT '{}'::jsonb,
    posted_at TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT NOW(),
    raw_text TEXT,

    -- classification output
    bucket VARCHAR(40),                              -- early_signals | emerging_clusters | calendar_24h | calendar_3d | calendar_7d | state_reconcile | paid_hype
    novelty_score REAL,
    included BOOLEAN,
    tags JSONB DEFAULT '[]'::jsonb,
    mechanic_notes TEXT,
    contract_address_hint VARCHAR(64),               -- EIP-55 checksum form after normalize
    promised_ts TIMESTAMP NULL,
    collection_name_hint VARCHAR(200),

    -- cross-validation result (JSONB shape: {"twitter_watchlist":[handle..], "catchmint":[{name,msg_id}], "mirror_meme":[{asset,msg_id}]})
    cross_refs JSONB DEFAULT '{}'::jsonb,

    -- short id для feedback refs (e1/c2/k1/s1/p1 per-tick)
    short_id VARCHAR(8),

    UNIQUE (tick_id, tweet_id)
);

CREATE INDEX IF NOT EXISTS idx_ct_items_tick_bucket ON ct_digest_items(tick_id, bucket);
CREATE INDEX IF NOT EXISTS idx_ct_items_tweet ON ct_digest_items(tweet_id);
CREATE INDEX IF NOT EXISTS idx_ct_items_handle ON ct_digest_items(author_handle);

-- ============================================================
-- ct_promises — extracted future-dated mint promises
-- ============================================================
CREATE TABLE IF NOT EXISTS ct_promises (
    id BIGSERIAL PRIMARY KEY,
    item_id BIGINT NOT NULL REFERENCES ct_digest_items(id) ON DELETE CASCADE,
    contract_address VARCHAR(64),                    -- EIP-55 checksum form (NULL if extraction failed)
    collection_name VARCHAR(200),                    -- fallback identifier для name-search
    promised_ts TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'announced'
        CHECK (status IN ('announced','upcoming','live','missed','unverifiable','deferred_check')),
    resolved_at TIMESTAMP NULL,
    ground_truth_source VARCHAR(40) NULL,            -- 'catchmint_delta' | 'no_signal_pre' | 'catchmint_gone' | 'no_baseline' | 'no_address_no_match'
    total_supply_pre INT NULL,
    total_supply_post INT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ct_promises_status_ts ON ct_promises(status, promised_ts);
CREATE INDEX IF NOT EXISTS idx_ct_promises_item ON ct_promises(item_id);
CREATE INDEX IF NOT EXISTS idx_ct_promises_address ON ct_promises(contract_address) WHERE contract_address IS NOT NULL;

-- ============================================================
-- ct_dev_credibility — per-handle aggregate (Phase 2 will feed this back into classifier)
-- ============================================================
CREATE TABLE IF NOT EXISTS ct_dev_credibility (
    handle VARCHAR(64) PRIMARY KEY,
    completed INT DEFAULT 0,
    missed INT DEFAULT 0,
    unverifiable INT DEFAULT 0,
    score REAL NULL,                                 -- = completed / (completed + missed), NULL если < 3 events
    last_updated TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ct_credibility_score ON ct_dev_credibility(score DESC NULLS LAST);

-- ============================================================
-- ct_feedback — user interactions on digest posts
-- ============================================================
CREATE TABLE IF NOT EXISTS ct_feedback (
    id BIGSERIAL PRIMARY KEY,
    digest_msg_id BIGINT NOT NULL,                   -- references ct_digest_ticks.digest_msg_id (not FK — soft join)
    tick_id BIGINT NOT NULL,
    action VARCHAR(20) NOT NULL
        CHECK (action IN ('thumbs_up','thumbs_down','knew','reply_note')),
    item_short_ids TEXT[] NULL,                      -- ['e3','c1'] для reply_note
    note_text TEXT NULL,
    parsed_prefs JSONB NULL,                         -- {mechanic_pref, chain_pref, risk_tolerance, dev_pref, timing_pref}
    user_id BIGINT NULL,                             -- Telegram user_id
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ct_feedback_msg ON ct_feedback(digest_msg_id);
CREATE INDEX IF NOT EXISTS idx_ct_feedback_tick ON ct_feedback(tick_id);

COMMIT;
