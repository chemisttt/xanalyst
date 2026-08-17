-- spec 003: CatchMint Mint Radar — migration
-- Создаёт таблицу catchmint_alerts для хранения burst-алертов + red-flag enrichment snapshot.
-- Apply: psql -U xanalyst -d xanalyst -f scripts/migrate_003_catchmint.sql
-- Revert: scripts/migrate_003_catchmint_revert.sql

BEGIN;

CREATE TABLE IF NOT EXISTS catchmint_alerts (
    id BIGSERIAL PRIMARY KEY,
    address VARCHAR(64) NOT NULL,
    chain VARCHAR(20) NOT NULL,
    name VARCHAR(200),
    image_url TEXT,

    -- snapshot at first alert
    first_bucket_count INT NOT NULL,
    first_total_counts INT NOT NULL,
    first_total_supply INT,
    max_supply INT,                                   -- C2: всегда из overview scalar
    is_verified BOOLEAN NOT NULL,
    simulation_passed BOOLEAN NOT NULL,

    -- live state
    peak_bucket_count INT NOT NULL,
    last_bucket_count INT NOT NULL,
    last_total_counts INT NOT NULL,

    -- red-flag enrichment
    flag_labels JSONB,
    flag_count INT DEFAULT 0,
    notable_flag_count INT DEFAULT 0,
    hide_count INT DEFAULT 0,
    severity VARCHAR(10) NOT NULL DEFAULT 'safe',     -- M4: NOT NULL DEFAULT
    deployer VARCHAR(64),
    deployed_at TIMESTAMP,
    first_mint_at TIMESTAMP,
    is_proxy BOOLEAN,
    implementation_address VARCHAR(64),
    unique_wallets INT,
    twitter_url TEXT,
    discord_url TEXT,
    website_url TEXT,

    -- TG state
    telegram_msg_id BIGINT,
    telegram_topic_id INT,
    emitted_locked BOOLEAN DEFAULT FALSE,
    edit_failed_count INT DEFAULT 0,

    -- timing
    first_seen TIMESTAMP NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    closed_at TIMESTAMP,
    last_enrich_at TIMESTAMP,                         -- для rate-limit enrichment refresh

    -- audit
    skip_reasons TEXT,                                -- для emit-locked skipped rows

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_catchmint_address ON catchmint_alerts(address);
-- m2: один активный alert на address (защита от дублей при concurrent restart / orphan recovery)
CREATE UNIQUE INDEX IF NOT EXISTS idx_catchmint_one_active
    ON catchmint_alerts(address) WHERE closed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_catchmint_first_seen ON catchmint_alerts(first_seen DESC);
CREATE INDEX IF NOT EXISTS idx_catchmint_severity ON catchmint_alerts(severity, first_seen DESC);

COMMIT;
