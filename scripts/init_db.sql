-- xanalyst: инициализация базы данных
-- Запуск: psql -U xanalyst -d xanalyst -f scripts/init_db.sql

-- ============================================================
-- Сообщения из Telegram каналов (для AI-сводки дня)
-- ============================================================
CREATE TABLE IF NOT EXISTS channel_messages (
    id BIGSERIAL PRIMARY KEY,

    -- Откуда пришло сообщение
    source VARCHAR(20) NOT NULL,          -- 'telegram' или 'discord'
    source_account VARCHAR(30) NOT NULL DEFAULT 'main',  -- 'main' (telegram_monitor) | 'private_mirror' (private_mirror_monitor)
    channel_id BIGINT NOT NULL,           -- ID канала/чата
    channel_name VARCHAR(200),            -- Название канала (для читаемости)

    -- Само сообщение
    message_id BIGINT NOT NULL,           -- ID сообщения в источнике
    author_name VARCHAR(200),             -- Автор (если есть)
    text TEXT,                            -- Текст сообщения
    has_media BOOLEAN DEFAULT FALSE,      -- Есть ли медиа (фото, видео)
    urls TEXT[],                          -- Извлечённые ссылки

    -- Извлечённые на write структурированные поля (private_mirror ingester)
    extracted_tickers TEXT[],             -- $TICKER из текста, например ['LAB', 'RAVE']
    extracted_cas TEXT[],                 -- CA (lowercase для EVM), например ['0xabc...', 'So111...']

    -- Время
    message_date TIMESTAMP NOT NULL,      -- Когда сообщение отправлено
    collected_at TIMESTAMP DEFAULT NOW(), -- Когда мы его собрали

    -- Не дублировать
    CONSTRAINT unique_message UNIQUE(source, channel_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_messages_date ON channel_messages(message_date);
CREATE INDEX IF NOT EXISTS idx_messages_source ON channel_messages(source, channel_id);
CREATE INDEX IF NOT EXISTS idx_messages_source_account ON channel_messages(source_account, message_date);


-- ============================================================
-- Private Mirror Monitor V1 — merged signals (dedup output)
-- ============================================================
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
    edit_failed_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_merged_fingerprint ON mirror_merged_signals(fingerprint, window_closed_at);
CREATE INDEX IF NOT EXISTS idx_merged_category_ts ON mirror_merged_signals(category, first_seen);
CREATE INDEX IF NOT EXISTS idx_merged_asset ON mirror_merged_signals(asset);


-- ============================================================
-- Private Mirror Monitor V1 — digest history
-- ============================================================
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


-- ============================================================
-- Результаты Twitter-анализа (retention: 30 дней)
-- ============================================================
CREATE TABLE IF NOT EXISTS twitter_analyses (
    id SERIAL PRIMARY KEY,

    -- Профиль
    handle VARCHAR(15) NOT NULL,          -- @username (без @)
    user_id BIGINT NOT NULL,              -- Числовой ID пользователя в Twitter
    display_name VARCHAR(100),
    bio TEXT,
    followers_count INT,
    following_count INT,
    tweets_count INT,
    account_created_at TIMESTAMP,
    verified BOOLEAN DEFAULT FALSE,

    -- Метрики
    engagement_rate DECIMAL(5,2),         -- % (лайки+RT+реплаи / фолловеры)
    bot_percentage DECIMAL(5,2),          -- % ботов среди фолловеров
    quality_score DECIMAL(5,2),           -- 100 - bot_percentage
    twitter_score INT,                    -- 0-100 (композитный балл)
    tier VARCHAR(5),                      -- S, A, B, C, D
    rt_percentage DECIMAL(5,2),           -- % ретвитов среди последних твитов
    growth_velocity DECIMAL(10,2),        -- скорость роста фолловеров/день
    account_age_days INT,                 -- возраст аккаунта в днях

    -- Reused name detection
    reused_name BOOLEAN DEFAULT FALSE,    -- handle ранее принадлежал другому user_id
    renamed BOOLEAN DEFAULT FALSE,        -- user_id сменил handle
    prev_usernames TEXT[],                -- история прошлых юзернеймов (из Alphagate)

    -- Мета
    source VARCHAR(20),                   -- откуда пришла ссылка ('discord')
    analyzed_at TIMESTAMP DEFAULT NOW(),
    analyzed_date DATE GENERATED ALWAYS AS (analyzed_at::date) STORED,

    CONSTRAINT unique_analysis_per_day UNIQUE(handle, analyzed_date)
);

CREATE INDEX IF NOT EXISTS idx_analyses_date ON twitter_analyses(analyzed_at);
CREATE INDEX IF NOT EXISTS idx_analyses_score ON twitter_analyses(twitter_score DESC);
CREATE INDEX IF NOT EXISTS idx_analyses_tier ON twitter_analyses(tier);


-- ============================================================
-- Snapshots: handle ↔ user_id (permanent, для reused name)
-- ============================================================
CREATE TABLE IF NOT EXISTS twitter_snapshots (
    id SERIAL PRIMARY KEY,
    handle VARCHAR(15) NOT NULL,
    user_id BIGINT NOT NULL,
    display_name VARCHAR(100),
    followers_count INT,
    first_seen TIMESTAMP DEFAULT NOW(),   -- Когда впервые увидели эту связку
    last_seen TIMESTAMP DEFAULT NOW(),    -- Последнее обновление

    -- Один handle может быть у разных user_id (reused name)
    CONSTRAINT unique_handle_user UNIQUE(handle, user_id)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_handle ON twitter_snapshots(handle);
CREATE INDEX IF NOT EXISTS idx_snapshots_user_id ON twitter_snapshots(user_id);


-- ============================================================
-- Watchlist: избранные Twitter-аккаунты для отслеживания
-- ============================================================
CREATE TABLE IF NOT EXISTS twitter_watchlist (
    id SERIAL PRIMARY KEY,
    handle VARCHAR(15) NOT NULL UNIQUE,    -- @username (без @)
    notes TEXT,                            -- Заметка пользователя
    last_score INT,                        -- Последний twitter_score
    last_tier VARCHAR(5),                  -- Последний tier
    added_at TIMESTAMP DEFAULT NOW()
);


-- ============================================================
-- Настройки (key-value)
-- ============================================================
CREATE TABLE IF NOT EXISTS settings (
    key VARCHAR(50) PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Дефолтные настройки
INSERT INTO settings (key, value) VALUES
    ('min_twitter_score', '30'),          -- Минимальный score для уведомления
    ('min_followers', '500'),             -- Минимум фолловеров
    ('max_bot_percentage', '60'),         -- Максимум % ботов
    ('daily_summary_hour', '21'),         -- Час отправки сводки (MSK)
    ('notify_tiers', 'S,A,B,C')          -- Какие тиры показывать
ON CONFLICT (key) DO NOTHING;


-- ============================================================
-- Spec 002 V1.5 — Theme Burst Detector
-- ============================================================

-- Extend channel_messages with telegram_topic_id (added by migrate_002).
ALTER TABLE channel_messages ADD COLUMN IF NOT EXISTS telegram_topic_id INT;
CREATE INDEX IF NOT EXISTS idx_msgs_chat_topic
  ON channel_messages(channel_id, telegram_topic_id)
  WHERE source_account='private_mirror';

-- alpha_signal_scores — per-cycle per-bucket audit trail.
CREATE TABLE IF NOT EXISTS alpha_signal_scores (
    id BIGSERIAL PRIMARY KEY,
    cycle_ts TIMESTAMP NOT NULL,
    cycle_ts_5min TIMESTAMP GENERATED ALWAYS AS (
        date_trunc('hour', cycle_ts) +
        (EXTRACT(MINUTE FROM cycle_ts)::int / 5) * INTERVAL '5 min'
    ) STORED,
    bucket_chat_id BIGINT NOT NULL,
    bucket_topic_id INT,
    bucket_label TEXT NOT NULL,
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
CREATE UNIQUE INDEX IF NOT EXISTS uq_signal_scores_5min_bucket
    ON alpha_signal_scores(cycle_ts_5min, bucket_chat_id, bucket_topic_id);
CREATE INDEX IF NOT EXISTS idx_signal_scores_bucket_ts
    ON alpha_signal_scores(bucket_chat_id, bucket_topic_id, cycle_ts DESC);
CREATE INDEX IF NOT EXISTS idx_signal_scores_fired
    ON alpha_signal_scores(fired, cycle_ts DESC) WHERE fired = TRUE;

-- alpha_events — fired events с LLM judgment + user feedback.
CREATE TABLE IF NOT EXISTS alpha_events (
    id BIGSERIAL PRIMARY KEY,
    bucket_chat_id BIGINT NOT NULL,
    bucket_topic_id INT,
    bucket_label TEXT NOT NULL,
    fired_at TIMESTAMP NOT NULL,
    signals_json JSONB NOT NULL,
    raw_msg_ids BIGINT[] NOT NULL,
    llm_topic TEXT,
    llm_summary TEXT,
    llm_tickers TEXT[],
    llm_cas TEXT[],
    llm_stance TEXT,
    llm_urgency TEXT,
    llm_noise BOOLEAN,
    llm_provider TEXT,
    llm_raw_response TEXT,
    tg_msg_id BIGINT,
    tg_topic_id INT,
    cooldown_until TIMESTAMP NOT NULL,
    shadow_mode BOOLEAN DEFAULT TRUE,
    was_raw_signal BOOLEAN DEFAULT FALSE,
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


-- ============================================================
-- Spec 003: CatchMint Mint Radar — burst-alert log + red-flag snapshot
-- ============================================================
CREATE TABLE IF NOT EXISTS catchmint_alerts (
    id BIGSERIAL PRIMARY KEY,
    address VARCHAR(64) NOT NULL,
    chain VARCHAR(20) NOT NULL,
    name VARCHAR(200),
    image_url TEXT,

    first_bucket_count INT NOT NULL,
    first_total_counts INT NOT NULL,
    first_total_supply INT,
    max_supply INT,
    is_verified BOOLEAN NOT NULL,
    simulation_passed BOOLEAN NOT NULL,

    peak_bucket_count INT NOT NULL,
    last_bucket_count INT NOT NULL,
    last_total_counts INT NOT NULL,

    flag_labels JSONB,
    flag_count INT DEFAULT 0,
    notable_flag_count INT DEFAULT 0,
    hide_count INT DEFAULT 0,
    severity VARCHAR(10) NOT NULL DEFAULT 'safe',
    deployer VARCHAR(64),
    deployed_at TIMESTAMP,
    first_mint_at TIMESTAMP,
    is_proxy BOOLEAN,
    implementation_address VARCHAR(64),
    unique_wallets INT,
    twitter_url TEXT,
    discord_url TEXT,
    website_url TEXT,

    telegram_msg_id BIGINT,
    telegram_topic_id INT,
    emitted_locked BOOLEAN DEFAULT FALSE,
    edit_failed_count INT DEFAULT 0,

    first_seen TIMESTAMP NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    closed_at TIMESTAMP,
    last_enrich_at TIMESTAMP,

    skip_reasons TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_catchmint_address ON catchmint_alerts(address);
CREATE UNIQUE INDEX IF NOT EXISTS idx_catchmint_one_active
    ON catchmint_alerts(address) WHERE closed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_catchmint_first_seen ON catchmint_alerts(first_seen DESC);
CREATE INDEX IF NOT EXISTS idx_catchmint_severity ON catchmint_alerts(severity, first_seen DESC);

-- ============================================================
-- Spec 004 — CT Alpha Digest V1 (NFT slice)
-- Sync with migrate_004_ct_digest.sql (kept here for test fixture pickup)
-- ============================================================

CREATE TABLE IF NOT EXISTS ct_digest_ticks (
    tick_id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP NULL,
    items_classified INT DEFAULT 0,
    llm_cost_cents REAL DEFAULT 0,
    digest_msg_id BIGINT NULL,
    status VARCHAR(20) DEFAULT 'running',
    manually_triggered_by BIGINT NULL
);
CREATE INDEX IF NOT EXISTS idx_ct_ticks_manual ON ct_digest_ticks(manually_triggered_by, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_ct_ticks_digest_msg ON ct_digest_ticks(digest_msg_id);

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
    bucket VARCHAR(40),
    novelty_score REAL,
    included BOOLEAN,
    tags JSONB DEFAULT '[]'::jsonb,
    mechanic_notes TEXT,
    contract_address_hint VARCHAR(64),
    promised_ts TIMESTAMP NULL,
    collection_name_hint VARCHAR(200),
    cross_refs JSONB DEFAULT '{}'::jsonb,
    short_id VARCHAR(8),
    UNIQUE (tick_id, tweet_id)
);
CREATE INDEX IF NOT EXISTS idx_ct_items_tick_bucket ON ct_digest_items(tick_id, bucket);
CREATE INDEX IF NOT EXISTS idx_ct_items_tweet ON ct_digest_items(tweet_id);
CREATE INDEX IF NOT EXISTS idx_ct_items_handle ON ct_digest_items(author_handle);

CREATE TABLE IF NOT EXISTS ct_promises (
    id BIGSERIAL PRIMARY KEY,
    item_id BIGINT NOT NULL REFERENCES ct_digest_items(id) ON DELETE CASCADE,
    contract_address VARCHAR(64),
    collection_name VARCHAR(200),
    promised_ts TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'announced'
        CHECK (status IN ('announced','upcoming','live','missed','unverifiable','deferred_check')),
    resolved_at TIMESTAMP NULL,
    ground_truth_source VARCHAR(40) NULL,
    total_supply_pre INT NULL,
    total_supply_post INT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ct_promises_status_ts ON ct_promises(status, promised_ts);
CREATE INDEX IF NOT EXISTS idx_ct_promises_item ON ct_promises(item_id);
CREATE INDEX IF NOT EXISTS idx_ct_promises_address ON ct_promises(contract_address) WHERE contract_address IS NOT NULL;

CREATE TABLE IF NOT EXISTS ct_dev_credibility (
    handle VARCHAR(64) PRIMARY KEY,
    completed INT DEFAULT 0,
    missed INT DEFAULT 0,
    unverifiable INT DEFAULT 0,
    score REAL NULL,
    last_updated TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ct_credibility_score ON ct_dev_credibility(score DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS ct_feedback (
    id BIGSERIAL PRIMARY KEY,
    digest_msg_id BIGINT NOT NULL,
    tick_id BIGINT NOT NULL,
    action VARCHAR(20) NOT NULL
        CHECK (action IN ('thumbs_up','thumbs_down','knew','reply_note')),
    item_short_ids TEXT[] NULL,
    note_text TEXT NULL,
    parsed_prefs JSONB NULL,
    user_id BIGINT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ct_feedback_msg ON ct_feedback(digest_msg_id);
CREATE INDEX IF NOT EXISTS idx_ct_feedback_tick ON ct_feedback(tick_id);
