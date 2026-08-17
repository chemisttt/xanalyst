-- migrate_002c_concurrent.sql — Spec 002 V1.5 supplementary index (CONCURRENTLY-built).
--
-- Apply ONLY если non-concurrent index в migrate_002_theme_burst.sql
-- failed (e.g. blocked by long-running V1 transaction).
--
-- IMPORTANT: must run OUTSIDE transaction. Apply via:
--   psql -h 127.0.0.1 -U xanalyst -d xanalyst -c "$(cat scripts/migrate_002c_concurrent.sql)"
--
-- NOT via psql -f (which wraps в transaction).

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_msgs_chat_topic_concurrent
    ON channel_messages(channel_id, telegram_topic_id)
    WHERE source_account='private_mirror';
