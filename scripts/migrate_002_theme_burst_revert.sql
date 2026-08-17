-- migrate_002_theme_burst_revert.sql — Spec 002 V1.5 soft-revert.
--
-- BEFORE running this script — export user_tag data to preserve manual review work:
--   pg_dump -h 127.0.0.1 -U xanalyst --table=alpha_events --table=alpha_signal_scores \
--           xanalyst > backup_alpha_$(date +%s).sql
--
-- Then apply: psql -h 127.0.0.1 -U xanalyst -d xanalyst -f scripts/migrate_002_theme_burst_revert.sql
--
-- Soft-revert design (rev2 M13):
--   - Drops alpha_* tables (deletes audit + events data, hence pg_dump first).
--   - KEEPS channel_messages.telegram_topic_id column (data preserved — soft, не destructive).
--   - To fully revert column: uncomment ALTER TABLE DROP COLUMN at bottom (NOT recommended —
--     would lose ingester wiring data; spec rev 3 treats column as additive).

BEGIN;

DROP INDEX IF EXISTS idx_alpha_events_raw_signal;
DROP INDEX IF EXISTS idx_alpha_events_user_tag;
DROP INDEX IF EXISTS idx_alpha_events_shadow;
DROP INDEX IF EXISTS idx_alpha_events_bucket_fired;
DROP TABLE IF EXISTS alpha_events;

DROP INDEX IF EXISTS idx_signal_scores_fired;
DROP INDEX IF EXISTS idx_signal_scores_bucket_ts;
DROP INDEX IF EXISTS uq_signal_scores_5min_bucket;
DROP TABLE IF EXISTS alpha_signal_scores;

-- Soft-revert: telegram_topic_id column KEPT.
-- To fully revert (NOT recommended), uncomment below + run separately:
-- DROP INDEX IF EXISTS idx_msgs_chat_topic;
-- DROP INDEX IF EXISTS idx_msgs_chat_topic_concurrent;
-- ALTER TABLE channel_messages DROP COLUMN IF EXISTS telegram_topic_id;

COMMIT;
