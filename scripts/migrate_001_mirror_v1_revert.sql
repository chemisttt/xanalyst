-- =============================================================================
-- REVERT Migration 001: Private Mirror Monitor V1
-- =============================================================================
-- Restores schema to pre-001 state.
--
-- WARNING: dropping mirror_merged_signals + mirror_digests loses all data
-- collected since migration applied. Backup first if needed.
--
-- Apply: psql -U xanalyst -d xanalyst -f scripts/migrate_001_mirror_v1_revert.sql
-- Verify after:
--   \d+ channel_messages         (should NOT show extracted_*, source_account)
--   \dt mirror_*                  (should be empty — no orphan tables)
-- =============================================================================

DROP TABLE IF EXISTS mirror_digests;
DROP TABLE IF EXISTS mirror_merged_signals;

DROP INDEX IF EXISTS idx_messages_source_account;

ALTER TABLE channel_messages
    DROP COLUMN IF EXISTS source_account,
    DROP COLUMN IF EXISTS extracted_tickers,
    DROP COLUMN IF EXISTS extracted_cas;
