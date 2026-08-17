-- spec 004: CT Alpha Digest V1 — REVERT
-- Drop в reverse FK order. БЕЗ CASCADE — чтобы ошибка ordering catch'илась явно
-- (CASCADE молча маскирует mis-ordered DROP).
-- Apply: psql -U xanalyst -d xanalyst -f scripts/migrate_004_ct_digest_revert.sql

BEGIN;

-- ct_promises has FK к ct_digest_items → drop child first
DROP TABLE IF EXISTS ct_promises;

-- Standalone tables (no FK in current schema)
DROP TABLE IF EXISTS ct_feedback;
DROP TABLE IF EXISTS ct_dev_credibility;

-- Now parents
DROP TABLE IF EXISTS ct_digest_items;
DROP TABLE IF EXISTS ct_digest_ticks;

COMMIT;
