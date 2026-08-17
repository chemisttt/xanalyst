-- spec 003 revert — destructive: dropping catchmint_alerts loses all alert history.
-- Use only when fully reverting the feature.
-- Apply: psql -U xanalyst -d xanalyst -f scripts/migrate_003_catchmint_revert.sql

BEGIN;

-- WARNING: destructive. Export first if you need the data:
--   pg_dump -t catchmint_alerts xanalyst > catchmint_alerts_$(date +%F).sql
DROP TABLE IF EXISTS catchmint_alerts CASCADE;

COMMIT;
