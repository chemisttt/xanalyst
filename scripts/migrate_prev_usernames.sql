-- Миграция: добавить колонку prev_usernames в twitter_analyses
-- Запуск на VPS: psql -U xanalyst -d xanalyst -f scripts/migrate_prev_usernames.sql

ALTER TABLE twitter_analyses
    ADD COLUMN IF NOT EXISTS prev_usernames TEXT[];
