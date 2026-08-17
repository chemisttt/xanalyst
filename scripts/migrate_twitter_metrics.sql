-- Миграция: новые метрики для Twitter-аналитики
-- Запуск: psql -U xanalyst -d xanalyst -f scripts/migrate_twitter_metrics.sql

-- RT% — процент ретвитов среди последних твитов
ALTER TABLE twitter_analyses ADD COLUMN IF NOT EXISTS rt_percentage DECIMAL(5,2);

-- Growth velocity — скорость роста фолловеров (фолловеров/день)
ALTER TABLE twitter_analyses ADD COLUMN IF NOT EXISTS growth_velocity DECIMAL(10,2);

-- Возраст аккаунта в днях
ALTER TABLE twitter_analyses ADD COLUMN IF NOT EXISTS account_age_days INT;
