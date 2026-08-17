#!/bin/bash
# Deploy V1.5 theme_burst_worker to VPS — spec 002 Task 9.
#
# Run on VPS as botuser, with sudo for systemctl.
# Idempotent — safe to re-run.
#
# Usage:
#   ssh botuser@203.0.113.10
#   cd ~/xanalyst
#   bash deploy/deploy_v15.sh

set -euo pipefail

echo "=== xanalyst V1.5 deploy — spec 002 theme_burst_worker ==="
echo

# 1. Sync code
cd /opt/xanalyst
git pull --ff-only

# 2. Apply DB migration
echo "[1/5] Applying migrate_002_theme_burst.sql..."
PGPASSWORD="${DB_PASS:-changeme}" psql -h 127.0.0.1 -U xanalyst -d xanalyst \
    -f scripts/migrate_002_theme_burst.sql
echo "  ✓ Migration applied"

# 3. Verify schema
echo "[2/5] Verifying schema..."
PGPASSWORD="${DB_PASS:-changeme}" psql -h 127.0.0.1 -U xanalyst -d xanalyst -c "
    SELECT
      (SELECT COUNT(*) FROM information_schema.columns
       WHERE table_name='channel_messages' AND column_name='telegram_topic_id') AS topic_col,
      (SELECT COUNT(*) FROM information_schema.tables WHERE table_name='alpha_signal_scores') AS scores_table,
      (SELECT COUNT(*) FROM information_schema.tables WHERE table_name='alpha_events') AS events_table;
"

# 4. Install systemd units
echo "[3/5] Installing systemd unit + timer..."
sudo cp deploy/systemd/xanalyst-theme-burst-worker.service /etc/systemd/system/
sudo cp deploy/systemd/xanalyst-theme-burst-worker.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable xanalyst-theme-burst-worker.timer
sudo systemctl start xanalyst-theme-burst-worker.timer
echo "  ✓ systemd timer enabled and started"

# 5. Verify
echo "[4/5] Verifying timer status..."
sudo systemctl status xanalyst-theme-burst-worker.timer --no-pager | head -10

echo "[5/5] List timers..."
systemctl list-timers --all | grep theme_burst || echo "  (no theme_burst timer yet — first run in 2 min after boot)"

echo
echo "=== Deploy complete ==="
echo
echo "Next steps:"
echo "  1. Create forum topics in xanalyst bot: Theme Burst + Theme Burst Shadow"
echo "  2. Set TELEGRAM_MIRROR_THEME_BURST_TOPIC_ID + _SHADOW_TOPIC_ID in .env"
echo "  3. Wait for first tick (within 5min) — check logs/theme_burst_worker.log"
echo "  4. After 4h: run CHAT_TOPIC_WHITELIST discovery SQL (see shared/source_routing.py)"
echo "  5. Replace topic_id placeholders в source_routing.py CHAT_TOPIC_WHITELIST"
echo "  6. After 7d: review shadow alerts, run reconcile, flip THEME_BURST_DRY_RUN=false"
