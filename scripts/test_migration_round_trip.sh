#!/usr/bin/env bash
# spec 004: migration round-trip integrity test (ADR 0002 R-1)
#
# Snapshots ct_* schema columns before, after revert, and after re-apply.
# Asserts: revert leaves no ct_* tables; re-apply produces identical state to original.
#
# Usage: scripts/test_migration_round_trip.sh
# Requires: PG accessible via $DATABASE_URL or default xanalyst connection.
# Exit codes: 0 = round-trip OK; 1 = revert leaked state; 2 = re-apply diverged.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATE="$SCRIPT_DIR/migrate_004_ct_digest.sql"
REVERT="$SCRIPT_DIR/migrate_004_ct_digest_revert.sql"

PSQL_ARGS=()
if [ -n "${DATABASE_URL:-}" ]; then
    PSQL_ARGS+=("$DATABASE_URL")
else
    PSQL_ARGS+=("-U" "xanalyst" "-d" "xanalyst")
fi

snapshot_query="SELECT table_name || '.' || column_name || ':' || data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name LIKE 'ct_%'
                ORDER BY table_name, ordinal_position"

tmpdir=$(mktemp -d)
trap "rm -rf $tmpdir" EXIT

echo "[1/4] Ensure migrate applied (idempotent), snapshot original state..."
psql "${PSQL_ARGS[@]}" -f "$MIGRATE" > /dev/null
psql "${PSQL_ARGS[@]}" -t -A -c "$snapshot_query" > "$tmpdir/before.txt"
echo "    rows: $(wc -l < "$tmpdir/before.txt")"

echo "[2/4] Apply revert..."
psql "${PSQL_ARGS[@]}" -f "$REVERT" > /dev/null
psql "${PSQL_ARGS[@]}" -t -A -c "$snapshot_query" > "$tmpdir/after_revert.txt"
echo "    rows after revert: $(wc -l < "$tmpdir/after_revert.txt")"

if [ -s "$tmpdir/after_revert.txt" ]; then
    echo "FAIL: revert left non-empty ct_* state:"
    cat "$tmpdir/after_revert.txt"
    exit 1
fi
echo "    OK: revert clean."

echo "[3/4] Re-apply migrate..."
psql "${PSQL_ARGS[@]}" -f "$MIGRATE" > /dev/null
psql "${PSQL_ARGS[@]}" -t -A -c "$snapshot_query" > "$tmpdir/after_migrate.txt"
echo "    rows after re-apply: $(wc -l < "$tmpdir/after_migrate.txt")"

echo "[4/4] Diff vs original..."
if ! diff -q "$tmpdir/before.txt" "$tmpdir/after_migrate.txt"; then
    echo "FAIL: re-apply diverged from original:"
    diff "$tmpdir/before.txt" "$tmpdir/after_migrate.txt"
    exit 2
fi

echo "OK: round-trip identical. $(wc -l < "$tmpdir/before.txt") column rows preserved."
exit 0
