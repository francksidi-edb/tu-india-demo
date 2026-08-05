#!/bin/bash
# =============================================================================
# TU CDC Demo — Truncate Target Tables
# =============================================================================
# Stops the three tu_* FlowServer jobs (so nothing writes mid-truncate — the
# credit_accounts job runs in merge/upsert mode, which would error against
# rows disappearing under it), truncates all four tu_bureau_demo target
# tables, then optionally restarts the jobs from earliest.
#
# Usage: ./cleanup_tu_targets.sh [--yes] [--restart]
#   --yes       skip the confirmation prompt
#   --restart   restart the tu_* FlowServer jobs after truncating (reset-to-earliest)
# =============================================================================
set -uo pipefail

GP_HOST=localhost
GP_PORT=5432
GP_USER=gpadmin
GP_DB=tu
GP_SCHEMA=tu_bureau_demo

JOBS=(tu_load_credit_accounts tu_load_credit_inquiries tu_load_bureau_scores)

SKIP_CONFIRM=0
RESTART=0
while [[ $# -gt 0 ]]; do
    case $1 in
        --yes)     SKIP_CONFIRM=1; shift ;;
        --restart) RESTART=1;      shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "=============================================="
echo " TU CDC Demo — Truncate Target Tables"
echo "=============================================="
echo " This will TRUNCATE (all rows, all four tables):"
echo "   $GP_SCHEMA.credit_accounts"
echo "   $GP_SCHEMA.bureau_score_events"
echo "   $GP_SCHEMA.credit_inquiries"
echo "   $GP_SCHEMA.lender_feed_landing"
echo " in database '$GP_DB' on $GP_HOST:$GP_PORT"
echo "=============================================="

if [[ $SKIP_CONFIRM -eq 0 ]]; then
    read -rp "Type 'yes' to continue: " CONFIRM
    if [[ "$CONFIRM" != "yes" ]]; then
        echo "Aborted."
        exit 0
    fi
fi

echo ""
echo "► Stopping tu_* FlowServer jobs..."
for JOB in "${JOBS[@]}"; do
    OUT=$(flowcli --host localhost --port 6060 stop "$JOB" 2>&1)
    echo "  $JOB: $(echo "$OUT" | tail -1)"
done
sleep 1
echo ""

echo "► Truncating target tables..."
psql -h "$GP_HOST" -p "$GP_PORT" -U "$GP_USER" -d "$GP_DB" -v ON_ERROR_STOP=1 -c "
TRUNCATE TABLE
    $GP_SCHEMA.credit_accounts,
    $GP_SCHEMA.bureau_score_events,
    $GP_SCHEMA.credit_inquiries,
    $GP_SCHEMA.lender_feed_landing;
"
if [[ $? -eq 0 ]]; then
    echo "  ✓ Tables truncated"
else
    echo "  ✗ Truncate failed — see error above"
    exit 1
fi
echo ""

if [[ $RESTART -eq 1 ]]; then
    echo "► Restarting tu_* FlowServer jobs (reset-to-earliest)..."
    for JOB in "${JOBS[@]}"; do
        OUT=$(flowcli --host localhost --port 6060 start --reset-to-earliest "$JOB" 2>&1)
        echo "  $JOB: $(echo "$OUT" | tail -1)"
    done
    echo ""
    echo "=============================================="
    echo " ✓ Cleanup complete — jobs restarted fresh"
    echo "=============================================="
else
    echo "=============================================="
    echo " ✓ Cleanup complete — jobs left STOPPED"
    echo " Restart manually via the dashboard, or:"
    for JOB in "${JOBS[@]}"; do
        echo "   flowcli --host localhost --port 6060 start --reset-to-earliest $JOB"
    done
    echo "=============================================="
fi

