#!/bin/bash
# =============================================================================
# TU CDC Demo — Reset EPAS Source Tables
# =============================================================================
# Stops the Debezium connector, TRUNCATEs oltp.credit_accounts and
# oltp.bureau_score_events on EPAS, then optionally restarts the connector.
#
# This is the SOURCE-side counterpart to cleanup_tu_targets.sh (which resets
# the WHPG target side). They are independent — run either, or both, depending
# on what you want cleared.
#
# Usage: ./reset_tu_source.sh [--yes] [--restart-cdc]
#   --yes           skip the confirmation prompt
#   --restart-cdc   restart the connector after truncating (default: leave stopped)
# =============================================================================
set -uo pipefail

EPAS_HOST=localhost
EPAS_PORT=5444
EPAS_USER=enterprisedb
EPAS_DB=tu

CDC_PID_FILE=/tmp/connect.pid
CDC_CONNECT_SH=/home/kafka/connect.sh

SKIP_CONFIRM=0
RESTART_CDC=0
while [[ $# -gt 0 ]]; do
    case $1 in
        --yes)         SKIP_CONFIRM=1; shift ;;
        --restart-cdc) RESTART_CDC=1;  shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "=============================================="
echo " TU CDC Demo — Reset EPAS Source Tables"
echo "=============================================="
echo " This will:"
echo "   1. Stop the Debezium CDC connector (connect-standalone)"
echo "   2. TRUNCATE oltp.credit_accounts + oltp.bureau_score_events"
echo "      on EPAS ($EPAS_HOST:$EPAS_PORT / db $EPAS_DB)"
echo "   3. $([[ $RESTART_CDC -eq 1 ]] && echo 'Restart the connector' || echo 'Leave the connector STOPPED')"
echo "=============================================="
echo " NOTE: this does NOT touch the WHPG target tables (tu_bureau_demo schema)."
echo "       Run cleanup_tu_targets.sh separately for those."
echo "=============================================="

if [[ $SKIP_CONFIRM -eq 0 ]]; then
    read -rp "Type 'yes' to continue: " CONFIRM
    if [[ "$CONFIRM" != "yes" ]]; then
        echo "Aborted."
        exit 0
    fi
fi

echo ""
echo "► Reminder: if simulate_tu_cdc.sh is running in another terminal, Ctrl-C"
echo "  it now — this script can't stop that for you."
echo ""

echo "► Stopping CDC connector..."
PID=$(sudo su - kafka -c "cat $CDC_PID_FILE 2>/dev/null" | tr -d '[:space:]')
if [[ -n "$PID" ]] && sudo su - kafka -c "kill -0 $PID 2>/dev/null"; then
    sudo su - kafka -c "kill $PID 2>/dev/null; sleep 1; kill -9 $PID 2>/dev/null; rm -f $CDC_PID_FILE"
    echo "  ✓ Connector stopped (was PID $PID)"
else
    echo "  ~ Connector was not running"
fi
echo ""

echo "► Truncating EPAS source tables..."
psql -h "$EPAS_HOST" -p "$EPAS_PORT" -U "$EPAS_USER" -d "$EPAS_DB" -v ON_ERROR_STOP=1 -c "
TRUNCATE TABLE oltp.credit_accounts, oltp.bureau_score_events RESTART IDENTITY;
"
if [[ $? -eq 0 ]]; then
    echo "  ✓ EPAS source tables truncated (bureau_score_events id counter reset too)"
else
    echo "  ✗ Truncate failed — see error above"
    exit 1
fi
echo ""

if [[ $RESTART_CDC -eq 1 ]]; then
    echo "► Restarting CDC connector..."
    sudo su - kafka -c "bash $CDC_CONNECT_SH"
    sleep 2
    NEWPID=$(sudo su - kafka -c "cat $CDC_PID_FILE 2>/dev/null" | tr -d '[:space:]')
    if [[ -n "$NEWPID" ]]; then
        echo "  ✓ Connector restarted (PID $NEWPID)"
    else
        echo "  ✗ Connector failed to restart — check /home/kafka/connect.log"
    fi
    echo ""
    echo "=============================================="
    echo " ✓ EPAS source reset complete — connector running fresh"
    echo "=============================================="
else
    echo "=============================================="
    echo " ✓ EPAS source reset complete — connector left STOPPED"
    echo " Restart via the dashboard's CDC tab, or:"
    echo "   sudo su - kafka -c \"bash $CDC_CONNECT_SH\""
    echo "=============================================="
fi

echo ""
echo " If you also want the WHPG target tables cleared:"
echo "   ./cleanup_tu_targets.sh --yes --restart"

