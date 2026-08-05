#!/bin/bash
# =============================================================================
# TU CDC Demo — Full Reconciled Reset (EPAS + Kafka + WHPG)
# =============================================================================
# Truncating EPAS and WHPG separately is NOT enough for a true reconciled
# reset: the Kafka topics still hold old messages, and if the jobs restart
# with --reset-to-earliest they'll replay that stale data back into WHPG,
# out of sync with the now-empty EPAS tables. This script resets ALL FOUR
# layers together in the correct order:
#
#   1. Stop FlowServer jobs (consumers)
#   2. Stop the Debezium connector (producer)
#   3. Truncate EPAS source tables
#   4. Drop the replication slot (Debezium recreates it fresh on restart)
#   5. Delete + recreate the Kafka topics (wipes stale messages)
#   6. Clear Kafka Connect's local offsets file (stale LSN reference is now
#      meaningless since the slot was dropped)
#   7. Truncate WHPG target tables
#   8. Restart the connector, then the jobs
#
# After this, EPAS, Kafka, and WHPG are all genuinely at zero — ready for
# simulate_tu_cdc.sh to generate fresh, fully reconciled data from scratch.
#
# Usage: ./reset_tu_full.sh [--yes]
#   --yes   skip the confirmation prompt
# =============================================================================
set -uo pipefail

EPAS_HOST=localhost
EPAS_PORT=5444
EPAS_USER=enterprisedb
EPAS_DB=tu

GP_HOST=localhost
GP_PORT=5432
GP_USER=gpadmin
GP_DB=tu
GP_SCHEMA=tu_bureau_demo

CDC_PID_FILE=/tmp/connect.pid
CDC_CONNECT_SH=/home/kafka/connect.sh
CDC_SLOT=debezium_tu_slot
KAFKA_BIN=/opt/kafka/bin
KAFKA_BROKER=localhost:9092
TOPICS=(tu.oltp.credit_accounts tu.oltp.bureau_score_events)

JOBS=(tu_load_credit_accounts tu_load_credit_inquiries tu_load_bureau_scores)

SKIP_CONFIRM=0
while [[ $# -gt 0 ]]; do
    case $1 in
        --yes) SKIP_CONFIRM=1; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "=============================================="
echo " TU CDC Demo — FULL Reconciled Reset"
echo "=============================================="
echo " This will, in order:"
echo "   1. Stop tu_* FlowServer jobs"
echo "   2. Stop the Debezium connector"
echo "   3. TRUNCATE EPAS oltp.credit_accounts + oltp.bureau_score_events"
echo "   4. Drop replication slot '$CDC_SLOT'"
echo "   5. Delete + recreate Kafka topics: ${TOPICS[*]}"
echo "   6. Clear Kafka Connect's offset file"
echo "   7. TRUNCATE WHPG $GP_SCHEMA.* (all 4 tables)"
echo "   8. Restart the connector, then the jobs"
echo "=============================================="
echo " Result: EPAS, Kafka, and WHPG all at zero — genuinely reconciled."
echo "=============================================="

if [[ $SKIP_CONFIRM -eq 0 ]]; then
    read -rp "Type 'yes' to continue: " CONFIRM
    if [[ "$CONFIRM" != "yes" ]]; then
        echo "Aborted."
        exit 0
    fi
fi

echo ""
echo "► Reminder: if simulate_tu_cdc.sh is running, Ctrl-C it now."
echo ""

# ── 1. Stop FlowServer jobs ───────────────────────────────────────────────────
echo "► [1/8] Stopping tu_* FlowServer jobs..."
for JOB in "${JOBS[@]}"; do
    OUT=$(flowcli --host localhost --port 6060 stop "$JOB" 2>&1)
    echo "  $JOB: $(echo "$OUT" | tail -1)"
done
echo ""

# ── 2. Stop connector ──────────────────────────────────────────────────────────
echo "► [2/8] Stopping CDC connector..."
PID=$(sudo su - kafka -c "cat $CDC_PID_FILE 2>/dev/null" | tr -d '[:space:]')
if [[ -n "$PID" ]] && sudo su - kafka -c "kill -0 $PID 2>/dev/null"; then
    sudo su - kafka -c "kill $PID 2>/dev/null; sleep 1; kill -9 $PID 2>/dev/null; rm -f $CDC_PID_FILE"
    echo "  ✓ Connector stopped (was PID $PID)"
else
    echo "  ~ Connector was not running"
fi
echo ""

# ── 3. Truncate EPAS source ───────────────────────────────────────────────────
echo "► [3/8] Truncating EPAS source tables..."
psql -h "$EPAS_HOST" -p "$EPAS_PORT" -U "$EPAS_USER" -d "$EPAS_DB" -v ON_ERROR_STOP=1 -c "
TRUNCATE TABLE oltp.credit_accounts, oltp.bureau_score_events RESTART IDENTITY;
"
[[ $? -eq 0 ]] && echo "  ✓ EPAS source truncated" || { echo "  ✗ Failed — aborting"; exit 1; }
echo ""

# ── 4. Drop replication slot ──────────────────────────────────────────────────
echo "► [4/8] Dropping replication slot '$CDC_SLOT'..."
psql -h "$EPAS_HOST" -p "$EPAS_PORT" -U "$EPAS_USER" -d "$EPAS_DB" -c "
SELECT pg_drop_replication_slot('$CDC_SLOT')
WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '$CDC_SLOT');
"
echo "  ✓ Slot dropped (Debezium recreates it automatically on next connect)"
echo ""

# ── 5. Delete + recreate Kafka topics ────────────────────────────────────────
echo "► [5/8] Resetting Kafka topics..."
INNER="/tmp/kafka-full-reset-$$.sh"
cat > "$INNER" << INNEREOF
#!/bin/bash
KAFKA_BIN="$KAFKA_BIN"
KAFKA_BROKER="$KAFKA_BROKER"
[[ -x "\$KAFKA_BIN/kafka-topics" ]] && KT="\$KAFKA_BIN/kafka-topics" || KT="\$KAFKA_BIN/kafka-topics.sh"
for TOPIC in ${TOPICS[@]}; do
    if "\$KT" --list --bootstrap-server "\$KAFKA_BROKER" 2>/dev/null | grep -q "^\${TOPIC}\$"; then
        "\$KT" --delete --topic "\$TOPIC" --bootstrap-server "\$KAFKA_BROKER" 2>/dev/null
        echo "  deleted: \$TOPIC"
    fi
done
echo "  waiting for deletion to confirm..."
for i in \$(seq 1 30); do
    REMAINING=0
    for TOPIC in ${TOPICS[@]}; do
        "\$KT" --list --bootstrap-server "\$KAFKA_BROKER" 2>/dev/null | grep -q "^\${TOPIC}\$" && REMAINING=\$((REMAINING+1))
    done
    [[ "\$REMAINING" -eq 0 ]] && break
    sleep 1
done
for TOPIC in ${TOPICS[@]}; do
    "\$KT" --create --topic "\$TOPIC" --partitions 3 --replication-factor 1 --bootstrap-server "\$KAFKA_BROKER" 2>/dev/null
    echo "  created: \$TOPIC"
done
INNEREOF
chmod 755 "$INNER"
sudo su - kafka -c "bash $INNER"
rm -f "$INNER"
echo ""

# ── 6. Clear Kafka Connect offsets ────────────────────────────────────────────
echo "► [6/8] Clearing Kafka Connect offset file..."
sudo su - kafka -c "rm -f /tmp/connect.offsets"
echo "  ✓ Offsets cleared"
echo ""

# ── 7. Truncate WHPG target ───────────────────────────────────────────────────
echo "► [7/8] Truncating WHPG target tables..."
psql -h "$GP_HOST" -p "$GP_PORT" -U "$GP_USER" -d "$GP_DB" -v ON_ERROR_STOP=1 -c "
TRUNCATE TABLE
    $GP_SCHEMA.credit_accounts,
    $GP_SCHEMA.bureau_score_events,
    $GP_SCHEMA.credit_inquiries,
    $GP_SCHEMA.lender_feed_landing;
"
[[ $? -eq 0 ]] && echo "  ✓ WHPG target truncated" || { echo "  ✗ Failed — aborting"; exit 1; }
echo ""

# ── 8. Restart connector, then jobs ───────────────────────────────────────────
echo "► [8/8] Restarting connector + jobs..."
sudo su - kafka -c "bash $CDC_CONNECT_SH"
sleep 3
NEWPID=$(sudo su - kafka -c "cat $CDC_PID_FILE 2>/dev/null" | tr -d '[:space:]')
if [[ -n "$NEWPID" ]]; then
    echo "  ✓ Connector restarted (PID $NEWPID)"
else
    echo "  ✗ Connector failed to restart — check /home/kafka/connect.log"
fi

for JOB in "${JOBS[@]}"; do
    OUT=$(flowcli --host localhost --port 6060 start --reset-to-earliest "$JOB" 2>&1)
    echo "  $JOB: $(echo "$OUT" | tail -1)"
done

echo ""
echo "=============================================="
echo " ✓ Full reconciled reset complete"
echo "=============================================="
echo " EPAS, Kafka, and WHPG are all at zero."
echo " Run simulate_tu_cdc.sh to generate fresh data, then verify with:"
echo "   ./validate_tu_pipeline.sh"
echo "   curl -s http://localhost:5055/api/cdc/compare | python3 -m json.tool"
echo "=============================================="

