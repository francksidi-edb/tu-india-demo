#!/bin/bash
# =============================================================================
# TU CDC Pipeline — End-to-End Validation
# =============================================================================
# Checks every hop: EPAS source -> replication slot -> Kafka -> FlowServer job
# status -> WHPG target counts. Run from gpkickoff (has access to both EPAS
# on :5444 and Greenplum on :5432).
#
# Usage: ./validate_tu_pipeline.sh
# =============================================================================
set -uo pipefail

EPAS_PORT=5444
EPAS_DB=tu
EPAS_USER=enterprisedb

GP_HOST=localhost
GP_PORT=5432
GP_USER=gpadmin
GP_DB=tu
GP_SCHEMA=tu_bureau_demo

KAFKA_BIN=/opt/kafka/bin
KAFKA_BROKER=localhost:9092
TOPICS=(tu.oltp.credit_accounts tu.oltp.bureau_score_events)

echo "=============================================="
echo " TU CDC Pipeline Validation"
echo "=============================================="

echo ""
echo "► 1. EPAS source counts (oltp schema, port $EPAS_PORT)"
psql -h localhost -p "$EPAS_PORT" -U "$EPAS_USER" -d "$EPAS_DB" -c "
SELECT 'credit_accounts'     AS tbl, COUNT(*) AS row_count, MAX(event_ts) AS latest_ts FROM oltp.credit_accounts
UNION ALL
SELECT 'bureau_score_events', COUNT(*),                     MAX(score_ts)             FROM oltp.bureau_score_events;
" 2>&1 || echo "  ✗ Could not reach EPAS on :$EPAS_PORT / db $EPAS_DB"

echo ""
echo "► 2. Replication slot status (confirms the connector is actively consuming WAL)"
psql -h localhost -p "$EPAS_PORT" -U "$EPAS_USER" -d "$EPAS_DB" -c "
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag_bytes
FROM pg_replication_slots
WHERE slot_name = 'debezium_tu_slot';
" 2>&1

echo ""
echo "► 3. Kafka topic offsets (total messages produced so far)"
INNER="/tmp/kafka-offsets-$$.sh"
cat > "$INNER" << INNEREOF
#!/bin/bash
KAFKA_BIN="$KAFKA_BIN"
KAFKA_BROKER="$KAFKA_BROKER"
for T in ${TOPICS[@]}; do
    echo "  \$T:"
    if [[ -x "\$KAFKA_BIN/kafka-get-offsets.sh" ]]; then
        "\$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "\$KAFKA_BROKER" --topic "\$T" 2>/dev/null | sed 's/^/    /'
    else
        "\$KAFKA_BIN/kafka-run-class.sh" kafka.tools.GetOffsetShell \
            --broker-list "\$KAFKA_BROKER" --topic "\$T" --time -1 2>/dev/null | sed 's/^/    /'
    fi
done
INNEREOF
chmod 755 "$INNER"
sudo su - kafka -c "bash $INNER"
rm -f "$INNER"

echo ""
echo "► 4. FlowServer job status (tu_ jobs)"
FLOWCLI_OUT=$(flowcli --host localhost --port 6060 list 2>/dev/null)
echo "$FLOWCLI_OUT" | grep -iE "jobname|tu_load|tu-load" || echo "  ~ FlowServer not reachable, or tu jobs not submitted"

echo ""
echo "► 5. WHPG target counts ($GP_SCHEMA schema, port $GP_PORT)"
psql -h "$GP_HOST" -p "$GP_PORT" -U "$GP_USER" -d "$GP_DB" -c "
SELECT 'credit_accounts'     AS tbl, COUNT(*) AS row_count, MAX(ingested_ts) AS latest_ingested FROM $GP_SCHEMA.credit_accounts
UNION ALL
SELECT 'bureau_score_events', COUNT(*),                     MAX(ingested_ts)                    FROM $GP_SCHEMA.bureau_score_events
UNION ALL
SELECT 'credit_inquiries',    COUNT(*),                     MAX(ingested_ts)                    FROM $GP_SCHEMA.credit_inquiries
UNION ALL
SELECT 'lender_feed_landing', COUNT(*),                     MAX(ingested_ts)                    FROM $GP_SCHEMA.lender_feed_landing;
" 2>&1 || echo "  ✗ Could not read $GP_SCHEMA — does the schema exist yet in database '$GP_DB'?"

echo ""
echo "=============================================="
echo " How to read this:"
echo "  • credit_accounts target  <= EPAS source row_count  (merge/upsert: 1 row per account_id, updates don't add rows)"
echo "  • bureau_score_events target == EPAS source row_count (insert-only, 1:1 with source)"
echo "  • lender_feed_landing target == credit_accounts + bureau_score_events + credit_inquiries combined"
echo "    (it's the audit trail fed by all three jobs)"
echo "  • replication slot 'active' should be 't' whenever connect.sh is running"
echo "  • lag_bytes near 0 means FlowServer/Kafka have caught up to the WAL"
echo "=============================================="

