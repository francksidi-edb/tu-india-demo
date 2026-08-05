#!/bin/bash
# =============================================================================
# CDC Debug — walks the whole EPAS -> Debezium -> Kafka -> FlowServer -> WHPG
# pipeline in order, stopping at the FIRST broken stage with a specific fix.
#
# Unlike the previous version, this does NOT guess your connector's name,
# database, schema, or slot name. It asks Kafka Connect's own REST API what's
# actually registered and reads the real config back — ground truth, not a
# hardcoded assumption that silently mismatches your setup.
#
# Override only if you have MULTIPLE Postgres connectors on the same worker
# and need to pick a specific one:
#   CDC_CONNECTOR_NAME=edb-tu-cdc-connector ./scripts/debug-cdc.sh
#
# Usage: ./scripts/debug-cdc.sh
# =============================================================================

GREEN="\033[0;32m"; RED="\033[0;31m"; AMBER="\033[0;33m"; RESET="\033[0m"; BOLD="\033[1m"
ok()   { echo -e "  ${GREEN}✓${RESET} $*"; }
fail() { echo -e "  ${RED}✗${RESET} $*"; }
warn() { echo -e "  ${AMBER}~${RESET} $*"; }
step() { echo ""; echo -e "${BOLD}── $* ──${RESET}"; }

CONNECT_HOST="${CONNECT_HOST:-localhost}"
CONNECT_PORT="${CONNECT_PORT:-8083}"
KAFKA_BROKER="${KAFKA_BROKER:-localhost:9092}"
KAFKA_BIN="${KAFKA_BIN_DIR:-/opt/kafka/bin}"
EPAS_USER_OVERRIDE="${EPAS_USER:-}"

STOP_AT_FIRST_FAILURE=1

step "Stage 1: Kafka Connect worker (port $CONNECT_PORT)"
CONNECTORS_JSON=$(curl -sf "http://$CONNECT_HOST:$CONNECT_PORT/connectors" 2>/dev/null)
if [[ -z "$CONNECTORS_JSON" ]]; then
    fail "Cannot reach Kafka Connect at $CONNECT_HOST:$CONNECT_PORT"
    fail "FIX: start the worker — nohup /opt/kafka/bin/connect-standalone.sh /opt/kafka/config/connect-standalone.properties <your-connector.json> &"
    exit 1
fi
ok "Kafka Connect REST API reachable"
echo "  Connectors on this worker: $CONNECTORS_JSON"

step "Stage 2: identifying the Postgres CDC connector"
ALL_NAMES=$(echo "$CONNECTORS_JSON" | python3 -c "import json,sys; print('\n'.join(json.load(sys.stdin)))" 2>/dev/null)
if [[ -z "$ALL_NAMES" ]]; then
    fail "No connectors registered on this worker at all"
    fail "FIX: register one — see cdc/README-cdc.md"
    exit 1
fi

if [[ -n "$CDC_CONNECTOR_NAME" ]]; then
    CONNECTOR_NAME="$CDC_CONNECTOR_NAME"
    if ! echo "$ALL_NAMES" | grep -qx "$CONNECTOR_NAME"; then
        fail "CDC_CONNECTOR_NAME='$CONNECTOR_NAME' isn't registered on this worker."
        fail "Available connectors:"
        echo "$ALL_NAMES" | sed 's/^/    /'
        exit 1
    fi
    ok "Using explicitly requested connector: $CONNECTOR_NAME"
else
    PG_CONNECTORS=""
    while IFS= read -r NAME; do
        [[ -z "$NAME" ]] && continue
        CLASS=$(curl -sf "http://$CONNECT_HOST:$CONNECT_PORT/connectors/$NAME/config" 2>/dev/null \
            | python3 -c "import json,sys; print(json.load(sys.stdin).get('connector.class',''))" 2>/dev/null)
        if [[ "$CLASS" == *"PostgresConnector"* ]]; then
            PG_CONNECTORS="$PG_CONNECTORS $NAME"
        fi
    done <<< "$ALL_NAMES"
    PG_CONNECTORS=$(echo "$PG_CONNECTORS" | xargs -n1 2>/dev/null | sort -u)
    COUNT=$(echo "$PG_CONNECTORS" | grep -c . || true)

    if [[ "$COUNT" -eq 0 ]]; then
        fail "No Postgres/Debezium connectors found among: $ALL_NAMES"
        exit 1
    elif [[ "$COUNT" -eq 1 ]]; then
        CONNECTOR_NAME=$(echo "$PG_CONNECTORS" | head -1)
        ok "Auto-detected connector: $CONNECTOR_NAME"
    else
        fail "Multiple Postgres connectors found — can't auto-pick one:"
        echo "$PG_CONNECTORS" | sed 's/^/    /'
        fail "FIX: re-run with CDC_CONNECTOR_NAME=<one of the above> ./scripts/debug-cdc.sh"
        exit 1
    fi
fi

step "Stage 3: reading $CONNECTOR_NAME's actual configuration"
CFG_JSON=$(curl -sf "http://$CONNECT_HOST:$CONNECT_PORT/connectors/$CONNECTOR_NAME/config" 2>/dev/null)
if [[ -z "$CFG_JSON" ]]; then
    fail "Could not fetch config for $CONNECTOR_NAME"
    exit 1
fi
EPAS_HOST=$(echo "$CFG_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin).get('database.hostname','localhost'))")
EPAS_PORT=$(echo "$CFG_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin).get('database.port','5432'))")
EPAS_USER=$(echo "$CFG_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin).get('database.user','postgres'))")
CDC_DB_NAME=$(echo "$CFG_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin).get('database.dbname',''))")
SLOT_NAME=$(echo "$CFG_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin).get('slot.name',''))")
TABLE_LIST=$(echo "$CFG_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin).get('table.include.list',''))")
[[ -n "$EPAS_USER_OVERRIDE" ]] && EPAS_USER="$EPAS_USER_OVERRIDE"

ok "database.hostname:port = $EPAS_HOST:$EPAS_PORT"
ok "database.user          = $EPAS_USER"
ok "database.dbname        = $CDC_DB_NAME"
ok "slot.name              = $SLOT_NAME"
ok "table.include.list     = $TABLE_LIST"
echo ""
echo "  (Stage 4 onward uses THESE values, read from Kafka Connect itself —"
echo "   not hardcoded guesses. If they look wrong, fix the connector config, not this script.)"

step "Stage 4: EPAS — reachable, logical replication enabled"
if psql -h "$EPAS_HOST" -p "$EPAS_PORT" -U "$EPAS_USER" -d postgres -c "SELECT 1;" >/dev/null 2>&1; then
    ok "EPAS reachable at $EPAS_HOST:$EPAS_PORT"
else
    fail "Cannot connect to EPAS at $EPAS_HOST:$EPAS_PORT as $EPAS_USER"
    fail "FIX: confirm EPAS is running, and your password (via ~/.pgpass or -W)"
    [[ $STOP_AT_FIRST_FAILURE -eq 1 ]] && exit 1
fi

WAL_LEVEL=$(psql -h "$EPAS_HOST" -p "$EPAS_PORT" -U "$EPAS_USER" -d postgres -tAc "SHOW wal_level;" 2>/dev/null)
if [[ "$WAL_LEVEL" == "logical" ]]; then
    ok "wal_level = logical"
else
    fail "wal_level = '$WAL_LEVEL' (needs to be 'logical')"
    fail "FIX: ALTER SYSTEM SET wal_level = 'logical'; then RESTART EPAS (not just reload)"
    [[ $STOP_AT_FIRST_FAILURE -eq 1 ]] && exit 1
fi

step "Stage 5: $CDC_DB_NAME database and source tables"
if psql -h "$EPAS_HOST" -p "$EPAS_PORT" -U "$EPAS_USER" -d "$CDC_DB_NAME" -c "SELECT 1;" >/dev/null 2>&1; then
    ok "$CDC_DB_NAME database exists"
else
    fail "$CDC_DB_NAME database doesn't exist or isn't reachable"
    [[ $STOP_AT_FIRST_FAILURE -eq 1 ]] && exit 1
fi

IFS=',' read -ra TABLES <<< "$TABLE_LIST"
for QUALIFIED in "${TABLES[@]}"; do
    CNT=$(psql -h "$EPAS_HOST" -p "$EPAS_PORT" -U "$EPAS_USER" -d "$CDC_DB_NAME" -tAc "SELECT COUNT(*) FROM $QUALIFIED;" 2>/dev/null)
    if [[ -n "$CNT" ]]; then
        ok "$QUALIFIED exists ($CNT rows)"
    else
        fail "$QUALIFIED missing or not queryable"
        [[ $STOP_AT_FIRST_FAILURE -eq 1 ]] && exit 1
    fi
done

step "Stage 6: $CONNECTOR_NAME — running state"
STATUS_JSON=$(curl -sf "http://$CONNECT_HOST:$CONNECT_PORT/connectors/$CONNECTOR_NAME/status" 2>/dev/null)
CONN_STATE=$(echo "$STATUS_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin).get('connector',{}).get('state','?'))" 2>/dev/null)
TASK_STATE=$(echo "$STATUS_JSON" | python3 -c "import json,sys; d=json.load(sys.stdin); t=d.get('tasks',[]); print(t[0].get('state','?') if t else 'NO_TASKS')" 2>/dev/null)
if [[ "$CONN_STATE" == "RUNNING" && "$TASK_STATE" == "RUNNING" ]]; then
    ok "Connector RUNNING, task RUNNING"
else
    fail "Connector state=$CONN_STATE, task state=$TASK_STATE"
    FULL_TRACE=$(echo "$STATUS_JSON" | python3 -c "
import json, sys
d = json.load(sys.stdin)
for t in d.get('tasks', []):
    if t.get('trace'):
        print(t['trace'])
" 2>/dev/null)
    if [[ -n "$FULL_TRACE" ]]; then
        echo "  --- task trace (truncated to 800 chars for display) ---"
        echo "${FULL_TRACE:0:800}" | sed 's/^/  /'
    fi
    if echo "$FULL_TRACE" | grep -qi "no longer available on the server"; then
        fail "Stale-offset/slot mismatch."
        fail "NOTE: DELETE /connectors/{name}/offsets is unreliable in Kafka Connect"
        fail "standalone mode (confirmed — it can silently not clear anything). Don't"
        fail "rely on it. Go straight to a new connector name instead:"
        echo "    curl -s http://$CONNECT_HOST:$CONNECT_PORT/connectors/$CONNECTOR_NAME/config > /tmp/cdc-config.json"
        echo "    curl -X DELETE http://$CONNECT_HOST:$CONNECT_PORT/connectors/$CONNECTOR_NAME"
        echo "    psql -h $EPAS_HOST -p $EPAS_PORT -U $EPAS_USER -d $CDC_DB_NAME -c \"SELECT pg_drop_replication_slot('$SLOT_NAME');\""
        echo "    python3 -c \"import json; d=json.load(open('/tmp/cdc-config.json')); d['name']='${CONNECTOR_NAME}-v2'; json.dump({'name':d['name'],'config':d}, open('/tmp/cdc-v2.json','w'))\""
        echo "    curl -X POST http://$CONNECT_HOST:$CONNECT_PORT/connectors -H 'Content-Type: application/json' -d @/tmp/cdc-v2.json"
    else
        fail "FIX: curl -X DELETE http://$CONNECT_HOST:$CONNECT_PORT/connectors/$CONNECTOR_NAME, then re-register"
    fi
    [[ $STOP_AT_FIRST_FAILURE -eq 1 ]] && exit 1
fi

step "Stage 7: replication slot ($SLOT_NAME)"
SLOT_INFO=$(psql -h "$EPAS_HOST" -p "$EPAS_PORT" -U "$EPAS_USER" -d "$CDC_DB_NAME" -tAc "
SELECT active || '|' || pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
FROM pg_replication_slots WHERE slot_name = '$SLOT_NAME';" 2>/dev/null)
if [[ -n "$SLOT_INFO" ]]; then
    ACTIVE=$(echo "$SLOT_INFO" | cut -d'|' -f1)
    LAG=$(echo "$SLOT_INFO" | cut -d'|' -f2)
    if [[ "$ACTIVE" == "t" ]]; then
        ok "Slot active, WAL lag: $LAG"
    else
        warn "Slot exists but active=$ACTIVE (may be normal if the connector just (re)started)"
    fi
else
    fail "Slot '$SLOT_NAME' doesn't exist"
    [[ $STOP_AT_FIRST_FAILURE -eq 1 ]] && exit 1
fi

step "Stage 8: Kafka topics"
TOPIC_PREFIX=$(echo "$CFG_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin).get('topic.prefix',''))")
if [[ -x "$KAFKA_BIN/kafka-topics.sh" ]]; then
    TOPICS=$("$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$KAFKA_BROKER" --list 2>/dev/null | grep "^$TOPIC_PREFIX")
    if [[ -n "$TOPICS" ]]; then
        ok "Found topics with prefix '$TOPIC_PREFIX':"
        echo "$TOPICS" | sed 's/^/    /'
        for T in $TOPICS; do
            OFFSETS=$("$KAFKA_BIN/kafka-run-class.sh" kafka.tools.GetOffsetShell \
                --broker-list "$KAFKA_BROKER" --topic "$T" --time -1 2>/dev/null \
                | awk -F: '{sum+=$3} END{print sum+0}')
            echo "    $T — approx $OFFSETS messages total"
        done
    else
        fail "No topics found with prefix '$TOPIC_PREFIX' on $KAFKA_BROKER"
        [[ $STOP_AT_FIRST_FAILURE -eq 1 ]] && exit 1
    fi
else
    warn "kafka-topics.sh not found at $KAFKA_BIN — set KAFKA_BIN_DIR if it's elsewhere"
fi

# ── Stage 9: FlowServer daemon + jobs ─────────────────────────────────────────
step "Stage 9: FlowServer — daemon and jobs consuming these topics"
FLOWSERVER_HOST="${FLOWSERVER_HOST:-localhost}"
FLOWSERVER_PORT="${FLOWSERVER_PORT:-6060}"
if nc -z "$FLOWSERVER_HOST" "$FLOWSERVER_PORT" 2>/dev/null; then
    ok "FlowServer daemon up on $FLOWSERVER_HOST:$FLOWSERVER_PORT"
    FLOW_LIST=$(flowcli --host "$FLOWSERVER_HOST" --port "$FLOWSERVER_PORT" list 2>/dev/null)
    if [[ -z "$FLOW_LIST" ]]; then
        warn "flowcli list returned nothing — can't confirm job status"
    else
        echo "$FLOW_LIST" | sed 's/^/  /'
        if echo "$FLOW_LIST" | grep -qiE "RUNNING"; then
            ok "At least one job shows RUNNING"
        else
            fail "No job shows RUNNING — this is very likely why nothing is reaching WHPG"
            fail "FIX: flowcli stop <job>; flowcli start <job> for each job consuming tu.oltp.* topics"
            fail "     (a Debezium offset reset, like the one just done, produces a fresh snapshot —"
            fail "      a FlowServer job already running from BEFORE that reset can be left stuck)"
        fi
    fi
else
    fail "FlowServer daemon not reachable on $FLOWSERVER_HOST:$FLOWSERVER_PORT"
    fail "FIX: nohup flowserver -c flowserver/flow_server.json > flow.out 2>&1 &"
fi

# ── Stage 10: is data actually landing in WHPG? ───────────────────────────────
step "Stage 10: WHPG — is data actually landing (the end-to-end check)"
WHPG_HOST="${WHPG_HOST:-localhost}"
WHPG_PORT="${WHPG_PORT:-5432}"
WHPG_USER="${WHPG_USER:-gpadmin}"
WHPG_DB="${WHPG_DB:-tu}"
WHPG_SCHEMA="${WHPG_SCHEMA:-tu_bureau_demo}"
if psql -h "$WHPG_HOST" -p "$WHPG_PORT" -U "$WHPG_USER" -d "$WHPG_DB" -c "SELECT 1;" >/dev/null 2>&1; then
    for T in credit_accounts bureau_score_events; do
        LATEST=$(psql -h "$WHPG_HOST" -p "$WHPG_PORT" -U "$WHPG_USER" -d "$WHPG_DB" -tAc \
            "SET search_path=$WHPG_SCHEMA,public; SELECT MAX(ingested_ts) FROM $T;" 2>/dev/null)
        NOW=$(date -u +"%Y-%m-%d %H:%M:%S")
        echo "  $T — latest ingested_ts in WHPG: $LATEST  (now: $NOW UTC)"
    done
    warn "If 'latest' above is old (hours/days) despite fresh EPAS activity, the CDC data"
    warn "is genuinely not reaching WHPG — re-check Stage 9's FlowServer job status."
else
    warn "Could not connect to WHPG at $WHPG_HOST:$WHPG_PORT/$WHPG_DB as $WHPG_USER"
    warn "Set WHPG_HOST/WHPG_PORT/WHPG_USER/WHPG_DB/WHPG_SCHEMA if these don't match your setup"
fi

echo ""
echo -e "${BOLD}=============================================="
echo -e " Debug complete"
echo -e "==============================================${RESET}"

