#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

PGHOST="localhost"
PGPORT="5432"
PGUSER="gpadmin"
PGDATABASE="streaming_demo"

# CDC demo settings (Postgres EPAS source -> Debezium -> Kafka -> FlowServer -> WarehousePG)
CDC_SRC_HOST="localhost"
CDC_SRC_PORT="5444"
CDC_SRC_USER="debezium_user"
CDC_SRC_DB="tu"
CDC_TGT_HOST="localhost"
CDC_TGT_PORT="5432"
CDC_TGT_USER="gpadmin"
CDC_TGT_DB="tu"
CDC_SLOT="debezium_tu_slot"
CDC_CONNECTOR_CFG="/home/kafka/configs/debezium-tu-connector.json"
CDC_CONNECT_PROPS="/opt/kafka/config/connect-standalone.properties"
CDC_PID_FILE="/tmp/connect.pid"
CDC_LOG="/home/kafka/connect.log"
CDC_TOPICS=(tu.oltp.credit_accounts tu.oltp.bureau_score_events)

# ClickHouse ecommerce mirror (real-time dual-consumer of the ecommerce-orders topic)
CH_HOST="${CH_HOST:-localhost}"
CH_NATIVE_PORT="${CH_NATIVE_PORT:-9094}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-}"
CH_DB="${CH_DB:-default}"
CH_SETUP_SQL="$REPO_DIR/sql/01_clickhouse_setup.sql"
CH_TARGET_TABLE="default.ecommerce_orders"
CH_KAFKA_TABLE="default.kafka_ecommerce_orders"

ECOM_RATE=10000
IOT_RATE=10000
ECOM_TOTAL=0
IOT_TOTAL=0
REGION="india"

while [[ $# -gt 0 ]]; do
    case $1 in
        --ecom-rate)  ECOM_RATE="$2";  shift 2 ;;
        --iot-rate)   IOT_RATE="$2";   shift 2 ;;
        --ecom-total) ECOM_TOTAL="$2"; shift 2 ;;
        --iot-total)  IOT_TOTAL="$2";  shift 2 ;;
        --region)     REGION="$2";     shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

REGION="$(echo "$REGION" | tr '[:upper:]' '[:lower:]')"
if [[ "$REGION" != "india" && "$REGION" != "egypt" ]]; then
    echo "Unknown --region '$REGION' — valid options: india, egypt"
    exit 1
fi

# Helper: run clickhouse-client with common connection args, password only if set
ch() {
    local args=(--host "$CH_HOST" --port "$CH_NATIVE_PORT" --user "$CH_USER" --database "$CH_DB")
    [[ -n "$CH_PASSWORD" ]] && args+=(--password "$CH_PASSWORD")
    clickhouse-client "${args[@]}" "$@"
}

echo ""
echo "=============================================="
echo " WHPG Real Streaming Demo — Full Reset"
echo "=============================================="
echo " Region : $REGION"
echo " This will:"
echo "   1. Stop FlowServer, jobs, generators, dashboard API"
echo "   2. DROP + recreate ecommerce_orders and iot_sensor_readings"
echo "   3. Reset ClickHouse's ecommerce-orders mirror"
echo "   4. Reset the TU CDC pipeline (Postgres -> Debezium -> Kafka -> WarehousePG)"
echo "   5. Start everything fresh"
echo "   6. Health-check all services"
echo "=============================================="
echo ""

# ── 1/6 Stop everything (also deletes Kafka topics — no --keep-topics) ───────
echo "► Step 1/6 — Stopping everything..."
"$SCRIPT_DIR/stop-demo.sh"
echo ""

# ── 2/6 Drop + recreate tables ────────────────────────────────────────────────
echo "► Step 2/6 — Dropping & recreating tables..."
SQL_FILE="$REPO_DIR/sql/02_create_tables.sql"
if [[ ! -f "$SQL_FILE" ]]; then
    echo "  ✗ Not found: $SQL_FILE"
    exit 1
fi
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -f "$SQL_FILE"
echo "  ✓ Tables dropped and recreated (ecommerce_orders, iot_sensor_readings)"
echo ""

# ── 3/6 Reset ClickHouse's ecommerce mirror ───────────────────────────────────
echo "► Step 3/6 — Resetting ClickHouse ecommerce mirror..."

if command -v clickhouse-client >/dev/null 2>&1; then
    # Ensure schema exists (idempotent — the setup SQL uses CREATE ... IF NOT EXISTS),
    # so this is safe to re-run even on a brand new environment.
    if [[ -f "$CH_SETUP_SQL" ]]; then
        ch --multiquery < "$CH_SETUP_SQL"
        echo "  ✓ ClickHouse schema ensured (Kafka table, target table, materialized view)"
    else
        echo "  ~ $CH_SETUP_SQL not found — skipping schema ensure (assuming already created)"
    fi

    # Only truncate the target MergeTree table — TRUNCATE doesn't apply to the
    # Kafka engine table (it holds no data of its own).
    if ch --query "EXISTS TABLE $CH_TARGET_TABLE" 2>/dev/null | grep -q '^1'; then
        ch --query "TRUNCATE TABLE $CH_TARGET_TABLE"
        echo "  ✓ $CH_TARGET_TABLE truncated"
    else
        echo "  ~ $CH_TARGET_TABLE does not exist yet — nothing to truncate"
    fi
else
    echo "  ~ clickhouse-client not found in PATH — skipping ClickHouse reset"
fi
echo ""

# ── 4/6 Reset the TU CDC pipeline ─────────────────────────────────────────────
echo "► Step 4/6 — Resetting TU CDC pipeline..."

echo "  Stopping Debezium connector..."
sudo su - kafka -c "
    if [[ -f '$CDC_PID_FILE' ]]; then
        PID=\$(cat '$CDC_PID_FILE')
        kill \"\$PID\" 2>/dev/null || true
        for i in 1 2 3; do kill -0 \"\$PID\" 2>/dev/null || break; sleep 1; done
        kill -9 \"\$PID\" 2>/dev/null || true
        rm -f '$CDC_PID_FILE'
        echo '  ✓ connect-standalone stopped'
    else
        echo '  ~ no connect.pid found — assuming already stopped'
    fi
"

echo "  Truncating source tables (EPAS $CDC_SRC_DB)..."
PGPASSWORD="${CDC_SRC_PGPASSWORD:-admin}" psql -h "$CDC_SRC_HOST" -p "$CDC_SRC_PORT" \
    -U "$CDC_SRC_USER" -d "$CDC_SRC_DB" -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE TABLE oltp.credit_accounts;
TRUNCATE TABLE oltp.bureau_score_events;
SQL
echo "  ✓ source tables truncated"

echo "  Truncating target tables (WarehousePG $CDC_TGT_DB)..."
PGPASSWORD="${CDC_TGT_PGPASSWORD:-admin}" psql -h "$CDC_TGT_HOST" -p "$CDC_TGT_PORT" \
    -U "$CDC_TGT_USER" -d "$CDC_TGT_DB" -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE TABLE tu_bureau_demo.credit_accounts;
TRUNCATE TABLE tu_bureau_demo.bureau_score_events;
TRUNCATE TABLE tu_bureau_demo.lender_feed_landing;
SQL
echo "  ✓ target tables truncated"

echo "  Ensuring publication exists..."
PGPASSWORD="${CDC_SRC_PGPASSWORD:-admin}" psql -h "$CDC_SRC_HOST" -p "$CDC_SRC_PORT" \
    -U "$CDC_SRC_USER" -d "$CDC_SRC_DB" -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'debezium_tu_publication') THEN
        CREATE PUBLICATION debezium_tu_publication
            FOR TABLE oltp.credit_accounts, oltp.bureau_score_events;
    END IF;
END
$$;
SQL
echo "  ✓ publication present"

echo "  Dropping stale replication slot..."
PGPASSWORD="${CDC_SRC_PGPASSWORD:-admin}" psql -h "$CDC_SRC_HOST" -p "$CDC_SRC_PORT" \
    -U "$CDC_SRC_USER" -d "$CDC_SRC_DB" -v ON_ERROR_STOP=1 <<SQL
DO \$\$
DECLARE
    v_pid int;
BEGIN
    SELECT active_pid INTO v_pid
    FROM pg_replication_slots
    WHERE slot_name = '$CDC_SLOT' AND active;

    IF v_pid IS NOT NULL THEN
        PERFORM pg_terminate_backend(v_pid);
        PERFORM pg_sleep(1);
    END IF;
END
\$\$;

SELECT pg_drop_replication_slot('$CDC_SLOT')
WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '$CDC_SLOT');
SQL
echo "  ✓ slot dropped (or was already gone)"

echo "  Clearing connector offsets file..."
sudo su - kafka -c "
    OFFSETS_FILE=\$(grep -oP '^offset.storage.file.filename=\K.*' '$CDC_CONNECT_PROPS' || echo '')
    if [[ -n \"\$OFFSETS_FILE\" && -f \"\$OFFSETS_FILE\" ]]; then
        mv \"\$OFFSETS_FILE\" \"\${OFFSETS_FILE}.bak.\$(date +%s)\"
        echo \"  ✓ offsets file cleared (\$OFFSETS_FILE)\"
    else
        echo '  ~ no offsets file found — skipping'
    fi
"

echo "  Deleting + recreating TU Kafka topics..."
sudo su - kafka -c "
    KT=/opt/kafka/bin/kafka-topics.sh
    for TOPIC in ${CDC_TOPICS[*]}; do
        \"\$KT\" --bootstrap-server localhost:9092 --delete --topic \"\$TOPIC\" 2>/dev/null || true
    done
    sleep 2
    for TOPIC in ${CDC_TOPICS[*]}; do
        \"\$KT\" --bootstrap-server localhost:9092 --create --topic \"\$TOPIC\" \
            --partitions 3 --replication-factor 1 2>/dev/null \
            && echo \"  ✓ recreated \$TOPIC\" \
            || echo \"  ~ \$TOPIC already exists\"
    done
"

echo "  Restarting Debezium connector..."
sudo su - kafka -c "
    nohup /opt/kafka/bin/connect-standalone.sh \
        '$CDC_CONNECT_PROPS' \
        '$CDC_CONNECTOR_CFG' \
        > '$CDC_LOG' 2>&1 &
    echo \$! > '$CDC_PID_FILE'
    echo \"  ✓ connect-standalone started (PID \$!)\"
"

echo "  Waiting to confirm connect-standalone stays up..."
sleep 8
CDC_PID=$(sudo su - kafka -c "cat '$CDC_PID_FILE' 2>/dev/null" || echo "")
if [[ -n "$CDC_PID" ]] && sudo su - kafka -c "kill -0 '$CDC_PID' 2>/dev/null"; then
    echo "  ✓ connect-standalone still running after 8s (PID $CDC_PID)"
else
    echo "  ✗ connect-standalone died immediately — check $CDC_LOG"
    echo "    last 30 lines:"
    sudo su - kafka -c "tail -30 '$CDC_LOG'" || true
fi

echo "  ✓ TU CDC pipeline reset"
echo ""

# ── 5/6 Start everything fresh ────────────────────────────────────────────────
echo "► Step 5/6 — Starting everything fresh (region: $REGION)..."
"$SCRIPT_DIR/start-demo.sh" \
    --ecom-rate "$ECOM_RATE" --iot-rate "$IOT_RATE" \
    --ecom-total "$ECOM_TOTAL" --iot-total "$IOT_TOTAL" \
    --region "$REGION"
echo ""

# start-demo.sh just deleted+recreated the ecommerce-orders topic. ClickHouse's
# Kafka engine table can get stuck on stale topic/partition metadata when the
# topic it's consuming gets recreated out from under it — detach/reattach
# forces it to re-resolve metadata and resume consuming cleanly.
if command -v clickhouse-client >/dev/null 2>&1; then
    echo "  Reattaching ClickHouse Kafka consumer to pick up the recreated topic..."
    if ch --query "EXISTS TABLE $CH_KAFKA_TABLE" 2>/dev/null | grep -q '^1'; then
        ch --query "DETACH TABLE $CH_KAFKA_TABLE"
        sleep 1
        ch --query "ATTACH TABLE $CH_KAFKA_TABLE"
        echo "  ✓ $CH_KAFKA_TABLE reattached"
    else
        echo "  ~ $CH_KAFKA_TABLE does not exist — nothing to reattach"
    fi
    echo ""
fi

# ── 6/6 Health check — verify everything that should be up, is up ────────────
echo "► Step 6/6 — Health check..."
HEALTH_OK=1

check_port() {
    local NAME="$1" PORT="$2"
    if nc -z localhost "$PORT" 2>/dev/null; then
        echo "  ✓ $NAME reachable (port $PORT)"
    else
        echo "  ✗ $NAME NOT reachable (port $PORT)"
        HEALTH_OK=0
    fi
}

check_pidfile() {
    local NAME="$1" PF="$2"
    if [[ -f "$PF" ]]; then
        local PID; PID=$(cat "$PF")
        if kill -0 "$PID" 2>/dev/null; then
            echo "  ✓ $NAME running (PID $PID)"
        else
            echo "  ✗ $NAME NOT running (stale PID file $PF)"
            HEALTH_OK=0
        fi
    else
        echo "  ✗ $NAME — no PID file ($PF)"
        HEALTH_OK=0
    fi
}

echo "  -- Kafka broker --"
check_port "Kafka broker" 9092

echo "  -- Debezium CDC connector --"
CDC_PID_CHECK=$(sudo su - kafka -c "cat '$CDC_PID_FILE' 2>/dev/null" || echo "")
if [[ -n "$CDC_PID_CHECK" ]] && sudo su - kafka -c "kill -0 '$CDC_PID_CHECK' 2>/dev/null"; then
    echo "  ✓ connect-standalone running (PID $CDC_PID_CHECK)"
else
    echo "  ✗ connect-standalone NOT running"
    HEALTH_OK=0
fi

SLOT_STATUS=$(PGPASSWORD="${CDC_SRC_PGPASSWORD:-admin}" psql -h "$CDC_SRC_HOST" -p "$CDC_SRC_PORT" \
    -U "$CDC_SRC_USER" -d "$CDC_SRC_DB" -tA -v ON_ERROR_STOP=1 -c \
    "SELECT active FROM pg_replication_slots WHERE slot_name = '$CDC_SLOT';" 2>/dev/null || echo "")
if [[ "$SLOT_STATUS" == "t" ]]; then
    echo "  ✓ replication slot '$CDC_SLOT' active"
elif [[ "$SLOT_STATUS" == "f" ]]; then
    echo "  ✗ replication slot '$CDC_SLOT' exists but NOT active"
    HEALTH_OK=0
else
    echo "  ✗ replication slot '$CDC_SLOT' not found"
    HEALTH_OK=0
fi

echo "  -- FlowServer + generic demo jobs --"
check_port "FlowServer" 6060
check_pidfile "order-generator" "/tmp/flowserver-demo/order-generator.pid"
check_pidfile "iot-generator" "/tmp/flowserver-demo/iot-generator.pid"
check_pidfile "dashboard-api" "/tmp/flowserver-demo/dashboard-api.pid"

echo "  -- ClickHouse ecommerce mirror --"
if command -v clickhouse-client >/dev/null 2>&1; then
    check_port "ClickHouse (native)" "$CH_NATIVE_PORT"
    CH_COUNT=$(ch --query "SELECT count() FROM $CH_TARGET_TABLE" 2>/dev/null || echo "")
    if [[ -n "$CH_COUNT" ]]; then
        echo "  ✓ $CH_TARGET_TABLE reachable (rows: $CH_COUNT — should climb as the topic refills)"
    else
        echo "  ✗ could not query $CH_TARGET_TABLE"
        HEALTH_OK=0
    fi
else
    echo "  ~ clickhouse-client not found — skipping ClickHouse health check"
fi

echo ""
if [[ $HEALTH_OK -eq 1 ]]; then
    echo "  ✓ All checked services are up"
else
    echo "  ✗ One or more services failed health check — see ✗ items above"
fi
echo ""
echo "  Reminder: CDC FlowServer jobs (tu_load_credit_accounts,"
echo "  tu_load_bureau_score_events) are submitted from the UI —"
echo "  resubmit/restart them there now that topics were recreated."
echo ""

echo "=============================================="
if [[ $HEALTH_OK -eq 1 ]]; then
    echo " ✓ Full reset complete — demo running fresh"
else
    echo " ⚠ Reset finished but health check found issues — see above"
fi
echo "=============================================="
echo ""
echo " Dashboard : http://localhost:5055/"
echo ""

[[ $HEALTH_OK -eq 1 ]] || exit 1

