#!/bin/bash
# =============================================================================
# WHPG Real Streaming Demo — Start Everything
# =============================================================================
# Starts in nohup: FlowServer, FlowServer jobs, generators, dashboard API
# Everything survives SSH disconnection.
#
# Usage: ./scripts/start-demo.sh [--ecom-rate N] [--iot-rate N] [--region india|egypt]
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
PID_DIR="/tmp/flowserver-demo"
LOG_DIR="$PID_DIR/logs"
FS_BIN="/usr/edb/whpg7/bin/flowserver"
FS_CFG="$REPO_DIR/configs/flow_server.json"
KAFKA_BIN="/opt/kafka/bin"
KAFKA_BROKER="localhost:9092"

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

mkdir -p "$PID_DIR" "$LOG_DIR"
cd "$REPO_DIR"

stop_pid() {
    local PF="$PID_DIR/$1.pid"
    if [[ -f "$PF" ]]; then
        local PID; PID=$(cat "$PF")
        kill "$PID" 2>/dev/null || true
        sleep 0.5; kill -9 "$PID" 2>/dev/null || true
        rm -f "$PF"
    fi
}

echo ""
echo "=============================================="
echo " WHPG Real Streaming Demo — Starting"
echo "=============================================="
echo " E-Com : ${ECOM_RATE}/s (region: ${REGION})   IoT : ${IOT_RATE}/s"
echo "=============================================="
echo ""

# ── Stop previous instances ───────────────────────────────────────────────────
echo "► Clearing previous instances..."
for C in flowserver order-generator iot-generator dashboard-api; do stop_pid "$C"; done

# Kill ONLY the flowserver on port 6060 — leave port 6061 (CDC demo) untouched
FS_PID_6060=$(lsof -ti TCP:6060 2>/dev/null | head -1)
if [[ -n "$FS_PID_6060" ]]; then
    kill "$FS_PID_6060" 2>/dev/null && echo "  ✓ flowserver port 6060 stopped (PID $FS_PID_6060)" || true
fi
sleep 1
echo "  ✓ Cleared"
echo ""

# ── FlowServer ────────────────────────────────────────────────────────────────
echo "► Starting FlowServer..."
if [[ ! -x "$FS_BIN" ]]; then
    echo "  ✗ Not found: $FS_BIN"; exit 1
fi
nohup "$FS_BIN" -c "$FS_CFG" > "$LOG_DIR/flowserver.log" 2>&1 &
FS_PID=$!
echo "$FS_PID" > "$PID_DIR/flowserver.pid"
echo "  ✓ FlowServer PID=$FS_PID — waiting for port 6060..."
for i in $(seq 1 15); do
    nc -z localhost 6060 2>/dev/null && break
    kill -0 "$FS_PID" 2>/dev/null || { echo "  ✗ Crashed — check $LOG_DIR/flowserver.log"; exit 1; }
    sleep 1
done
nc -z localhost 6060 2>/dev/null || { echo "  ✗ Port 6060 never opened"; exit 1; }
echo "  ✓ FlowServer ready (${i}s)"
echo ""

# ── Kafka broker + topics ─────────────────────────────────────────────────────
# Kafka's own log dir (kraft-combined-logs) lives under /tmp and does NOT
# survive a reboot, so topics must be (re)created here every time — we can't
# rely on generator auto-create, because that races with the FlowServer job
# submission below and causes UNKNOWN_TOPIC_OR_PARTITION.
#
# Kafka's CLI tools live under $KAFKA_BIN and must run as the 'kafka' user
# (same as stop-demo.sh's topic deletion) — gpadmin can't run them directly.
echo "► Ensuring Kafka broker + topics are ready..."
for i in $(seq 1 15); do
    nc -z localhost 9092 2>/dev/null && break
    [[ $i -eq 15 ]] && { echo "  ✗ Kafka broker on 9092 never came up"; exit 1; }
    sleep 1
done
echo "  ✓ Kafka broker reachable"

INNER="/tmp/kafka-start-$$.sh"
cat > "$INNER" << INNEREOF
#!/bin/bash
KAFKA_BIN="$KAFKA_BIN"
KAFKA_BROKER="$KAFKA_BROKER"
[[ -x "\$KAFKA_BIN/kafka-topics" ]] && KT="\$KAFKA_BIN/kafka-topics" || KT="\$KAFKA_BIN/kafka-topics.sh"
for TOPIC in ecommerce-orders iot-sensors-csv; do
    if "\$KT" --list --bootstrap-server "\$KAFKA_BROKER" 2>/dev/null | grep -q "^\${TOPIC}\$"; then
        echo "  ~ topic '\$TOPIC' already exists"
    else
        if "\$KT" --create --topic "\$TOPIC" --partitions 6 --replication-factor 1 \
            --bootstrap-server "\$KAFKA_BROKER" 2>&1 | tee /tmp/kafka-create-\$TOPIC.log | grep -qi "created\|already exists"; then
            echo "  ✓ topic '\$TOPIC' created"
        else
            echo "  ✗ failed to create topic '\$TOPIC' — see /tmp/kafka-create-\$TOPIC.log"
        fi
    fi
done
INNEREOF
chmod 755 "$INNER"
sudo su - kafka -c "bash $INNER"
rm -f "$INNER"
echo ""

# ── Reset stale FlowServer job history ────────────────────────────────────────
# FlowServer's job history/offsets are stored in Postgres and DO survive a
# reboot even though the Kafka log dir above does not. A leftover history
# offset pointing past the (now-empty/recreated) topic's high watermark
# causes "history offsets are out of bounds" and the job aborts. Since this
# script always starts a fresh demo run, wipe any stale history tables first.
echo "► Resetting stale FlowServer job history..."
PGPASSWORD="${PGPASSWORD:-}" psql -h localhost -p 5432 -U gpadmin -d streaming_demo -v ON_ERROR_STOP=1 <<'SQL' >/dev/null 2>&1 \
    && echo "  ✓ stale history tables cleared" \
    || echo "  ~ no stale history tables found (or psql unavailable — skipping)"
DO $$
DECLARE r RECORD;
BEGIN
    FOR r IN SELECT tablename FROM pg_tables
             WHERE schemaname = 'public' AND tablename LIKE 'whpgfs_hs_%'
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS public.%I', r.tablename);
    END LOOP;
END $$;
SQL
echo ""

# ── FlowServer jobs ───────────────────────────────────────────────────────────
echo "► FlowServer Jobs..."
for YAML in jobs/ecommerce-orders.yaml jobs/iot-sensors-csv.yaml; do
    NAME=$(basename "$YAML" .yaml)

    # Remove stale job registration from any previous FlowServer instance
    echo "  $ flowcli --host localhost --port 6060 remove $NAME"
    flowcli --host localhost --port 6060 remove "$NAME" 2>/dev/null || true

    # Submit fresh — use absolute path so flowcli finds it regardless of cwd
    ABS_YAML="$REPO_DIR/$YAML"
    echo "  $ flowcli --host localhost --port 6060 submit $ABS_YAML"
    OUT=$(flowcli --host localhost --port 6060 submit "$ABS_YAML" 2>&1)
    echo "    → $OUT"
    echo "$OUT" | grep -qi "success\|submitted"         && echo "  ✓ '$NAME' submitted"         || echo "  ✗ '$NAME' submit failed"

    # Start — now safe: topic exists (created above) and history is clean (reset above)
    echo "  $ flowcli --host localhost --port 6060 start --reset-to-earliest $NAME"
    OUT=$(flowcli --host localhost --port 6060 start --reset-to-earliest "$NAME" 2>&1)
    echo "    → $OUT"
    echo "$OUT" | grep -qi "success\|started"         && echo "  ✓ '$NAME' started"         || { echo "$OUT" | grep -qi "already\|running"             && echo "  ~ '$NAME' already running"             || echo "  ✗ '$NAME' failed to start"; }
done
echo ""

# ── Generators ────────────────────────────────────────────────────────────────
echo "► Starting Generators..."
ECOM_BIN="$REPO_DIR/generators/order-generator"
IOT_BIN="$REPO_DIR/generators/iot-generator"
if [[ ! -x "$ECOM_BIN" ]] || [[ ! -x "$IOT_BIN" ]]; then
    echo "  Generators not built — building now..."
    cd "$REPO_DIR/generators"
    go mod tidy
    go build -o order-generator order-generator.go && echo "  ✓ order-generator built"
    go build -o iot-generator   iot-generator.go   && echo "  ✓ iot-generator built"
    cd "$REPO_DIR"
fi
nohup "$ECOM_BIN" -rate "$ECOM_RATE" -max-messages "$ECOM_TOTAL" -region "$REGION" \
    > "$LOG_DIR/order-generator.log" 2>&1 &
echo $! > "$PID_DIR/order-generator.pid"
echo "  ✓ order-generator PID=$!  ${ECOM_RATE}/s  region=${REGION}"

nohup "$IOT_BIN" -rate "$IOT_RATE" -max-messages "$IOT_TOTAL" \
    > "$LOG_DIR/iot-generator.log" 2>&1 &
echo $! > "$PID_DIR/iot-generator.pid"
echo "  ✓ iot-generator   PID=$!  ${IOT_RATE}/s"
echo ""

# ── Dashboard API ─────────────────────────────────────────────────────────────
echo "► Starting Dashboard API..."
nohup python3 "$REPO_DIR/dashboards/api.py" --app-port 5055 \
    > "$LOG_DIR/dashboard-api.log" 2>&1 &
echo $! > "$PID_DIR/dashboard-api.pid"
echo "  ✓ Dashboard API PID=$!  port=5055"
echo ""

sleep 1
echo "=============================================="
echo " ✓ All services started (nohup — SSH-safe)"
echo "=============================================="
echo ""
echo " Dashboard  : http://localhost:5055/"
echo " FlowServer : http://localhost:6060/"
echo ""
echo " Stop : ./scripts/stop-demo.sh"
echo " Logs : tail -f $LOG_DIR/*.log"
echo ""

