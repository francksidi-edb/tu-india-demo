#!/bin/bash
# =============================================================================
# WHPG Real Streaming Demo — Stop Everything
# =============================================================================
# Usage: ./scripts/stop-demo.sh [--keep-topics]
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
PID_DIR="/tmp/flowserver-demo"
KAFKA_BIN="/opt/kafka/bin"
KAFKA_BROKER="localhost:9092"
KEEP_TOPICS=0

while [[ $# -gt 0 ]]; do
    case $1 in
        --keep-topics) KEEP_TOPICS=1; shift ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

stop_pid() {
    local NAME="$1"
    local PF="$PID_DIR/$NAME.pid"
    if [[ -f "$PF" ]]; then
        local PID; PID=$(cat "$PF")
        if kill -0 "$PID" 2>/dev/null; then
            kill "$PID" 2>/dev/null
            for i in 1 2 3; do kill -0 "$PID" 2>/dev/null || break; sleep 1; done
            kill -9 "$PID" 2>/dev/null || true
            echo "  ✓ $NAME stopped (PID $PID)"
        else
            echo "  ~ $NAME not running (stale PID)"
        fi
        rm -f "$PF"
    else
        echo "  ~ $NAME — no PID file"
    fi
}

echo ""
echo "=============================================="
echo " WHPG Real Streaming Demo — Stopping"
echo "=============================================="

# ── 1. Stop FlowServer jobs ───────────────────────────────────────────────────
echo ""
echo "► Stopping FlowServer Jobs..."
if nc -z localhost 6060 2>/dev/null; then
    for JOB in ecommerce-orders iot-sensors-csv; do
        OUT=$(flowcli --host localhost --port 6060 stop "$JOB" 2>&1)
        echo "$OUT" | grep -qi "success\|stopped" \
            && echo "  ✓ '$JOB' stopped" \
            || echo "  ~ '$JOB': $(echo "$OUT" | tail -1)"
    done
else
    echo "  ~ FlowServer not reachable — skipping job stop"
fi

# ── 2. Stop generators ────────────────────────────────────────────────────────
echo ""
echo "► Stopping Generators..."
stop_pid "order-generator"
stop_pid "iot-generator"

# ── 3. Stop Dashboard API ─────────────────────────────────────────────────────
echo ""
echo "► Stopping Dashboard API..."
stop_pid "dashboard-api"

# ── 4. Stop FlowServer ───────────────────────────────────────────────────────
echo ""
echo "► Stopping FlowServer..."
stop_pid "flowserver"
# Kill ONLY the flowserver bound to port 6060
FS_PID_6060=$(lsof -ti TCP:6060 2>/dev/null | head -1)
if [[ -n "$FS_PID_6060" ]]; then
    kill "$FS_PID_6060" 2>/dev/null \
        && echo "  ✓ FlowServer on port 6060 stopped (PID $FS_PID_6060)" \
        || true
else
    echo "  ~ No process found on port 6060"
fi

# ── 5. Delete Kafka topics ────────────────────────────────────────────────────
if [[ $KEEP_TOPICS -eq 0 ]]; then
    echo ""
    echo "► Deleting Kafka Topics (sudo su - kafka)..."
    INNER="/tmp/kafka-stop-$$.sh"
    cat > "$INNER" << INNEREOF
#!/bin/bash
KAFKA_BIN="$KAFKA_BIN"
KAFKA_BROKER="$KAFKA_BROKER"
[[ -x "\$KAFKA_BIN/kafka-topics" ]] && KT="\$KAFKA_BIN/kafka-topics" || KT="\$KAFKA_BIN/kafka-topics.sh"
for TOPIC in ecommerce-orders iot-sensors-csv; do
    if "\$KT" --list --bootstrap-server "\$KAFKA_BROKER" 2>/dev/null | grep -q "^\${TOPIC}$"; then
        "\$KT" --delete --topic "\$TOPIC" --bootstrap-server "\$KAFKA_BROKER" 2>/dev/null || true
        echo "  ✓ Topic '\$TOPIC' deleted"
    else
        echo "  ~ Topic '\$TOPIC' not found"
    fi
done
INNEREOF
    chmod 755 "$INNER"
    sudo su - kafka -c "bash $INNER"
    rm -f "$INNER"
fi

echo ""
echo "  ✓ All stopped"
echo "=============================================="
echo ""
echo " To restart:  ./scripts/start-demo.sh"
echo ""
