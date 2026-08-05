#!/bin/bash
# =============================================================================
# WHPG Real Streaming Demo — Status Check
# =============================================================================

export PATH="/opt/kafka/bin:$PATH"
PID_DIR="/tmp/flowserver-demo"
LOG_DIR="$PID_DIR/logs"

# ── colours ───────────────────────────────────────────────────────────────────
GREEN="\033[0;32m"
RED="\033[0;31m"
AMBER="\033[0;33m"
RESET="\033[0m"
BOLD="\033[1m"

ok()   { echo -e "  ${GREEN}✓${RESET} $*"; }
fail() { echo -e "  ${RED}✗${RESET} $*"; }
warn() { echo -e "  ${AMBER}~${RESET} $*"; }

check_pid() {
    local NAME="$1"
    local PID_FILE="$PID_DIR/$NAME.pid"
    if [[ -f "$PID_FILE" ]]; then
        local PID
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            ok "$NAME  running  (PID $PID)"
            return 0
        else
            fail "$NAME  NOT running  (stale PID $PID)"
            return 1
        fi
    else
        fail "$NAME  NOT running  (no PID file)"
        return 1
    fi
}

check_port() {
    local PORT="$1"
    nc -z localhost "$PORT" 2>/dev/null && return 0 || return 1
}

echo ""
echo -e "${BOLD}=============================================="
echo -e " WHPG Real Streaming Demo — Status"
echo -e "==============================================${RESET}"
echo ""

# ── Process status ────────────────────────────────────────────────────────────
echo -e "${BOLD}Processes:${RESET}"
check_pid "flowserver"
check_pid "order-generator"
check_pid "iot-generator"
check_pid "dashboard-api"
echo ""

# ── Port status ───────────────────────────────────────────────────────────────
echo -e "${BOLD}Ports:${RESET}"
check_port 6060 && ok "FlowServer API  :6060" || fail "FlowServer API  :6060"
check_port 6070 && ok "Gpfdist         :6070" || warn "Gpfdist         :6070  (not yet active)"
check_port 9080 && ok "Prometheus      :9080" || warn "Prometheus      :9080  (not yet active)"
check_port 5055 && ok "Dashboard API   :5055" || fail "Dashboard API   :5055"
check_port 9092 && ok "Kafka           :9092" || fail "Kafka           :9092"
echo ""

# ── FlowServer job status ─────────────────────────────────────────────────────
echo -e "${BOLD}FlowServer Jobs:${RESET}"
if check_port 6060 >/dev/null 2>&1; then
    # flowcli list shows all jobs — parse each job name from output
    LIST=$(flowcli --host localhost --port 6060 list 2>/dev/null || echo "unavailable")
    if echo "$LIST" | grep -q "unavailable"; then
        warn "flowcli unavailable"
    else
        for JOB in ecommerce-orders iot-sensors-csv; do
            JOB_LINE=$(echo "$LIST" | grep "^$JOB ")
            if [[ -z "$JOB_LINE" ]]; then
                warn "$JOB  not submitted"
            elif echo "$JOB_LINE" | grep -qi "JOB_RUNNING\|running"; then
                ok "$JOB  running"
            elif echo "$JOB_LINE" | grep -qi "JOB_STOPPED\|stopped\|idle"; then
                warn "$JOB  stopped"
            else
                STATUS=$(echo "$JOB_LINE" | awk '{print $3}')
                warn "$JOB  $STATUS"
            fi
        done
    fi
else
    warn "FlowServer not reachable — cannot check job status"
fi
echo ""

# ── Kafka topics ──────────────────────────────────────────────────────────────
echo -e "${BOLD}Kafka Topics:${RESET}"
KAFKA_PROPS="$(dirname "$SCRIPT_DIR")/configs/kafka-client.properties"
kafka_status_cmd() {
    if [[ -f "$KAFKA_PROPS" ]]; then
        kafka-topics "$@" --command-config "$KAFKA_PROPS"
    else
        kafka-topics "$@"
    fi
}
if check_port 9092 >/dev/null 2>&1; then
    for TOPIC in ecommerce-orders iot-sensors-csv; do
        if kafka_status_cmd --list --bootstrap-server localhost:9092 2>/dev/null | grep -q "^${TOPIC}$"; then
            OFFSET=$(kafka-run-class kafka.tools.GetOffsetShell \
                --broker-list localhost:9092 --topic "$TOPIC" --time -1 \
                ${KAFKA_PROPS:+--consumer.config "$KAFKA_PROPS"} 2>/dev/null \
                | awk -F: '{sum+=$3} END{print sum}')
            ok "$TOPIC  exists  (~${OFFSET:-?} messages total)"
        else
            fail "$TOPIC  does NOT exist"
        fi
    done
else
    warn "Kafka not reachable — cannot check topics"
fi
echo ""

# ── Database ──────────────────────────────────────────────────────────────────
echo -e "${BOLD}Database:${RESET}"
if check_port 5432 >/dev/null 2>&1; then
    ECOM_COUNT=$(psql -h localhost -p 5432 -U gpadmin -d streaming_demo -tAc \
        "SELECT COUNT(*) FROM public.ecommerce_orders" 2>/dev/null || echo "error")
    IOT_COUNT=$(psql -h localhost -p 5432 -U gpadmin -d streaming_demo -tAc \
        "SELECT COUNT(*) FROM public.iot_sensor_readings" 2>/dev/null || echo "error")
    ECOM_RATE=$(psql -h localhost -p 5432 -U gpadmin -d streaming_demo -tAc \
        "SELECT COUNT(*) FROM public.ecommerce_orders WHERE timestamp > NOW()-INTERVAL '1 minute'" 2>/dev/null || echo "?")
    IOT_RATE=$(psql -h localhost -p 5432 -U gpadmin -d streaming_demo -tAc \
        "SELECT COUNT(*) FROM public.iot_sensor_readings WHERE timestamp > NOW()-INTERVAL '1 minute'" 2>/dev/null || echo "?")

    if [[ "$ECOM_COUNT" != "error" ]]; then
        ok "ecommerce_orders    $ECOM_COUNT rows  (${ECOM_RATE}/min last minute)"
        ok "iot_sensor_readings $IOT_COUNT rows  (${IOT_RATE}/min last minute)"
    else
        fail "Cannot query streaming_demo DB"
    fi
else
    fail "PostgreSQL not reachable on :5432"
fi
echo ""

# ── Recent log lines ──────────────────────────────────────────────────────────
echo -e "${BOLD}Recent Logs (last 3 lines each):${RESET}"
for LOG in flowserver order-generator iot-generator dashboard-api; do
    LOG_FILE="$LOG_DIR/$LOG.log"
    if [[ -f "$LOG_FILE" ]]; then
        echo "  ── $LOG ──"
        tail -3 "$LOG_FILE" | sed 's/^/    /'
    fi
done
echo ""

echo -e "${BOLD}Commands:${RESET}"
echo "  Start  : ./scripts/start-demo.sh"
echo "  Stop   : ./scripts/stop-demo.sh"
echo "  Logs   : tail -f $LOG_DIR/*.log"
echo "  Board  : http://localhost:5055/"
echo ""
