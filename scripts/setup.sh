#!/bin/bash
# =============================================================================
# WHPG Real Streaming Demo — Full Setup
# =============================================================================
# Run as: gpadmin
# Usage:  ./scripts/setup.sh [options]
#
# Options:
#   --pg-host HOST        PostgreSQL host        (default: localhost)
#   --pg-port PORT        PostgreSQL port        (default: 5432)
#   --pg-user USER        PostgreSQL user        (default: gpadmin)
#   --kafka-broker ADDR   Kafka broker           (default: localhost:9092)
#   --kafka-bin DIR       Kafka bin dir          (default: /opt/kafka/bin)
#   --partitions N        Topic partitions       (default: 3)
#   --replication N       Replication factor     (default: 1)
#   --skip-db             Skip Step 1  (DB + tables)
#   --skip-kafka          Skip Step 2  (Kafka topics)
#   --skip-build          Skip Step 3  (Go build)
#   --skip-pip            Skip Step 4  (pip install)
#   --skip-jobs           Skip Step 5  (FlowServer job submit)
# =============================================================================

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

PG_HOST="localhost"
PG_PORT="5432"
PG_USER="gpadmin"
KAFKA_BROKER="localhost:9092"
KAFKA_BIN="/opt/kafka/bin"
PARTITIONS=3
REPLICATION=1
SKIP_DB=0
SKIP_KAFKA=0
SKIP_BUILD=0
SKIP_PIP=0
SKIP_JOBS=0

while [[ $# -gt 0 ]]; do
    case $1 in
        --pg-host)      PG_HOST="$2";      shift 2 ;;
        --pg-port)      PG_PORT="$2";      shift 2 ;;
        --pg-user)      PG_USER="$2";      shift 2 ;;
        --kafka-broker) KAFKA_BROKER="$2"; shift 2 ;;
        --kafka-bin)    KAFKA_BIN="$2";    shift 2 ;;
        --partitions)   PARTITIONS="$2";   shift 2 ;;
        --replication)  REPLICATION="$2";  shift 2 ;;
        --skip-db)      SKIP_DB=1;         shift ;;
        --skip-kafka)   SKIP_KAFKA=1;      shift ;;
        --skip-build)   SKIP_BUILD=1;      shift ;;
        --skip-pip)     SKIP_PIP=1;        shift ;;
        --skip-jobs)    SKIP_JOBS=1;       shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

cd "$REPO_DIR"
mkdir -p /tmp/flowserver-demo/logs

echo ""
echo "=============================================="
echo " WHPG Real Streaming Demo — Setup"
echo "=============================================="
echo " Repo      : $REPO_DIR"
echo " PG        : $PG_USER@$PG_HOST:$PG_PORT"
echo " Kafka     : $KAFKA_BROKER"
echo " Kafka bin : $KAFKA_BIN"
echo "=============================================="
echo ""

# ── STEP 1 — Database & Tables ────────────────────────────────────────────────
if [[ $SKIP_DB -eq 0 ]]; then
    echo "► Step 1/5 — Database & Tables"

    # Terminate any active connections so DROP DATABASE succeeds
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d postgres -c \
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
         WHERE datname='streaming_demo' AND pid <> pg_backend_pid();" \
        > /dev/null 2>&1 || true

    psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d postgres \
         -f sql/01_create_database.sql
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d streaming_demo \
         -f sql/02_create_tables.sql
    echo "  ✓ Database ready"
else
    echo "► Step 1/5 — Database & Tables [SKIPPED]"
fi
echo ""

# ── STEP 2 — Kafka Topics (runs as kafka user) ────────────────────────────────
if [[ $SKIP_KAFKA -eq 0 ]]; then
    echo "► Step 2/5 — Kafka Topics (sudo su - kafka)"

    INNER="/tmp/kafka-setup-$$.sh"
    cat > "$INNER" << INNEREOF
#!/bin/bash
set -e
KAFKA_BIN="$KAFKA_BIN"
KAFKA_BROKER="$KAFKA_BROKER"
PARTITIONS="$PARTITIONS"
REPLICATION="$REPLICATION"

# Use kafka-topics or kafka-topics.sh
if [[ -x "\$KAFKA_BIN/kafka-topics" ]]; then
    KT="\$KAFKA_BIN/kafka-topics"
else
    KT="\$KAFKA_BIN/kafka-topics.sh"
fi

echo "  Kafka bin : \$KAFKA_BIN"
echo "  Running as: \$(whoami)"
echo ""

for TOPIC in ecommerce-orders iot-sensors-csv; do
    if "\$KT" --list --bootstrap-server "\$KAFKA_BROKER" 2>/dev/null | grep -q "^\${TOPIC}$"; then
        echo "  ~ Topic '\$TOPIC' already exists — keeping it"
    else
        "\$KT" --create --topic "\$TOPIC" \
            --bootstrap-server "\$KAFKA_BROKER" \
            --partitions "\$PARTITIONS" \
            --replication-factor "\$REPLICATION"
        echo "  ✓ Topic '\$TOPIC' created (\$PARTITIONS partitions)"
    fi
done

echo ""
echo "  Topics confirmed:"
"\$KT" --list --bootstrap-server "\$KAFKA_BROKER" | \
    grep -E "^(ecommerce-orders|iot-sensors-csv)$" | sed 's/^/    /'
INNEREOF

    chmod 755 "$INNER"
    sudo su - kafka -c "bash $INNER"
    rm -f "$INNER"
    # Restore ownership in case kafka user touched anything
    chown -R "$(whoami):" "$REPO_DIR" 2>/dev/null || true
    echo "  ✓ Kafka topics ready"
else
    echo "► Step 2/5 — Kafka Topics [SKIPPED]"
fi
echo ""

# ── STEP 3 — Build Go Generators ─────────────────────────────────────────────
if [[ $SKIP_BUILD -eq 0 ]]; then
    echo "► Step 3/5 — Build Go Generators"
    chown -R "$(whoami):" "$REPO_DIR/generators" 2>/dev/null || true
    chmod u+rw "$REPO_DIR/generators/go.mod" "$REPO_DIR/generators/go.sum" 2>/dev/null || true

    cd "$REPO_DIR/generators"
    echo "  Running go mod tidy..."
    go mod tidy
    echo "  Building order-generator..."
    go build -o order-generator order-generator.go
    echo "  ✓ order-generator built"
    echo "  Building iot-generator..."
    go build -o iot-generator iot-generator.go
    echo "  ✓ iot-generator built"
    cd "$REPO_DIR"
else
    echo "► Step 3/5 — Go Generators [SKIPPED]"
fi
echo ""

# ── STEP 4 — Python Dependencies ─────────────────────────────────────────────
if [[ $SKIP_PIP -eq 0 ]]; then
    echo "► Step 4/5 — Python Dependencies"
    pip3 install flask flask-cors psycopg2-binary --break-system-packages -q \
        || pip install flask flask-cors psycopg2-binary --break-system-packages -q
    echo "  ✓ flask, flask-cors, psycopg2-binary installed"
else
    echo "► Step 4/5 — Python Dependencies [SKIPPED]"
fi
echo ""

# ── STEP 5 — Submit FlowServer Jobs ──────────────────────────────────────────
if [[ $SKIP_JOBS -eq 0 ]]; then
    echo "► Step 5/5 — FlowServer Jobs"

    if ! nc -z localhost 6060 2>/dev/null; then
        echo "  ⚠ FlowServer not running at localhost:6060"
        echo "    Start it first, then re-run:"
        echo "    ./scripts/setup.sh --skip-db --skip-kafka --skip-build --skip-pip"
    else
        for JOB in jobs/ecommerce-orders.yaml jobs/iot-sensors-csv.yaml; do
            NAME=$(basename "$JOB" .yaml)
            OUT=$(flowcli --host localhost --port 6060 submit "$JOB" 2>&1)
            if echo "$OUT" | grep -qi "success\|submitted"; then
                echo "  ✓ '$NAME' submitted"
            else
                echo "  ~ '$NAME' already submitted"
            fi
        done
    fi
else
    echo "► Step 5/5 — FlowServer Jobs [SKIPPED]"
fi
echo ""

echo "=============================================="
echo " ✓ Setup complete!"
echo "=============================================="
echo ""
echo " Next:"
echo "   ./scripts/start-demo.sh"
echo "   open http://localhost:5055/"
echo ""
