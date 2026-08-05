#!/bin/bash
# =============================================================================
# WHPG Real Streaming Demo — gpadmin Setup
# =============================================================================
# Run as: gpadmin
# Does:   DB creation, table creation, Go build, pip install, FlowServer jobs
#
# Usage: ./scripts/setup-gpadmin.sh [options]
#
# Options:
#   --pg-host HOST        PostgreSQL host      (default: localhost)
#   --pg-port PORT        PostgreSQL port      (default: 5432)
#   --pg-user USER        PostgreSQL user      (default: gpadmin)
#   --skip-db             Skip database/table creation
#   --skip-build          Skip Go build
#   --skip-pip            Skip Python pip install
#   --skip-jobs           Skip FlowServer job submission
# =============================================================================

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

PG_HOST="localhost"
PG_PORT="5432"
PG_USER="gpadmin"
SKIP_DB=0
SKIP_BUILD=0
SKIP_PIP=0
SKIP_JOBS=0

while [[ $# -gt 0 ]]; do
    case $1 in
        --pg-host)   PG_HOST="$2";  shift 2 ;;
        --pg-port)   PG_PORT="$2";  shift 2 ;;
        --pg-user)   PG_USER="$2";  shift 2 ;;
        --skip-db)   SKIP_DB=1;     shift ;;
        --skip-build) SKIP_BUILD=1; shift ;;
        --skip-pip)  SKIP_PIP=1;    shift ;;
        --skip-jobs) SKIP_JOBS=1;   shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

cd "$REPO_DIR"

echo ""
echo "=============================================="
echo " WHPG Streaming Demo — gpadmin Setup"
echo "=============================================="
echo " Repo : $REPO_DIR"
echo " PG   : $PG_USER@$PG_HOST:$PG_PORT"
echo "=============================================="
echo ""

# ── 1. Database & Tables ──────────────────────────────────────────────────────
if [[ $SKIP_DB -eq 0 ]]; then
    echo "► Step 1 — Database & Tables"
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d postgres \
         -f sql/01_create_database.sql
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d streaming_demo \
         -f sql/02_create_tables.sql
    echo "  ✓ Database ready"
else
    echo "► Step 1 — Database & Tables [SKIPPED]"
fi
echo ""

# ── 2. Build Go generators ────────────────────────────────────────────────────
if [[ $SKIP_BUILD -eq 0 ]]; then
    echo "► Step 2 — Build Go Generators"
    cd generators
    go mod tidy
    go build -o order-generator order-generator.go
    go build -o iot-generator   iot-generator.go
    cd ..
    echo "  ✓ order-generator built"
    echo "  ✓ iot-generator built"
else
    echo "► Step 2 — Go Generators [SKIPPED]"
fi
echo ""

# ── 3. Python dependencies ────────────────────────────────────────────────────
if [[ $SKIP_PIP -eq 0 ]]; then
    echo "► Step 3 — Python Dependencies"
    pip install flask flask-cors psycopg2-binary --break-system-packages -q
    echo "  ✓ flask, flask-cors, psycopg2-binary installed"
else
    echo "► Step 3 — Python Dependencies [SKIPPED]"
fi
echo ""

# ── 4. Submit FlowServer jobs ─────────────────────────────────────────────────
if [[ $SKIP_JOBS -eq 0 ]]; then
    echo "► Step 4 — FlowServer Jobs"
    echo "  (FlowServer must be running: ./flowserver -c configs/flow_server.json)"
    echo ""
    for JOB in jobs/ecommerce-orders.yaml jobs/iot-sensors-csv.yaml; do
        NAME=$(basename "$JOB" .yaml)
        if flowcli --host localhost --port 6060 submit "$JOB" 2>/dev/null; then
            echo "  ✓ '$NAME' submitted"
        else
            echo "  ⚠ '$NAME' — submit failed (start FlowServer first, then retry)"
        fi
    done
else
    echo "► Step 4 — FlowServer Jobs [SKIPPED]"
fi
echo ""

echo "=============================================="
echo " ✓ gpadmin setup complete!"
echo "=============================================="
echo ""
echo " Next: run setup-kafka.sh as the kafka user:"
echo "   sudo su - kafka"
echo "   cd $REPO_DIR"
echo "   ./scripts/setup-kafka.sh"
echo ""
