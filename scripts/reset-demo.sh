#!/bin/bash
# =============================================================================
# WHPG Real Streaming Demo — Full Reset
# =============================================================================
# Stops everything (jobs, generators, FlowServer, deletes Kafka topics),
# drops & recreates ecommerce_orders + iot_sensor_readings from
# sql/02_create_tables.sql, then starts everything fresh.
#
# Usage: ./scripts/reset-demo.sh [--ecom-rate N] [--iot-rate N] [--ecom-total N] [--iot-total N]
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

PGHOST="localhost"
PGPORT="5432"
PGUSER="gpadmin"
PGDATABASE="streaming_demo"

ECOM_RATE=10000
IOT_RATE=10000
ECOM_TOTAL=0
IOT_TOTAL=0

while [[ $# -gt 0 ]]; do
    case $1 in
        --ecom-rate)  ECOM_RATE="$2";  shift 2 ;;
        --iot-rate)   IOT_RATE="$2";   shift 2 ;;
        --ecom-total) ECOM_TOTAL="$2"; shift 2 ;;
        --iot-total)  IOT_TOTAL="$2";  shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo ""
echo "=============================================="
echo " WHPG Real Streaming Demo — Full Reset"
echo "=============================================="
echo " This will:"
echo "   1. Stop FlowServer, jobs, generators, dashboard API"
echo "   2. Delete Kafka topics"
echo "   3. DROP + recreate ecommerce_orders and iot_sensor_readings"
echo "   4. Start everything fresh"
echo "=============================================="
echo ""

# ── 1/3 Stop everything (also deletes Kafka topics — no --keep-topics) ───────
echo "► Step 1/3 — Stopping everything..."
"$SCRIPT_DIR/stop-demo.sh"
echo ""

# ── 2/3 Drop + recreate tables ────────────────────────────────────────────────
echo "► Step 2/3 — Dropping & recreating tables..."
SQL_FILE="$REPO_DIR/sql/02_create_tables.sql"
if [[ ! -f "$SQL_FILE" ]]; then
    echo "  ✗ Not found: $SQL_FILE"
    exit 1
fi
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -f "$SQL_FILE"
echo "  ✓ Tables dropped and recreated (ecommerce_orders, iot_sensor_readings)"
echo ""

# ── 3/3 Start everything fresh ────────────────────────────────────────────────
echo "► Step 3/3 — Starting everything fresh..."
"$SCRIPT_DIR/start-demo.sh" \
    --ecom-rate "$ECOM_RATE" --iot-rate "$IOT_RATE" \
    --ecom-total "$ECOM_TOTAL" --iot-total "$IOT_TOTAL"

echo ""
echo "=============================================="
echo " ✓ Full reset complete — demo running fresh"
echo "=============================================="
echo ""
echo " Dashboard : http://localhost:5055/"
echo ""

