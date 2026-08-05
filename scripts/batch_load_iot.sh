#!/bin/bash
# =============================================================================
# WHPG Demo — One-shot gpfdist batch load into iot_sensor_readings
# =============================================================================
# Generates N synthetic IoT rows, serves them via a dedicated gpfdist instance
# (separate from the FlowServer streaming job's gpfdist on :6070), loads them
# through a Greenplum external table, then tears everything down.
#
# Usage: ./batch_load_iot.sh [row_count] [gpfdist_port]
#   row_count     default 5000000
#   gpfdist_port  default 6072  (must differ from FlowServer's :6070)
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ROW_COUNT="${1:-5000000}"
GPFDIST_PORT="${2:-6072}"

PGHOST="localhost"
PGPORT="5432"
PGUSER="gpadmin"
PGDATABASE="streaming_demo"

DATA_DIR="/tmp/gpfdist-batch"
CSV_NAME="iot_batch_$(date +%s).csv"
CSV_PATH="$DATA_DIR/$CSV_NAME"
PID_FILE="/tmp/gpfdist-batch-load.pid"
GPFDIST_HOST="$(hostname -f 2>/dev/null || hostname)"

EXT_TABLE="ext_iot_batch_load"

cleanup() {
    if [[ -f "$PID_FILE" ]]; then
        PID=$(cat "$PID_FILE" 2>/dev/null || true)
        if [[ -n "${PID:-}" ]] && kill -0 "$PID" 2>/dev/null; then
            kill "$PID" 2>/dev/null || true
            echo "  ✓ gpfdist (PID $PID) stopped"
        fi
        rm -f "$PID_FILE"
    fi
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
        -c "DROP EXTERNAL TABLE IF EXISTS public.${EXT_TABLE};" >/dev/null 2>&1 || true
    rm -f "$CSV_PATH"
}
trap cleanup EXIT

echo "=============================================="
echo " Batch Load: iot_sensor_readings"
echo "=============================================="
echo " Rows        : $ROW_COUNT"
echo " gpfdist port: $GPFDIST_PORT"
echo "=============================================="
echo ""

# ── 1. Generate the CSV ───────────────────────────────────────────────────────
echo "► Generating $ROW_COUNT rows..."
mkdir -p "$DATA_DIR"
python3 "$SCRIPT_DIR/generate_iot_batch.py" "$CSV_PATH" "$ROW_COUNT"
echo ""

# ── 2. Start a dedicated gpfdist instance ─────────────────────────────────────
echo "► Starting gpfdist on :$GPFDIST_PORT..."
if command -v gpfdist >/dev/null 2>&1; then
    GPFDIST_BIN="gpfdist"
else
    GPFDIST_BIN="/usr/local/greenplum-db/bin/gpfdist"
fi
"$GPFDIST_BIN" -d "$DATA_DIR" -p "$GPFDIST_PORT" -l "/tmp/gpfdist-batch-load.log" &
GPFDIST_PID=$!
echo "$GPFDIST_PID" > "$PID_FILE"

for i in $(seq 1 10); do
    nc -z localhost "$GPFDIST_PORT" 2>/dev/null && break
    [[ $i -eq 10 ]] && { echo "  ✗ gpfdist never came up on :$GPFDIST_PORT"; exit 1; }
    sleep 1
done
echo "  ✓ gpfdist ready (PID $GPFDIST_PID)"
echo ""

# ── 3. Create external table + load ───────────────────────────────────────────
echo "► Loading via external table..."
LOAD_START=$(date +%s)

psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 <<SQL
DROP EXTERNAL TABLE IF EXISTS public.${EXT_TABLE};

CREATE EXTERNAL TABLE public.${EXT_TABLE} (
    col_timestamp     text,
    col_sensor_id     text,
    col_location      text,
    col_temperature   text,
    col_humidity      text,
    col_pressure      text,
    col_battery_level text,
    col_status        text
)
LOCATION ('gpfdist://${GPFDIST_HOST}:${GPFDIST_PORT}/${CSV_NAME}')
FORMAT 'CSV' (HEADER)
LOG ERRORS SEGMENT REJECT LIMIT 1000 ROWS;

INSERT INTO public.iot_sensor_readings (
    timestamp, sensor_id, location, temperature, humidity, pressure,
    battery_level, status, alert_level, building, floor, temperature_f,
    comfort_index, battery_status, processed_at, data_quality
)
SELECT
    col_timestamp::timestamp,
    col_sensor_id,
    col_location,
    col_temperature::decimal(5,2),
    col_humidity::decimal(5,2),
    col_pressure::decimal(6,2),
    col_battery_level::decimal(5,2),
    col_status,
    CASE
        WHEN col_status = 'normal' THEN 0
        WHEN col_status IN ('high_temp', 'low_temp', 'high_humidity') THEN 1
        WHEN col_status = 'low_battery' THEN 2
        ELSE 3
    END,
    SPLIT_PART(col_location, '-', 1),
    CASE
        WHEN col_location LIKE '%Floor-%'
        THEN CAST(SPLIT_PART(col_location, '-', 3) AS INTEGER)
        ELSE NULL
    END,
    (col_temperature::decimal * 9.0 / 5.0) + 32.0,
    CASE
        WHEN col_temperature::decimal BETWEEN 20 AND 24
         AND col_humidity::decimal BETWEEN 40 AND 60 THEN 'comfortable'
        WHEN col_temperature::decimal BETWEEN 18 AND 26
         AND col_humidity::decimal BETWEEN 35 AND 65 THEN 'acceptable'
        ELSE 'uncomfortable'
    END,
    CASE
        WHEN col_battery_level::decimal >= 80 THEN 'good'
        WHEN col_battery_level::decimal >= 40 THEN 'medium'
        WHEN col_battery_level::decimal >= 20 THEN 'low'
        ELSE 'critical'
    END,
    NOW(),
    CASE
        WHEN col_temperature::decimal BETWEEN -40 AND 50
         AND col_humidity::decimal BETWEEN 0 AND 100
         AND col_pressure::decimal BETWEEN 900 AND 1100
        THEN 'valid'
        ELSE 'suspicious'
    END
FROM public.${EXT_TABLE};

DROP EXTERNAL TABLE public.${EXT_TABLE};
SQL

LOAD_END=$(date +%s)
DURATION=$((LOAD_END - LOAD_START))
echo ""
echo "  ✓ Load complete in ${DURATION}s"
echo ""

echo "=============================================="
echo " ✓ Batch load finished: $ROW_COUNT rows"
echo "=============================================="


