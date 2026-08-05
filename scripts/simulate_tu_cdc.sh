#!/bin/bash
# =============================================================================
# TU Bureau Demo — CDC Source Simulator
# =============================================================================
# Debezium only captures changes that actually happen — this script generates
# real INSERT/UPDATE traffic against oltp.credit_accounts and
# oltp.bureau_score_events on EPAS so the WAL has something to stream via
# pgoutput -> Kafka -> FlowServer -> WHPG.
#
# Run as the enterprisedb OS user, with env.sh already sourced so PGPORT /
# PGDATABASE are set (peer auth — no password needed):
#
#   source ~/env.sh
#   ./simulate_tu_cdc.sh --rate 5
#
# Usage: ./simulate_tu_cdc.sh [--rate N] [--max-events N]
#   --rate N        events/sec (default 5)
#   --max-events N  stop after N events (default 0 = unlimited, Ctrl-C to stop)
# =============================================================================
set -uo pipefail

RATE=5
MAX_EVENTS=0
NEW_ACCOUNT_PCT=30   # % of events: brand-new account (INSERT)
SCORE_EVENT_PCT=20   # % of events: bureau score event (INSERT, append-only)
# remainder = update an existing account (simulate repayment/delinquency change)

while [[ $# -gt 0 ]]; do
    case $1 in
        --rate)       RATE="$2";       shift 2 ;;
        --max-events) MAX_EVENTS="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

PSQL="psql -X -q -v ON_ERROR_STOP=1 -d ${PGDATABASE:-tu}"
SLEEP=$(awk "BEGIN{print 1/${RATE}}")

PRODUCTS=("Personal Loan" "Credit Card" "Auto Loan" "Home Loan" "Gold Loan" "Consumer Durable Loan")
DPD_BUCKETS=("Current" "1-30" "31-60" "61-90" "90+")
STATUSES=("Active" "Closed" "Delinquent" "Charged-off" "Restructured")
SCORE_VERSIONS=("v3.2" "v4.0")
SCORE_TRIGGERS=("periodic_refresh" "new_inquiry" "account_update" "dispute_resolution")

rand_choice() {
    local arr=("$@")
    echo "${arr[$RANDOM % ${#arr[@]}]}"
}

count=0
echo "=============================================="
echo " TU Bureau Demo — CDC Source Simulator"
echo "=============================================="
echo " Target : ${PGDATABASE:-tu} @ localhost:${PGPORT:-5444}"
echo " Rate   : ${RATE}/s   Max: ${MAX_EVENTS} (0=unlimited)"
echo "=============================================="
echo "Ctrl-C to stop."
echo ""

trap 'echo ""; echo "Stopped. Total events sent: $count"; exit 0' INT TERM

while true; do
    if [[ "$MAX_EVENTS" -gt 0 && "$count" -ge "$MAX_EVENTS" ]]; then
        echo "Reached max-events ($MAX_EVENTS), stopping."
        break
    fi

    ROLL=$((RANDOM % 100))
    CONSUMER_ID=$((RANDOM % 5000 + 1))
    LENDER_ID=$((RANDOM % 20 + 1))

    if (( ROLL < NEW_ACCOUNT_PCT )); then
        # ── New account (INSERT) ──────────────────────────────────────────
        ACCOUNT_ID=$(( $(date +%s%N) / 1000 + RANDOM ))   # near-unique bigint
        PRODUCT=$(rand_choice "${PRODUCTS[@]}")
        SANCTIONED=$(( RANDOM % 490000 + 10000 ))
        if $PSQL -c "
            INSERT INTO oltp.credit_accounts
                (account_id, consumer_id, lender_id, product,
                 sanctioned_amount, outstanding_amount, dpd_bucket, status,
                 opened_ts, event_ts)
            VALUES
                ($ACCOUNT_ID, $CONSUMER_ID, $LENDER_ID, '$PRODUCT',
                 $SANCTIONED, $SANCTIONED, 'Current', 'Active',
                 now(), now());
        " >/dev/null; then
            echo "[$(date +%T)] NEW   account_id=$ACCOUNT_ID consumer=$CONSUMER_ID lender=$LENDER_ID product='$PRODUCT' amount=$SANCTIONED"
        fi

    elif (( ROLL < NEW_ACCOUNT_PCT + SCORE_EVENT_PCT )); then
        # ── Bureau score event (INSERT, append-only) ─────────────────────
        SCORE=$(( RANDOM % 550 + 300 ))
        VERSION=$(rand_choice "${SCORE_VERSIONS[@]}")
        TRIGGER=$(rand_choice "${SCORE_TRIGGERS[@]}")
        if $PSQL -c "
            INSERT INTO oltp.bureau_score_events
                (consumer_id, score_value, score_version, score_trigger, score_ts)
            VALUES
                ($CONSUMER_ID, $SCORE, '$VERSION', '$TRIGGER', now());
        " >/dev/null; then
            echo "[$(date +%T)] SCORE consumer=$CONSUMER_ID score=$SCORE version=$VERSION trigger=$TRIGGER"
        fi

    else
        # ── Update an existing account (simulate repayment/delinquency) ──
        EXISTING=$($PSQL -t -A -c "
            SELECT account_id FROM oltp.credit_accounts
            WHERE status NOT IN ('Closed','Charged-off')
            ORDER BY random() LIMIT 1;
        ")
        if [[ -z "$EXISTING" ]]; then
            echo "[$(date +%T)] (no open accounts yet to update — will retry)"
        else
            DPD=$(rand_choice "${DPD_BUCKETS[@]}")
            STATUS=$(rand_choice "${STATUSES[@]}")
            DELTA=$(( RANDOM % 20000 - 10000 ))   # positive = new draw, negative = repayment
            if $PSQL -c "
                UPDATE oltp.credit_accounts
                SET outstanding_amount = GREATEST(0, outstanding_amount + $DELTA),
                    dpd_bucket = '$DPD',
                    status = '$STATUS',
                    event_ts = now()
                WHERE account_id = $EXISTING;
            " >/dev/null; then
                echo "[$(date +%T)] UPD   account_id=$EXISTING dpd='$DPD' status='$STATUS' delta=$DELTA"
            fi
        fi
    fi

    count=$((count+1))
    if (( count % 20 == 0 )); then
        echo "  ... $count events sent so far"
    fi
    sleep "$SLEEP"
done

