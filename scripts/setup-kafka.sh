#!/bin/bash
# =============================================================================
# WHPG Real Streaming Demo — Kafka Setup
# =============================================================================
# Run as: gpadmin (script will sudo su - kafka automatically)
#
# Usage: ./scripts/setup-kafka.sh [options]
#
# Options:
#   --broker BROKER       Kafka broker        (default: localhost:9092)
#   --kafka-bin DIR       Kafka bin dir       (default: auto-detect)
#   --partitions N        Partitions          (default: 3)
#   --replication N       Replication factor  (default: 1)
#   --no-auth             Skip SASL auth
# =============================================================================

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

KAFKA_BROKER="localhost:9092"
KAFKA_BIN=""
PARTITIONS=3
REPLICATION=1
NO_AUTH=0

while [[ $# -gt 0 ]]; do
    case $1 in
        --broker)      KAFKA_BROKER="$2"; shift 2 ;;
        --kafka-bin)   KAFKA_BIN="$2";    shift 2 ;;
        --partitions)  PARTITIONS="$2";   shift 2 ;;
        --replication) REPLICATION="$2";  shift 2 ;;
        --no-auth)     NO_AUTH=1;         shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo ""
echo "=============================================="
echo " WHPG Streaming Demo — Kafka Setup"
echo "=============================================="
echo " Broker     : $KAFKA_BROKER"
echo " Partitions : $PARTITIONS  Replication: $REPLICATION"
echo "=============================================="
echo ""

# ── Prompt for Kafka password ─────────────────────────────────────────────────
KAFKA_PASSWORD=""
if [[ $NO_AUTH -eq 0 ]]; then
    read -r -s -p "  Enter Kafka password (leave blank to skip auth): " KAFKA_PASSWORD
    echo ""
    if [[ -z "$KAFKA_PASSWORD" ]]; then
        echo "  ~ No password entered — connecting without authentication"
    else
        echo "  ✓ Password received"
    fi
    echo ""
fi

# ── Build the inner script that will run as kafka ─────────────────────────────
# Create it via mktemp with restrictive perms from the moment it exists, so
# there's no window where it's world-readable before we chmod it. It contains
# the plaintext Kafka password, so it must never be group/world readable.
INNER_SCRIPT="$(mktemp /tmp/kafka-setup-inner-XXXXXXXX.sh)"
chmod 600 "$INNER_SCRIPT"

cat > "$INNER_SCRIPT" << INNEREOF
#!/bin/bash
set -e

KAFKA_BROKER="$KAFKA_BROKER"
KAFKA_BIN="$KAFKA_BIN"
PARTITIONS="$PARTITIONS"
REPLICATION="$REPLICATION"
KAFKA_PASSWORD="$KAFKA_PASSWORD"

# ── Locate kafka-topics ───────────────────────────────────────────────────────
find_kafka_bin() {
    if [[ -n "\$KAFKA_BIN" ]]; then
        echo "\$KAFKA_BIN"; return 0
    fi
    if command -v kafka-topics &>/dev/null; then
        dirname "\$(command -v kafka-topics)"; return 0
    fi
    if command -v kafka-topics.sh &>/dev/null; then
        dirname "\$(command -v kafka-topics.sh)"; return 0
    fi
    for DIR in /opt/kafka/bin /opt/kafka_*/bin /usr/local/kafka/bin /kafka/bin /home/kafka/bin /opt/bitnami/kafka/bin; do
        for D in \$DIR; do
            if [[ -x "\$D/kafka-topics.sh" ]] || [[ -x "\$D/kafka-topics" ]]; then
                echo "\$D"; return 0
            fi
        done
    done
    return 1
}

KAFKA_BIN_DIR="\$(find_kafka_bin)" || {
    echo "  ✗ Cannot find kafka-topics. Pass --kafka-bin /path/to/kafka/bin"
    exit 1
}
export PATH="\$KAFKA_BIN_DIR:\$PATH"

if [[ -x "\$KAFKA_BIN_DIR/kafka-topics.sh" ]] && ! [[ -x "\$KAFKA_BIN_DIR/kafka-topics" ]]; then
    KT="\$KAFKA_BIN_DIR/kafka-topics.sh"
else
    KT="\$KAFKA_BIN_DIR/kafka-topics"
fi

echo "  Kafka bin : \$KAFKA_BIN_DIR"

# ── Write SASL properties to /tmp if password given ───────────────────────────
PROPS_FILE=""
if [[ -n "\$KAFKA_PASSWORD" ]]; then
    PROPS_FILE="\$(mktemp /tmp/kafka-props-XXXXXXXX.properties)"
    chmod 600 "\$PROPS_FILE"
    cat > "\$PROPS_FILE" << PROPSEOF
security.protocol=PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka" password="\${KAFKA_PASSWORD}";
PROPSEOF
fi

kt() {
    if [[ -n "\$PROPS_FILE" ]]; then
        "\$KT" "\$@" --command-config "\$PROPS_FILE"
    else
        "\$KT" "\$@"
    fi
}

# ── Create topics ─────────────────────────────────────────────────────────────
echo ""
echo "► Creating Kafka topics as user: \$(whoami)"
echo ""

for TOPIC in ecommerce-orders iot-sensors-csv; do
    kt --delete --topic "\$TOPIC" \
        --bootstrap-server "\$KAFKA_BROKER" 2>/dev/null && \
        echo "  ~ Deleted existing topic '\$TOPIC'" || true

    kt --create \
        --topic "\$TOPIC" \
        --bootstrap-server "\$KAFKA_BROKER" \
        --partitions "\$PARTITIONS" \
        --replication-factor "\$REPLICATION"

    echo "  ✓ Topic '\$TOPIC' created (\$PARTITIONS partitions)"
done

echo ""
echo "  Topics in broker:"
kt --list --bootstrap-server "\$KAFKA_BROKER" | \
    grep -E "ecommerce-orders|iot-sensors-csv" | sed 's/^/    /'

[[ -n "\$PROPS_FILE" ]] && rm -f "\$PROPS_FILE"
INNEREOF

chmod 700 "$INNER_SCRIPT"

# Hand ownership to kafka so it can read/execute the file (it holds the
# plaintext password), while chmod 700 keeps every other user locked out —
# this requires the same sudo privilege already needed for `sudo su - kafka`.
sudo chown kafka:kafka "$INNER_SCRIPT"

# ── Run as kafka via sudo su ───────────────────────────────────────────────────
echo "  Switching to kafka user via sudo su - kafka..."
echo ""

# Temporarily disable errexit so a non-zero exit from the inner script doesn't
# short-circuit past our cleanup and error-reporting logic below.
set +e
sudo su - kafka -c "bash $INNER_SCRIPT"
RC=$?
set -e

# Cleanup — the file is now owned by kafka, and /tmp's sticky bit means only
# the file's owner (or root) can unlink it, so plain `rm` may fail here.
sudo rm -f "$INNER_SCRIPT"

if [[ $RC -eq 0 ]]; then
    echo ""
    echo "=============================================="
    echo " ✓ Kafka topics ready!"
    echo "=============================================="
    echo ""
    echo " Next steps (as gpadmin):"
    echo "   ./flowserver -c configs/flow_server.json &"
    echo "   ./scripts/start-demo.sh"
    echo "   open http://localhost:5055/"
    echo ""
else
    echo ""
    echo "  ✗ Kafka setup failed (exit code $RC)"
    echo ""
    exit $RC
fi

