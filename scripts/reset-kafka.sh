#!/bin/bash

set -euo pipefail

KAFKA_BIN="/opt/kafka/bin"
BROKER="localhost:9092"

TOPICS=(
  "ecommerce-orders"
  "iot-sensors-csv"
)

PARTITIONS=3
REPLICATION=1

echo "========================================="
echo " Recreating Kafka Demo Topics"
echo "========================================="
echo "Broker: $BROKER"
echo

for TOPIC in "${TOPICS[@]}"; do
    echo "--------------------------------------------------"
    echo "Processing topic: $TOPIC"

    if $KAFKA_BIN/kafka-topics.sh \
        --bootstrap-server "$BROKER" \
        --list | grep -qx "$TOPIC"; then

        echo "Deleting existing topic..."
        $KAFKA_BIN/kafka-topics.sh \
            --bootstrap-server "$BROKER" \
            --delete \
            --topic "$TOPIC"

        # Wait until deletion completes
        while $KAFKA_BIN/kafka-topics.sh \
            --bootstrap-server "$BROKER" \
            --list | grep -qx "$TOPIC"; do
            echo "Waiting for topic deletion..."
            sleep 2
        done

        echo "Topic deleted."
    else
        echo "Topic does not exist."
    fi

    echo "Creating topic..."

    $KAFKA_BIN/kafka-topics.sh \
        --bootstrap-server "$BROKER" \
        --create \
        --topic "$TOPIC" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION"

    echo "Verifying..."

    $KAFKA_BIN/kafka-topics.sh \
        --bootstrap-server "$BROKER" \
        --describe \
        --topic "$TOPIC"

    echo
done

echo "========================================="
echo "Current Topics"
echo "========================================="

$KAFKA_BIN/kafka-topics.sh \
    --bootstrap-server "$BROKER" \
    --list | sort

echo
echo "✓ Kafka topics recreated successfully."

