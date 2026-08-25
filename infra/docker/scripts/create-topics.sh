#!/bin/bash

MAX_ATTEMPTS=12
INTERVAL=15
TOPICS=(
    "events-raw"
    "events-normalized"
    "events-aggregated"
)

for ((attempt=1; attempt<=MAX_ATTEMPTS; attempt++)); do
    echo "Attempt $attempt/$MAX_ATTEMPTS"

    success=true

    for topic in "${TOPICS[@]}"; do
        if ! /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server broker-1:19092 \
            --create \
            --if-not-exists \
            --topic "$topic" \
            --partitions 3 \
            --replication-factor 3 \
            --config retention.ms=604800000; then

            success=false
            break
        fi
    done

    if [ "$success" = true ]; then
        echo "Kafka topics created successfully!"
        exit 0
    fi

    if [ "$attempt" -lt "$MAX_ATTEMPTS" ]; then
        echo "Attempt failed. Retrying in $INTERVAL seconds..."
        sleep "$INTERVAL"
    fi
done

MAX_WAIT=$((MAX_ATTEMPTS * INTERVAL))

echo "Failed to create Kafka topics after $MAX_WAIT seconds."
exit 1