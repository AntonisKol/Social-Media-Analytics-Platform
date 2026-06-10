#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
TOPIC="${KAFKA_TOPIC:-social_posts_raw}"
PARTITIONS="${KAFKA_PARTITIONS:-1}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"

docker exec "$CONTAINER" kafka-topics \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION_FACTOR"

docker exec "$CONTAINER" kafka-topics \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --describe \
  --topic "$TOPIC"
