#!/usr/bin/env bash
# Creates the Kafka topics and registers the Debezium connectors. Safe to run again.
#
#   infra/setup.sh topics       after `docker compose up -d`
#   infra/setup.sh connectors   after each service has started once (Flyway creates the publications)
set -euo pipefail

cd "$(dirname "$0")/.."

kafka() {
  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 "$@"
}

create_topic() {
  local name=$1 partitions=$2
  shift 2
  kafka --create --if-not-exists --topic "$name" --partitions "$partitions" --replication-factor 3 "$@"
}

topics() {
  until kafka --list >/dev/null 2>&1; do
    echo "waiting for kafka..."
    sleep 2
  done

  for topic in orders inventory payments; do
    create_topic "$topic" 3
  done

  # Debezium heartbeats: the connector produces to these, and auto-create is off
  for prefix in order-db inventory-db payment-db; do
    create_topic "__debezium-heartbeat.$prefix" 1 --config cleanup.policy=compact
    create_topic "$prefix.public.debezium_heartbeat" 1 --config cleanup.policy=compact
  done
}

connectors() {
  until curl -sf localhost:8083/connectors >/dev/null; do
    echo "waiting for kafka connect..."
    sleep 2
  done

  for name in order inventory payment; do
    curl -sf -X PUT "localhost:8083/connectors/$name-outbox/config" \
      -H 'Content-Type: application/json' -d @"infra/debezium/$name-outbox-connector.json" >/dev/null
    echo "registered $name-outbox"
  done
}

case "${1:-}" in
  topics) topics ;;
  connectors) connectors ;;
  *) echo "usage: $0 topics|connectors" >&2; exit 1 ;;
esac
