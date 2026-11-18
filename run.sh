#!/usr/bin/env bash
# Runs the whole system on this machine: infrastructure in Docker Compose, the three services as local processes.
#
#   ./run.sh setup    first run, or after `docker compose down -v`: infrastructure, topics, services, connectors, schemas
#   ./run.sh up       start everything again after `./run.sh down`
#   ./run.sh down     stop the services and the containers; data is kept
#   ./run.sh status   containers, services, connectors
#   ./run.sh build    rebuild the three jars, after a code change
#   ./run.sh logs order|inventory|payment
#
# Service logs and process ids are kept in .run/.
set -euo pipefail

cd "$(dirname "$0")"

SERVICES=(order inventory payment)
RUN_DIR=.run

port() {
  case $1 in
    order) echo 8081 ;;
    inventory) echo 8082 ;;
    payment) echo 8084 ;;
  esac
}

jar() {
  echo "$1-service/target/$1-service-0.0.1-SNAPSHOT.jar"
}

java_home() {
  if [ -x /usr/libexec/java_home ] && /usr/libexec/java_home -v 25 >/dev/null 2>&1; then
    /usr/libexec/java_home -v 25
  elif [ -n "${JAVA_HOME:-}" ]; then
    echo "$JAVA_HOME"
  else
    echo "Java 25 not found: install it or set JAVA_HOME" >&2
    exit 1
  fi
}

build() {
  export JAVA_HOME
  JAVA_HOME=$(java_home)
  for service in "${SERVICES[@]}"; do
    echo "building $service-service"
    (cd "$service-service" && ./mvnw -q -B clean package -DskipTests)
  done
}

infrastructure() {
  if ! docker info >/dev/null 2>&1; then
    echo "Docker is not running. Start Docker Desktop first." >&2
    exit 1
  fi
  docker compose up -d
  until docker exec postgres pg_isready -U postgres >/dev/null 2>&1; do
    echo "waiting for postgres..."
    sleep 2
  done
}

start_services() {
  local missing=false
  for service in "${SERVICES[@]}"; do
    [ -f "$(jar "$service")" ] || missing=true
  done
  if [ "$missing" = true ]; then
    build
  fi

  mkdir -p "$RUN_DIR"
  local java
  java="$(java_home)/bin/java"
  for service in "${SERVICES[@]}"; do
    if running "$service"; then
      echo "$service-service already running"
      continue
    fi
    nohup "$java" -jar "$(jar "$service")" > "$RUN_DIR/$service.log" 2>&1 &
    echo $! > "$RUN_DIR/$service.pid"
    echo "started $service-service, log in $RUN_DIR/$service.log"
  done

  for service in "${SERVICES[@]}"; do
    local tries=0
    until curl -sf "localhost:$(port "$service")/actuator/health" >/dev/null; do
      tries=$((tries + 1))
      if [ $tries -gt 60 ] || ! running "$service"; then
        echo "$service-service did not start, see $RUN_DIR/$service.log" >&2
        exit 1
      fi
      sleep 2
    done
    echo "$service-service is up on port $(port "$service")"
  done
}

running() {
  local pid_file="$RUN_DIR/$1.pid"
  [ -f "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null
}

stop_services() {
  for service in "${SERVICES[@]}"; do
    if running "$service"; then
      local pid
      pid=$(cat "$RUN_DIR/$service.pid")
      kill "$pid"
      # Spring Boot shuts down gracefully; wait, so a following start does not find the port taken
      for _ in $(seq 1 30); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
      done
      echo "stopped $service-service"
    fi
    rm -f "$RUN_DIR/$service.pid"
  done
}

setup() {
  infrastructure
  infra/setup.sh topics
  # Flyway creates the outbox tables and publications when the services start, and the connectors need them
  start_services
  infra/setup.sh connectors
  infra/setup.sh schemas
  echo
  echo "ready: open http://localhost:8081/"
}

up() {
  infrastructure
  start_services
  echo
  echo "ready: open http://localhost:8081/"
}

down() {
  stop_services
  docker compose stop
}

status() {
  docker compose ps --format 'table {{.Name}}\t{{.Status}}'
  echo
  for service in "${SERVICES[@]}"; do
    if curl -sf "localhost:$(port "$service")/actuator/health" >/dev/null; then
      echo "$service-service  up on port $(port "$service")"
    else
      echo "$service-service  down"
    fi
  done
  echo
  for connector in order inventory payment; do
    local state
    state=$(curl -sf "localhost:8083/connectors/$connector-outbox/status" 2>/dev/null \
      | grep -o '"state":"[A-Z]*"' | head -1 | cut -d'"' -f4 || true)
    echo "$connector-outbox connector  ${state:-not found}"
  done
}

case "${1:-}" in
  setup) setup ;;
  up) up ;;
  down) down ;;
  status) status ;;
  build) build ;;
  logs) tail -f "$RUN_DIR/${2:?usage: $0 logs order|inventory|payment}.log" ;;
  *) echo "usage: $0 setup|up|down|status|build|logs <service>" >&2; exit 1 ;;
esac
