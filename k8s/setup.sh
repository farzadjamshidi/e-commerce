#!/usr/bin/env bash
# Builds the local Kubernetes environment step by step. Safe to run again.
#
#   k8s/setup.sh cluster    kind cluster with one control plane and two workers
#   k8s/setup.sh strimzi    the Strimzi operator, in the kafka namespace
#   k8s/setup.sh kafka      the Kafka cluster, three nodes in KRaft mode, and its topics
#   k8s/setup.sh postgres   Postgres with the three service databases, in the ecommerce namespace
#   k8s/setup.sh connect    Kafka Connect with the Debezium Postgres connector
#   k8s/setup.sh registry   Schema Registry, and the schemas registered by the build
#   k8s/setup.sh services   images of the three services, and their Deployments
#   k8s/setup.sh connectors the Debezium connectors, after the services have created the publications
set -euo pipefail

cd "$(dirname "$0")/.."

STRIMZI_VERSION=1.3.1

cluster() {
  if kind get clusters | grep -qx ecommerce; then
    echo "cluster ecommerce exists"
  else
    kind create cluster --config k8s/kind-config.yaml
  fi
  kubectl wait --for=condition=Ready nodes --all --timeout=180s
}

strimzi() {
  kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -
  # The release file binds its roles to the namespace "myproject"
  curl -sfL "https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-cluster-operator-${STRIMZI_VERSION}.yaml" \
    | sed 's/namespace: myproject/namespace: kafka/' \
    | kubectl apply --server-side -n kafka -f -
  kubectl -n kafka rollout status deployment/strimzi-cluster-operator --timeout=300s
}

kafka() {
  kubectl apply -f k8s/kafka/kafka-cluster.yaml
  kubectl -n kafka wait kafka/ecommerce --for=condition=Ready --timeout=600s
  kubectl apply -f k8s/kafka/topics.yaml
  kubectl -n kafka wait kafkatopic --all --for=condition=Ready --timeout=120s
}

postgres() {
  kubectl create namespace ecommerce --dry-run=client -o yaml | kubectl apply -f -
  # Practice credentials, created here so no secret is written in a file
  kubectl -n ecommerce create secret generic postgres --from-literal=password=postgres \
    --dry-run=client -o yaml | kubectl apply -f -
  # The same script Compose runs, so users and databases are defined once
  kubectl -n ecommerce create configmap postgres-init --from-file=infra/postgres/init.sql \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl apply -f k8s/postgres/postgres.yaml
  kubectl -n ecommerce rollout status statefulset/postgres --timeout=300s
}

connect() {
  # Built on this machine and copied into the kind nodes, so no image registry is needed
  docker build -t ecommerce/kafka-connect-debezium:3.7.0 k8s/debezium
  kind load docker-image ecommerce/kafka-connect-debezium:3.7.0 --name ecommerce
  kubectl apply -f k8s/debezium/kafka-connect.yaml
  kubectl -n kafka wait kafkaconnect/debezium --for=condition=Ready --timeout=600s
}

registry() {
  kubectl apply -f k8s/schema-registry/schema-registry.yaml
  kubectl -n kafka rollout status deployment/schema-registry --timeout=300s
  # The build registers schemas against localhost:8085, the same port as in Compose
  kubectl -n kafka port-forward svc/schema-registry 8085:8081 >/dev/null &
  local forward=$!
  trap 'kill $forward' RETURN
  until curl -sf localhost:8085/subjects >/dev/null; do sleep 1; done
  (cd inventory-service && ./mvnw -q -B schema-registry:register)
  echo "registered schemas: $(curl -s localhost:8085/subjects)"
}

services() {
  for service in order inventory payment; do
    docker build -t "ecommerce/${service}-service:0.1" "${service}-service"
    kind load docker-image "ecommerce/${service}-service:0.1" --name ecommerce
    kubectl apply -f "k8s/services/${service}-service.yaml"
  done
  for service in order inventory payment; do
    kubectl -n ecommerce rollout status "deployment/${service}-service" --timeout=300s
  done
}

connectors() {
  kubectl apply -f k8s/debezium/connectors.yaml
  kubectl -n kafka wait kafkaconnector --all --for=condition=Ready --timeout=180s
}

case "${1:-}" in
  cluster) cluster ;;
  strimzi) strimzi ;;
  kafka) kafka ;;
  postgres) postgres ;;
  connect) connect ;;
  registry) registry ;;
  services) services ;;
  connectors) connectors ;;
  *) echo "usage: $0 cluster|strimzi|kafka|postgres|connect|registry|services|connectors" >&2; exit 1 ;;
esac
