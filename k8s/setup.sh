#!/usr/bin/env bash
# Builds the local Kubernetes environment step by step. Safe to run again.
#
#   k8s/setup.sh cluster    kind cluster with one control plane and two workers
#   k8s/setup.sh strimzi    the Strimzi operator, in the kafka namespace
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

case "${1:-}" in
  cluster) cluster ;;
  strimzi) strimzi ;;
  *) echo "usage: $0 cluster|strimzi" >&2; exit 1 ;;
esac
