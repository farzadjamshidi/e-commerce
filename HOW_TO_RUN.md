# How to run

This file describes the branch it is on. Each branch has its own copy, written for its own code.

| Branch | What it adds | Runs on |
|---|---|---|
| `main` | the order saga with orchestration, outbox and CDC, retries and DLTs, monitoring, tracing, tests, Avro | Docker Compose |
| `kubernetes` | the same system on a local Kubernetes cluster, with Strimzi and KEDA | kind |
| `sse-with-frontend` | live order status over Server-Sent Events, and a page to watch it | Docker Compose |

**This branch: `kubernetes`.** Everything runs inside a local kind cluster: Kafka through the Strimzi operator,
Debezium on Kafka Connect, Schema Registry, Postgres, and the three services as Deployments. All of it is described by
the files in `k8s/`, and `k8s/setup.sh` builds it.

The Docker Compose setup of `main` is also on this branch, without `run.sh`. Do not run both at once: they need the
same port 8081 and together more memory than a laptop has.

## What you need

- **Docker Desktop** with about 8 GiB of memory (Settings → Resources). The three cluster nodes use about 6 GiB.
  Turn **Resource Saver** off there, or Docker shuts itself down after some idle minutes and the cluster with it.
- **kind** and **kubectl**: `brew install kind kubectl`.
- **Java 25**, to build the service images and to register the schema.
- **curl**.
- Port 8081 free, and nothing from `docker compose` running: `docker compose stop`.

## First run

```bash
k8s/setup.sh all
```

About 7 minutes, longer the first time because of image downloads. It runs these steps, each of which can also be run
on its own:

| Step | Builds |
|---|---|
| `cluster` | a kind cluster, `ecommerce`: one control plane and two workers |
| `strimzi` | the Strimzi operator, version 1.3.1, in namespace `kafka` |
| `kafka` | Kafka 4.2.2 with three nodes in KRaft mode, and the 15 topics as `KafkaTopic` resources |
| `postgres` | Postgres with the three databases, in namespace `ecommerce` |
| `connect` | an image of Kafka Connect with Debezium, loaded into kind, and the `KafkaConnect` resource |
| `registry` | Schema Registry, and the Avro schema registered by the inventory build |
| `services` | the images of the three services, loaded into kind, and their Deployments |
| `connectors` | the three Debezium connectors as `KafkaConnector` resources |
| `keda` | KEDA 2.21.0, and the autoscaling of inventory-service by consumer lag |

`services` must come before `connectors`: the services create the outbox tables when they start, and the connectors
need them. It ends with `ready: API at http://localhost:8081/orders`.

## Every day

```bash
k8s/setup.sh stop     # stop the cluster's containers; everything is kept
k8s/setup.sh start    # start them again; about 2 minutes until orders work
k8s/setup.sh delete   # delete the cluster and all its data
```

After `start` the services restart a few times until Kafka and Postgres are back. That is expected; `start` waits
until the order API answers and every pod is ready.

After changing a service's code, rebuild its image and restart it:

```bash
k8s/setup.sh services
kubectl -n ecommerce rollout restart deployment/order-service
```

The image tag stays `0.1`, so a new image needs the restart to be picked up.

## Try it

Place an order; the answer is `202` with status `PENDING`:

```bash
curl -s -X POST localhost:8081/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"me","sku":"laptop-pro-14","quantity":1,"unitPrice":10.00,"currency":"EUR"}'
```

Usually 3 to 10 seconds later, its status is final:

```bash
curl -s localhost:8081/orders/<orderId>
```

| Order | Expected status |
|---|---|
| `laptop-pro-14`, unit price 10.00 | `CONFIRMED` |
| `laptop-pro-14`, unit price 2500.00 | `CANCELLED`: the payment is refused above 2000 EUR, and the stock is released |
| `headphones-z` | `CANCELLED`: no stock |

**Stock runs out.** A new database has 10 `laptop-pro-14`, 3 `phone-x` and no `headphones-z`. To add more:

```bash
kubectl -n ecommerce exec postgres-0 -- psql -U postgres -d inventory_db \
  -c "UPDATE stock SET available = available + 100 WHERE sku = 'laptop-pro-14'"
```

## Addresses

Only the order API is published on the laptop. Everything else lives inside the cluster and is reached with
`kubectl`.

| What | How to reach it |
|---|---|
| Order API | http://localhost:8081/orders, through the NodePort that `k8s/kind-config.yaml` maps |
| Service health | http://localhost:8081/actuator/health |
| Kafka | `kubectl -n kafka exec ecommerce-dual-role-0 -c kafka -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| Inside the cluster | Kafka `ecommerce-kafka-bootstrap.kafka.svc:9092`, Postgres `postgres.ecommerce.svc:5432` |
| Kafka Connect | `kubectl -n kafka port-forward svc/debezium-connect-api 8083:8083`, then http://localhost:8083/connectors |
| Schema Registry | `kubectl -n kafka port-forward svc/schema-registry 8085:8081`, then http://localhost:8085/subjects |
| Postgres | `kubectl -n ecommerce exec -it postgres-0 -- psql -U postgres -d order_db` |

Prometheus, Grafana and Jaeger are not part of the cluster; tracing is turned off there.

## Looking around

```bash
kubectl get pods -A                                   # everything that runs
kubectl -n kafka get kafka,kafkanodepool,kafkatopic   # Kafka as Strimzi sees it
kubectl -n kafka get kafkaconnect,kafkaconnector      # Connect and the connectors
kubectl -n ecommerce get scaledobject,hpa             # the autoscaling of inventory
kubectl -n ecommerce logs deploy/order-service -f     # a service's log
```

The experiments in the README can be repeated from here, for example killing a broker under load:

```bash
kubectl -n kafka delete pod ecommerce-dual-role-1 --grace-period=0 --force
```

## Tests

The integration tests do not use the cluster. They start their own Postgres and Kafka in containers:

```bash
cd order-service && ./mvnw test
```

## Starting over

```bash
k8s/setup.sh delete
k8s/setup.sh all
```

## When something is wrong

| Symptom | Cause and fix |
|---|---|
| `kubectl` cannot reach the cluster | Docker Desktop or the cluster is stopped: start Docker, then `k8s/setup.sh start`. |
| Pods `Pending` or killed with `OOMKilled` | Not enough memory for Docker: raise it, and stop `docker compose`. |
| `curl localhost:8081` answers from somewhere else | A local order-service from Compose is still running: stop it. |
| An order stays `PENDING` | `kubectl -n kafka get kafkaconnector`: a connector is not ready. Orders whose messages were never sent are cancelled by the saga after about a minute. |
| A code change does not show | Rebuild with `k8s/setup.sh services` and run `rollout restart` for the Deployment. |
