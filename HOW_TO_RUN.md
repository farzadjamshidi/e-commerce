# How to run

This file describes the branch it is on. Each branch has its own copy, written for its own code.

| Branch | What it adds | Runs on |
|---|---|---|
| `main` | the order saga with orchestration, outbox and CDC, retries and DLTs, monitoring, tracing, tests, Avro | Docker Compose |
| `kubernetes` | the same system on a local Kubernetes cluster, with Strimzi and KEDA | kind |
| `sse-with-frontend` | live order status over Server-Sent Events, and a page to watch it | Docker Compose |

**This branch: `sse-with-frontend`.** Infrastructure runs in Docker Compose; the three services run as local Java
processes.

## What you need

- **Docker Desktop** with at least 6 GiB of memory (Settings → Resources). The containers use about 3.5 GiB, and the
  three services about 1.5 GiB more. Turning **Resource Saver** off there stops Docker from shutting itself down after
  some idle minutes.
- **Java 25.** `run.sh` finds it with `/usr/libexec/java_home -v 25` on macOS, or uses `JAVA_HOME`.
- **curl**, used by the scripts.
- Free ports: 3000, 4318, 5432, 8081 to 8085, 9090, 9308, 16686, 19092, 29092, 39092.

Maven is not needed: each service has its own wrapper, `mvnw`.

## First run

```bash
./run.sh setup
```

It takes a few minutes the first time, mostly downloading images and dependencies. In order, it:

1. starts the containers and waits for Postgres,
2. creates the Kafka topics,
3. builds the three jars if they are missing, and starts the services; their database migrations run at start,
4. registers the three Debezium connectors, which need the tables from step 3,
5. registers the Avro schema in Schema Registry.

It ends with `ready: open http://localhost:8081/`.

## Every day

```bash
./run.sh up          # start containers and services
./run.sh down        # stop both; all data is kept
./run.sh status      # containers, services and connectors
./run.sh logs order  # follow one service's log: order, inventory or payment
./run.sh build       # rebuild the jars after changing code, then down and up
```

Service logs and process ids are in `.run/`.

## Try it

Open **http://localhost:8081/** and click the three shortcuts:

| Shortcut | Expected |
|---|---|
| Success | `PENDING`, then `CONFIRMED` after a few seconds |
| Payment over the limit | `PENDING`, then `CANCELLED`: the payment is refused above 2000 EUR, and the stock is released |
| No stock | `PENDING`, then `CANCELLED`: `headphones-z` has no stock |

The same from a terminal:

```bash
curl -s -X POST localhost:8081/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"me","sku":"laptop-pro-14","quantity":1,"unitPrice":10.00,"currency":"EUR"}'
# copy the orderId from the answer, then follow its status:
curl -N localhost:8081/orders/<orderId>/events
```

## Addresses

| What | Address | Notes |
|---|---|---|
| Order page | http://localhost:8081/ | |
| Order API | http://localhost:8081/orders | `POST` to place, `GET /orders/{id}`, `GET /orders/{id}/events` for the stream |
| Grafana | http://localhost:3000/d/kafka-saga | lag, throughput, failures, DLTs; no login |
| Prometheus | http://localhost:9090 | queries, and alerts at `/alerts` |
| Jaeger | http://localhost:16686 | traces; pick service `order-service`, operation `http post /orders` |
| Schema Registry | http://localhost:8085/subjects | |
| Kafka Connect | http://localhost:8083/connectors | the Debezium connectors |
| Kafka | `localhost:19092,localhost:29092,localhost:39092` | for clients on this machine |
| Postgres | `localhost:5432`, user `postgres`, password `postgres` | databases `order_db`, `inventory_db`, `payment_db` |
| Service health | http://localhost:8081/actuator/health | inventory on 8082, payment on 8084 |

A Kafka command, from inside a broker:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --list
```

## Tests

Each service has integration tests against real Postgres and Kafka in containers, so Docker must be running:

```bash
cd order-service && ./mvnw test
```

`./mvnw clean test` deletes the jar in `target/`; `./run.sh up` notices a missing jar and builds the jars again.

## Starting over

```bash
./run.sh down
docker compose down -v   # deletes all data: databases, topics, schemas
./run.sh setup
```

## When something is wrong

| Symptom | Cause and fix |
|---|---|
| `Docker is not running` or containers gone | Docker Desktop stopped, often by Resource Saver. Start it, then `./run.sh up`. |
| An order stays `PENDING` | `./run.sh status`: a connector or a service is down. Orders placed while a connector is missing never reach Kafka; the saga cancels them after about a minute. |
| `... did not start, see .run/...log` | Read that log. A port in use, or Postgres not ready yet: run `./run.sh up` again. |
| A change in the code does not show | `./run.sh up` builds only when a jar is missing: `./run.sh build`, then `./run.sh down` and `./run.sh up`. |
| The page looks old after a change | Rebuild and restart, then reload the page. |
