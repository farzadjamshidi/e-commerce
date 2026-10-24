# E-commerce Order System: Kafka Practice Project

A practice project for learning Kafka and event-driven architecture in depth.
Built step by step with Java, Spring Boot and Spring for Apache Kafka.

## Scenario

A customer orders a laptop. The order is reserved, charged, then shipped.
Services, each with its own database: Order, Inventory, Payment, Shipping.

## Roadmap

1. Kafka internals (KRaft, producer, consumer, rebalancing, retention vs compaction)
2. Reliable order flow (transactional outbox, idempotent consumers, inbox table)
3. Failures and Saga (choreography vs orchestration, compensations, retry topics, DLQ)
4. Production concerns (Schema Registry, tracing, metrics, Kubernetes with Strimzi and KEDA)
5. Interview fluency (system design practice)

## Progress

### Phase 1: Kafka internals

#### Step 1: Git repository

- Initialized a Git repository with `main` as the default branch.
- Added a `.gitignore` for Java build output, IDE files, macOS files, local env files and logs.

**Why:** every step gets its own commit, so the history shows how the system grew
and any step can be revisited or compared later.

#### Step 2: Single Kafka broker in KRaft mode

`docker-compose.yml` runs one Kafka 4.1 node using the official `apache/kafka` image.

```bash
docker compose up -d

docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic orders

docker exec kafka /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 \
  describe --status
```

Key settings:

| Setting | Value | Why |
|---|---|---|
| `KAFKA_PROCESS_ROLES` | `broker,controller` | One process plays both roles ("combined mode"). Fine locally, not in production. |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@kafka:9093` | The KRaft quorum. Here it has a single voter. |
| `CLUSTER_ID` | fixed value | KRaft formats storage with a cluster ID. A fixed ID keeps restarts predictable. |
| Listeners | `PLAINTEXT` 9092, `CONTROLLER` 9093 | Clients use 9092. Controller traffic is isolated on 9093. |
| Internal topic RF | `1` | `__consumer_offsets` and `__transaction_state` default to RF 3 and would fail on one broker. |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `false` | Topics are created on purpose, with chosen partitions and RF. Typos do not create topics. |

**What KRaft changes:** ZooKeeper is gone (removed in Kafka 4.0). Cluster metadata lives in an
internal Raft log (`__cluster_metadata`) managed by controller nodes. One controller is the
active leader; the others are hot standbys with the full metadata already in memory, so
failover is fast and the partition count a cluster can handle is much higher.

**Observed:** `describe --status` shows node 1 as the only voter and the quorum leader.
`describe --topic` shows leader, replicas and ISR all equal to node 1, plus a new `Elr`
column (Eligible Leader Replicas, KIP-966), which helps pick a safe leader after ISR shrinks.

**Limitations for now:** no data volume, so `docker compose down` deletes all data.
The advertised listener is `localhost:9092`, which works for the CLI and apps on the host,
not for other containers.

#### Step 3: Console producer and consumer, keys and partitions

Produce keyed messages (`key:value` per line):

```bash
printf 'order-1:created\norder-2:created\norder-3:created\norder-1:paid\norder-2:paid\norder-1:shipped\norder-4:created\norder-5:created\n' \
  | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic orders --property parse.key=true --property key.separator=:
```

Consume with partition, offset and key:

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders --from-beginning --timeout-ms 5000 \
  --property print.key=true --property print.partition=true --property print.offset=true
```

**Observed (keyed):**

| Partition | Offset | Key | Value |
|---|---|---|---|
| 0 | 0 | order-2 | created |
| 0 | 1 | order-3 | created |
| 0 | 2 | order-2 | paid |
| 1 | 0 | order-1 | created |
| 1 | 1 | order-1 | paid |
| 1 | 2 | order-1 | shipped |
| 2 | 0 | order-4 | created |
| 2 | 1 | order-5 | created |

- The same key always lands on the same partition: `partition = murmur2(key) % numPartitions`.
- So all events of `order-1` are read in order: created, paid, shipped.
- Offsets are per partition. There is no global order across partitions.
- Several keys share a partition (`order-2` and `order-3`). Ordering holds per key, but a slow
  key can delay the others on the same partition.

**Observed (no key):** three producer runs of four messages each. Every run went entirely to one
partition, and each run picked a different partition. This is the sticky partitioner (KIP-480):
without a key, the producer fills a batch for one partition, then switches. Fewer, larger batches
mean better throughput, but there is no ordering guarantee.

**Design takeaways:**

- Use `orderId` as the key, so all events of one order stay ordered.
- Adding partitions later changes `murmur2(key) % numPartitions`, so existing keys move to other
  partitions and ordering breaks during the change. Pick the partition count up front.
- A hot key (one very busy entity) cannot be spread across partitions. That is the cost of
  per-key ordering.
