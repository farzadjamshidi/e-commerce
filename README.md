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

#### Step 4: Three-node cluster, RF 3, min.insync.replicas 2

`docker-compose.yml` now runs three nodes (`kafka-1`, `kafka-2`, `kafka-3`). Each node is both
broker and controller. Shared settings live in one YAML anchor (`x-kafka-env`).

From this step on, CLI commands run in `kafka-1` and use the internal listener:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
  --create --topic orders --partitions 3 --replication-factor 3

docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
  --describe --topic orders

docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 \
  describe --replication
```

**Listeners:** each node has three.

| Listener | Port | Used by |
|---|---|---|
| `INTERNAL` | 9092 | Broker-to-broker replication and the CLI inside containers (`kafka-N:9092`) |
| `CONTROLLER` | 9093 | KRaft quorum traffic between controllers |
| `EXTERNAL` | 19092 inside, mapped to host 19092 / 29092 / 39092 | Apps on the host (`localhost:19092,localhost:29092,localhost:39092`) |

A client connects to any bootstrap address, receives metadata with the *advertised* address of
every broker, then talks to each partition leader directly. So each listener must advertise an
address that is reachable from where the client runs. That is why containers and the host need
different listeners. Calling `localhost:19092` from inside a container finds node 1, then fails
on nodes 2 and 3, because `localhost:29092` inside a container is not `kafka-2`.

**Replication settings:**

| Setting | Value | Why |
|---|---|---|
| `default.replication.factor` | 3 | Every partition has a leader and two followers. |
| `min.insync.replicas` | 2 | With `acks=all`, a write succeeds only when at least 2 replicas have it. |
| Internal topics RF | 3 | `__consumer_offsets` and `__transaction_state` are as durable as data topics. |
| `transaction.state.log.min.isr` | 2 | Same rule for the transaction log. |

RF 3 with `min.insync.replicas=2` tolerates one broker down with no data loss and no
unavailability. With two down, producers using `acks=all` get `NotEnoughReplicas` errors:
Kafka chooses consistency over availability.

**Observed:**

```
Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
```

- Leaders are spread across brokers, one each, so write load is balanced.
- The first replica in the list is the *preferred leader*. Kafka moves leadership back to it
  after a failure (`auto.leader.rebalance.enable`).
- The KRaft quorum has 3 voters. Node 3 is the active controller; nodes 1 and 2 follow with lag 0.
  A 3-voter quorum survives one controller failure (majority = 2).
- External ports were checked from the host with `nc`. A real Kafka client on the host will be
  tested with Spring Boot in a later step.

#### Step 5: Broker failures, leader election and ISR

Brokers were killed with `docker kill` (SIGKILL), which simulates a crash rather than a clean shutdown.

```bash
docker kill kafka-2
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic orders
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --under-replicated-partitions
docker compose start kafka-2 kafka-3
docker exec kafka-1 /opt/kafka/bin/kafka-leader-election.sh --bootstrap-server kafka-1:9092 \
  --election-type preferred --all-topic-partitions
```

**A. One broker down (kafka-2 killed)**

| Partition | Before | 2 s after kill | 15 s after kill |
|---|---|---|---|
| 0 | Leader 2, ISR 2,3,1 | unchanged | Leader 3, ISR 3,1 |
| 1 | Leader 3, ISR 3,1,2 | unchanged | Leader 3, ISR 3,1 |
| 2 | Leader 1, ISR 1,2,3 | unchanged | Leader 1, ISR 1,3 |

- Nothing changed for a few seconds. The controller only notices a dead broker when its
  heartbeats stop for `broker.session.timeout.ms` (9 s by default). Then it fences the broker,
  removes it from every ISR and elects new leaders from the remaining ISR.
- All 3 partitions were reported as under-replicated. That is the main alert for this situation.
- Producing with `acks=all` still worked: ISR size 2 meets `min.insync.replicas=2`.

**B. Two brokers down (kafka-3 killed too)**

Each node is also a controller, so only 1 of 3 voters was left. The KRaft quorum lost its majority.

- Metadata froze. `describe` still showed dead kafka-3 as leader and in the ISR, because no
  active controller existed to change anything. `describe --status` on the quorum failed.
- Partition 1 (leader kafka-3, dead): producing with `acks=1` timed out. No leader, no writes.
- Partition 2 (leader kafka-1, alive, ISR still 1,3):
  - `acks=all` timed out on the client after `delivery.timeout.ms`.
  - `acks=1` succeeded on the client.
  - The end offset stayed 0 and consumers saw nothing.
  - `kafka-dump-log.sh` on kafka-1 showed **both** records in the leader's log at offsets 0 and 1.

Why: the leader appends first, then waits for the ISR. The high watermark (the last offset
replicated to the whole ISR) could not move, because dead kafka-3 was still in the ISR and no
controller could remove it. Consumers only read up to the high watermark.

**C. Recovery**

- After `docker compose start`, all partitions were back to ISR 1,2,3 in about 14 s.
- Every partition was first led by kafka-1, the only node that stayed up.
- Partition 2 now showed both `order-4 paid` records at offsets 0 and 1. The `acks=all` write that
  the client saw as **failed** was in fact stored. A client that retried it would create a duplicate.
- The `order-1 shipped` write to partition 1 was never stored.
- A preferred leader election moved partitions 0 and 1 back to brokers 2 and 3. Kafka also does this
  automatically every `leader.imbalance.check.interval.seconds` (5 minutes by default).

**Design takeaways:**

- RF 3 with `min.insync.replicas=2` survives one broker failure with no data loss and no downtime.
- A producer timeout does not mean "not written". It means "unknown". This is why producers enable
  idempotence (the broker drops retried duplicates) and consumers must still be idempotent.
- `acks=1` reports success before replication. If the leader then dies, an unreplicated record is lost.
- Combined mode couples broker failures and controller failures. Losing 2 of 3 nodes also lost the
  metadata quorum. Production clusters run 3 or 5 dedicated controllers, separate from brokers.
- Alert on under-replicated partitions, offline partitions and the active controller count.
