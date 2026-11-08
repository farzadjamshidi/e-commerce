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

#### Step 6: Spring Boot skeleton connected to Kafka

`order-service/` is a Spring Boot 4.1 project (Maven wrapper, Java 25) with only
`spring-boot-starter-kafka`. It does not produce or consume yet.

Requirements: JDK 25 (Temurin 25 was installed in `~/Library/Java/JavaVirtualMachines`).

```bash
docker compose up -d
cd order-service
JAVA_HOME=$(/usr/libexec/java_home -v 25) ./mvnw spring-boot:run
```

`application.yml` lists all three external listeners:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:19092,localhost:29092,localhost:39092
```

The bootstrap list is used only for the first connection. The client then gets the full broker
list from metadata. Listing several brokers means the app can still start when one is down.

`KafkaConnectionCheck` runs once at startup. It uses an `AdminClient` built from Spring's
`KafkaAdmin` config to log the cluster ID, the active controller and each broker.
It sends a broker-config request to every broker. Such a request goes to that broker directly,
so a success proves every advertised `EXTERNAL` listener is reachable from the host.
This is the real host test that step 4 left open.

**Observed:**

```
Cluster id: 5L6g3nShT-eMCtK--X86sw
Controller: 1
Broker 1 at localhost:19092 reachable, min.insync.replicas=2
Broker 2 at localhost:29092 reachable, min.insync.replicas=2
Broker 3 at localhost:39092 reachable, min.insync.replicas=2
Topics: [orders]
```

**Notes:**

- The app exits after the check. There is no web server and no listener yet keeping it alive.
- The client library is Kafka 4.2, the brokers are 4.1. Kafka clients and brokers are compatible
  in both directions: they negotiate API versions on connect (`ApiVersions` request).
- The generated `contextLoads` test was removed. It would need a running cluster.
  Tests will come back with Testcontainers.
- Spring Initializr wrote the version as `4.1.2.RELEASE`. Maven Central has it as `4.1.2`, so the pom was fixed.
- `docker compose stop`/`start` (or a Docker restart) keeps topics and data in the containers.
  Only `docker compose down` deletes them.

#### Step 7: Simple producer with KafkaTemplate

- `OrderEventProducer` wraps `KafkaTemplate<String, String>` and sends to `orders` with
  `orderId` as the key. The value is a plain string for now (`OrderCreated`, `OrderPaid`, `OrderShipped`).
- `DemoOrderRunner` sends the full lifecycle for three orders at startup, then calls `flush()`.

**Async send:** `kafkaTemplate.send(...)` does not block. It puts the record in the producer's
buffer and returns a `CompletableFuture<SendResult>`. A background I/O thread groups records into
batches per partition and sends them. The callback (`whenComplete`) gets the partition and offset
on success, or the exception after all retries fail. Blocking on every send (`.get()`) would cost
one network round trip per record and kill throughput.

**Observed:**

```
Sent OrderCreated for order-1003 -> partition 1 offset 2
Sent OrderPaid for order-1003 -> partition 1 offset 3
Sent OrderShipped for order-1003 -> partition 1 offset 4
Sent OrderCreated for order-1001 -> partition 0 offset 4
Sent OrderCreated for order-1002 -> partition 0 offset 5
Sent OrderPaid for order-1001 -> partition 0 offset 6
Sent OrderPaid for order-1002 -> partition 0 offset 7
Sent OrderShipped for order-1001 -> partition 0 offset 8
Sent OrderShipped for order-1002 -> partition 0 offset 9
```

- The code sent records in the order created-paid-shipped, round by round across orders.
  Callbacks for partition 1 finished first. Callbacks across partitions have no ordering.
- Within each partition, each order's events are in order. The console consumer confirmed
  the same order on the broker.
- `order-1001` and `order-1002` share partition 0, but their events interleave correctly.

**Producer defaults in Kafka 4 clients** (from the `ProducerConfig` log, no settings changed):

| Setting | Default | Meaning |
|---|---|---|
| `acks` | `-1` (`all`) | Wait for all ISR replicas |
| `enable.idempotence` | `true` | The broker drops duplicates caused by producer retries |
| `retries` | `2147483647` | Retry until `delivery.timeout.ms` (2 min) runs out |
| `max.in.flight.requests.per.connection` | `5` | Up to 5 unacknowledged batches per broker; idempotence keeps them ordered |
| `linger.ms` | `5` | Wait up to 5 ms to fill a batch (was 0 before Kafka 4.0) |
| `batch.size` | `16384` | Max batch size in bytes per partition |

The safe settings (`acks=all`, idempotence) have been the default since Kafka 3.0.
The next step makes them explicit and tests what each one does.

#### Step 8: Explicit producer settings and their effect

`application.yml` now states every producer setting that matters, even where it equals the default.
Defaults change between versions (`linger.ms` went from 0 to 5 in Kafka 4.0), and explicit config
documents intent.

```yaml
spring:
  kafka:
    producer:
      acks: all
      batch-size: 16384
      properties:
        "[enable.idempotence]": true
        "[max.in.flight.requests.per.connection]": 5
        "[linger.ms]": 5
        "[delivery.timeout.ms]": 120000
        "[request.timeout.ms]": 30000
```

The `"[...]"` brackets keep the dots inside the map key, so Spring passes `linger.ms` to the Kafka
client as one property name.

**Benchmark:** `ProducerBenchmark` runs only with the `bench` profile (`DemoOrderRunner` is now
`!bench`). It sends N records of 200 bytes to the `producer-bench` topic (3 partitions, RF 3) with
1000 distinct keys, flushes, then prints throughput and producer metrics from `kafkaTemplate.metrics()`.

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
  --create --topic producer-bench --partitions 3 --replication-factor 3

cd order-service
JAVA_HOME=$(/usr/libexec/java_home -v 25) ./mvnw package -DskipTests
java -jar target/order-service-0.0.1-SNAPSHOT.jar --spring.profiles.active=bench \
  --bench.records=200000 "--spring.kafka.producer.properties[linger.ms]=0"
```

Options: `--bench.records`, `--bench.payload-bytes`, `--bench.pause-micros` (pause between sends to simulate low load).

**A. High load: 200,000 records, as fast as possible** (second of two runs, local Docker, numbers vary)

| Config | Time (ms) | Records/s | Avg batch (bytes) | Records/request | Requests |
|---|---|---|---|---|---|
| `linger.ms=0` | 1079 | 185k | 16,039 | 73 | 2,736 |
| `linger.ms=5` (default) | 964 | 207k | 16,193 | 74 | 2,710 |
| `linger.ms=20`, `batch.size=64KB` | 441 | 454k | 65,143 | 298 | 679 |
| `acks=1`, idempotence off | 721 | 277k | 16,193 | 74 | 2,709 |

- Under high load, `linger.ms` barely matters. Batches fill to `batch.size` while earlier
  requests are still in flight, so batching happens anyway.
- `batch.size` is the lever under high load: 4x bigger batches, 4x fewer requests, about 2x throughput.
- `acks=1` was about 35% faster, because the leader answers without waiting for followers.
  Step 5 showed the price: an acknowledged record can be lost if the leader dies.

**B. Low load: 2,000 records, ~1 ms pause between sends**

| `linger.ms` | Avg batch (bytes) | Records/request | Requests | Request latency (ms) |
|---|---|---|---|---|
| 0 | 279 | 1.0 | 2,006 | 0.6 |
| 5 | 532 | 2.2 | 932 | 1.0 |
| 20 | 1,398 | 6.1 | 333 | 2.5 |

- Under low load, `linger.ms` decides batching. With 0, every record is its own request.
  With 20, there are 6x fewer requests, at the cost of up to 20 ms extra latency per record.
- Requests go per broker. Each of the 3 brokers leads one partition, so a request carries about
  a third of the records sent during the linger window.

**C. Config validation**

```
--spring.kafka.producer.acks=1   (with enable.idempotence=true)
ConfigException: Must set acks to all in order to use the idempotent producer.
```

Idempotence needs `acks=all`, `retries > 0` and `max.in.flight.requests.per.connection <= 5`.
When idempotence is set explicitly, the client refuses a conflicting config at startup instead of
silently weakening it. When it is left at the default, the client turns idempotence off quietly.
That is another reason to set it explicitly.

**Design takeaways:**

- Keep `acks=all` and idempotence for business events. Tune throughput with `batch.size`,
  `linger.ms` and compression, not by weakening durability.
- `linger.ms` trades latency for efficiency at low traffic. `batch.size` caps batches at high traffic.
- Measure with the producer's own metrics: `batch-size-avg`, `records-per-request-avg`,
  `request-latency-avg`, `record-error-rate`.

#### Step 9: Simple consumer with @KafkaListener

The consumer lives in a new service, `inventory-service/`, not in `order-service`. In the scenario,
Inventory reacts to order events, so service boundaries are right from the start. It is generated
the same way as `order-service` (Spring Boot 4.1, Java 25, `spring-boot-starter-kafka`).

```bash
cd inventory-service
JAVA_HOME=$(/usr/libexec/java_home -v 25) ./mvnw spring-boot:run
```

```yaml
spring:
  kafka:
    consumer:
      group-id: inventory-service
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

- `group-id` is the service name. Every instance of Inventory joins the same group, and Kafka splits
  partitions between them. Another service (Payment) would use its own group and get every event too.
- `auto-offset-reset: earliest` applies only when the group has **no committed offset** for a
  partition. A new group reads from the beginning instead of only new events (`latest`, the default).

`OrderEventListener` receives a `ConsumerRecord` and logs value, key, partition and offset.

**What `@KafkaListener` hides:** it is Spring's wrapper around `KafkaConsumer`, like `KafkaTemplate`
is around `KafkaProducer`. A listener container owns a consumer thread that runs the poll loop:
`poll()` up to `max.poll.records` (500), call the method once per record, commit offsets, poll again.
These container threads are non-daemon, so the app keeps running, unlike `order-service`.

**Observed:**

- On first start the group had no offsets, so it read all 26 existing records from offset 0.
  The single consumer got all three partitions: `partitions assigned: [orders-0, orders-1, orders-2]`.
- Order was kept per partition, not globally. All of partition 1 came first, then 0, then 2.
- New events from a fresh `order-service` run arrived within milliseconds.
- The topic already contains duplicates: `order-1003`'s lifecycle appears more than once because the
  demo producer ran more than once, and `order-4 paid` twice from the failed `acks=all` write in step 5.
  The consumer cannot tell them apart from new events yet.

```
TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
orders  0          22              22              0
orders  1          11              11              0
orders  2          2               2               0

GROUP              COORDINATOR   ASSIGNMENT-STRATEGY  STATE   #MEMBERS
inventory-service  kafka-3:9092  range                Stable  1
```

- Lag = log-end offset minus committed offset. Zero means the group is up to date.
- The committed offset is the **next** offset to read (22), not the last one processed (21).
- The group coordinator is kafka-3: the leader of the `__consumer_offsets` partition for
  `hash("inventory-service") % 50`.
- After a restart the consumer logged `Setting offset for partition orders-0 to the committed offset 22`
  and processed nothing again. `auto-offset-reset` was ignored because offsets existed.

**Defaults worth knowing** (from the `ConsumerConfig` log):

| Setting | Value | Meaning |
|---|---|---|
| `enable.auto.commit` | `false` | Spring sets this. The container commits, not a background timer in the client. |
| Spring `AckMode` | `BATCH` | Offsets are committed after all records from one `poll()` are processed. |
| `max.poll.records` | `500` | Records returned by one `poll()` |
| `group.protocol` | `classic` | The old rebalance protocol. KIP-848 (`consumer`) must be enabled explicitly. |
| `partition.assignment.strategy` | `Range`, `CooperativeSticky` | `range` is used. Compared in a later step. |

With `AckMode.BATCH`, a crash in the middle of a batch means the whole batch is read again after
restart. That is at-least-once delivery. The next step makes commits manual and shows it.

#### Step 10: Manual commit and crash test

`OrderEventListener` now takes an `Acknowledgment` and calls `ack.acknowledge()` after processing.
`application.yml` sets the container ack mode:

```yaml
spring:
  kafka:
    listener:
      ack-mode: manual_immediate
```

A test-only property `inventory.processing-delay-ms` (default 0) slows each record down, so the
process can be killed in the middle of a batch.

**Crash test:** produce 20 new records, start the consumer with a 300 ms delay per record,
`kill -9` it after 5 records (a crash, no shutdown hooks), restart it, and compare which
partition/offset pairs were processed in both runs.

| Ack mode | Processed before crash | Committed before crash | Processed again after restart |
|---|---|---|---|
| `BATCH` (Spring default) | 5 | 0 | **5** (all of them) |
| `MANUAL_IMMEDIATE` | 5 | 5 | **0** |

- With `BATCH`, offsets are committed only after every record from one `poll()` is processed.
  A crash in the middle loses the commit for the whole batch, so all of it is redelivered.
  With up to 500 records per poll, that can be hundreds of duplicates.
- With `MANUAL_IMMEDIATE`, each `acknowledge()` commits that record's offset synchronously.
  At most one record can be redelivered: one that was processed but not yet acknowledged.
  In this run the kill landed after the 5th ack, so there were no duplicates.
  The window still exists, so this is still at-least-once.

**Ack modes in Spring Kafka:**

| Mode | When offsets are committed |
|---|---|
| `RECORD` | After each record returns, by the container |
| `BATCH` | After all records of one poll are processed (default) |
| `TIME` / `COUNT` | After a time interval or a number of records |
| `MANUAL` | On `acknowledge()`, but queued and committed together after the poll |
| `MANUAL_IMMEDIATE` | On `acknowledge()`, committed right away |

`MANUAL` lets the code decide *which* records count as done, but commits in bulk.
`MANUAL_IMMEDIATE` gives the smallest redelivery window but sends one synchronous commit per
record, which costs throughput. It is a trade-off, not a free win.

**Order matters:** process first, commit second. Committing before processing turns a crash into
a lost message (at-most-once). Processing before committing turns a crash into a duplicate
(at-least-once). Duplicates can be handled with idempotency (Phase 2); lost messages cannot be recovered.
No commit strategy alone gives exactly-once. That needs an idempotent consumer, or committing the
offset in the same transaction as the result.

**Side finding: restart took 43-45 seconds.** After `kill -9`, the dead member stays in the group
until `session.timeout.ms` (45 s by default) passes without heartbeats. The coordinator waits for it
before finishing the rebalance, so the new instance gets no partitions until then. A clean shutdown
(SIGTERM) sends a `LeaveGroup` request and the rebalance starts at once. Static membership and the
new rebalance protocol change this; that is the next step.

#### Step 11: Rebalancing: eager, cooperative, KIP-848 and static membership

A rebalance moves partitions between members of a consumer group: when a member joins, leaves, or
dies, or when partitions are added. The experiments start three `inventory-service` instances
(A, then B, then C, 6 s apart) on the 3-partition `orders` topic and record every
`partitions revoked` / `partitions assigned` callback. Each run uses its own group.

```bash
java -jar target/inventory-service-0.0.1-SNAPSHOT.jar --spring.kafka.consumer.group-id=rb-range \
  "--spring.kafka.consumer.properties[partition.assignment.strategy]=org.apache.kafka.clients.consumer.RangeAssignor"
java -jar target/inventory-service-0.0.1-SNAPSHOT.jar --spring.kafka.consumer.group-id=rb-coop \
  "--spring.kafka.consumer.properties[partition.assignment.strategy]=org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
java -jar target/inventory-service-0.0.1-SNAPSHOT.jar --spring.kafka.consumer.group-id=rb-kip848 \
  "--spring.kafka.consumer.properties[group.protocol]=consumer"
```

**A. Range assignor (classic protocol, eager)**

| Event | A | B | C |
|---|---|---|---|
| A starts | assigned 0,1,2 | | |
| B joins | **revoked 0,1,2**, assigned 0,1 | assigned 2 | |
| C joins | **revoked 0,1**, assigned 0 | **revoked 2**, assigned 2 | assigned 1 |

Every rebalance is stop-the-world. All members give up all partitions, wait for the whole group
to sync, then get a new assignment. B gave up partition 2 only to get partition 2 back.
Locally the gap was ~30 ms, but in production every member must first finish its current batch and
rejoin, so the pause can last up to `max.poll.interval.ms`.

**B. CooperativeStickyAssignor (classic protocol, incremental)**

| Event | A | B | C |
|---|---|---|---|
| A starts | assigned 0,1,2 | | |
| B joins | revoked **2** only | assigned 2 (3 s later) | |
| C joins | revoked **1** only | untouched | assigned 1 (3 s later) |

Only the partitions that move are revoked. A kept processing 0 and 1 the whole time. It takes two
rebalance rounds: the first revokes the moving partition, the second assigns it. The ~3 s gap matches
`heartbeat.interval.ms` (3 s): members most likely learn about the second round on their next heartbeat.

**C. KIP-848 (`group.protocol=consumer`)**

| Event | A | B | C |
|---|---|---|---|
| A starts | assigned 0,1,2 | | |
| B joins | revoked **2** only | assigned 2 (1.3 s later) | |
| C joins | revoked **1** only | untouched | assigned 1 (2.7 s later) |

Also incremental, but the design is different:

- The **broker** (group coordinator) computes the assignment. In the classic protocol a client,
  the group leader, does it. The client setting is `group.remote.assignor` (`uniform` or `range`);
  `partition.assignment.strategy` is not allowed with this protocol.
- There is no group-wide `JoinGroup`/`SyncGroup` barrier. Each member reconciles on its own through
  heartbeats, so a slow member does not block the others.
- The broker must support it: `kafka-features.sh describe` shows `group.version` level 1, the
  default since Kafka 4.0. Clients still default to `classic`.

**D. Static membership: kill -9 B, restart it after 1 s** (range assignor)

| | Without `group.instance.id` | With `group.instance.id=inventory-B` |
|---|---|---|
| Time until restarted B got partitions | **45 s** | **2 s** |
| B's partition while B was down | unprocessed for 45 s | unprocessed for ~2 s |
| A and C | revoked and reassigned, partitions moved | untouched |
| B after restart | got a different partition | got the same partition back |

Without static membership, the new B is a new member. The coordinator waits for the dead member's
`session.timeout.ms` (45 s) to expire, then runs a full rebalance. With static membership, the new B
rejoins with the same instance ID. The coordinator replaces the old member, returns its cached
assignment, and does not rebalance at all. This fixes the 45 s wait seen in step 10.

The cost: a static member that leaves for good is only removed after `session.timeout.ms`, so its
partitions sit idle until then. Each instance needs a stable, unique ID across restarts, e.g. the pod
name of a Kubernetes StatefulSet. Two instances with the same ID fence each other.

**Decision for this project:** `inventory-service` now uses `group.protocol=consumer` by default.
The existing `inventory-service` group was empty, so the coordinator converted it from `Classic` to
`Consumer` on first join and kept its offsets. Static membership will be added with Kubernetes in
Phase 4, where stable pod names exist.

**Interview summary:**

| | Eager (range, round-robin) | Cooperative sticky | KIP-848 |
|---|---|---|---|
| Who assigns | Group leader (client) | Group leader (client) | Broker |
| Revokes on rebalance | All partitions | Only moving ones | Only moving ones |
| Group-wide barrier | Yes | Yes, two rounds | No |
| Slow member blocks others | Yes | Yes | No |

#### Step 12: Retention vs compaction

A partition is stored as a series of **segment** files. Only the newest one, the *active* segment,
receives writes. Both cleanup policies work on closed segments only.

```
retention-demo-0/
  00000000000000000000.log   offsets 0-4    (closed)
  00000000000000000005.log   offsets 5-9    (closed)
  00000000000000000010.log   offsets 10-14  (active)
```

The file name is the first offset in the segment. A segment closes when it reaches
`segment.bytes` (1 GiB default) or `segment.ms` (7 days default) and a new record arrives.

**A. Retention (`cleanup.policy=delete`, the default)**

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create \
  --topic retention-demo --partitions 1 --replication-factor 3 \
  --config retention.ms=30000 --config segment.ms=10000
```

Three batches of 5 records were sent 12 s apart, so each went into its own segment.

| Time | Earliest offset | Latest offset | Segments |
|---|---|---|---|
| 22:20:48, last batch sent | 0 | 15 | 0, 5, 10 |
| 22:22:53, all data older than 30 s | 0 | 15 | 0, 5, 10 |
| 22:25:40 | **15** | 15 | 0, 5, 10 marked `.deleted`, new empty 15 |

- Retention deletes whole segments, never single records. A segment goes when its newest record
  is older than `retention.ms` (or the partition is over `retention.bytes`).
- Nothing happened for about 5 minutes after the data expired. The broker only checks every
  `log.retention.check.interval.ms` (5 min). Retention is a lower bound, not an exact time.
- Even the active segment was removed: when all of it is expired, Kafka rolls a new empty segment
  so the old one can be deleted.
- Files are first renamed to `.deleted` and removed after `file.delete.delay.ms` (60 s).
- Offsets never restart. The log start offset moved to 15 and the next record will get offset 15.

**Why it matters for consumers:** if a consumer group falls behind further than retention, its
committed offset points to deleted data. The consumer gets an out-of-range error and
`auto.offset.reset` decides: `earliest` skips to the oldest remaining record, `latest` jumps to the end.
Either way, messages are lost for that group. Alert on lag long before it reaches retention.

**B. Compaction (`cleanup.policy=compact`)**

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create \
  --topic stock-levels --partitions 1 --replication-factor 3 \
  --config cleanup.policy=compact --config segment.ms=5000 \
  --config min.cleanable.dirty.ratio=0.01 --config min.compaction.lag.ms=0 \
  --config delete.retention.ms=20000
```

Stock updates per SKU, then a tombstone (a record with a `null` value) for `sku-3`.
The console producer sends `null` with `--property null.marker=NULL`.

| Offset | Before | After 1st cleaning | After 2nd cleaning |
|---|---|---|---|
| 0 | sku-1 = 10 | | |
| 1 | sku-2 = 5 | | |
| 2 | sku-1 = 9 | | |
| 3 | sku-3 = 7 | | |
| 4 | sku-1 = 8 | sku-1 = 8 | |
| 5 | sku-2 = 4 | sku-2 = 4 | sku-2 = 4 |
| 6 | sku-3 = 6 | | |
| 7 | sku-3 = null | sku-3 = null | |
| 8 | sku-4 = 1 | sku-4 = 1 | sku-4 = 1 |
| 9 | | | sku-1 = 7 |
| 10 | | | sku-2 = 3 (active segment) |

- Compaction keeps the **latest value per key** and removes older ones. Offsets do not change;
  gaps appear instead. Order of the remaining records is kept.
- A tombstone deletes a key. It stays for `delete.retention.ms` after cleaning, so consumers that
  are reading can still see the delete. It was gone after the next cleaning, and `sku-3` with it.
- The cleaner only runs when there is new "dirty" data, so the second cleaning needed new records.
- `sku-2` appears twice at the end: offset 10 is in the active segment, which is never compacted.
  A compacted topic guarantees *at least* the latest value per key, not *only* the latest.
- Settings like `min.cleanable.dirty.ratio=0.01` and `segment.ms=5000` are for the demo only.
  Production defaults (0.5 ratio, 7 day segments) avoid constant rewriting.

**When to use which:**

| | Retention (delete) | Compaction |
|---|---|---|
| Keeps | Everything, for a time or size | The latest value per key, forever |
| Records are | Events: "order 1001 was paid" | State: "stock of sku-1 is 7" |
| Typical use | Order and payment events, logs | `__consumer_offsets`, changelog topics, reference data, CDC snapshots |
| A new consumer can | Replay recent history | Rebuild the full current state |

`cleanup.policy=compact,delete` combines both: latest value per key, but nothing older than retention.

**Design takeaways:**

- Event topics like `orders` use retention. Keys matter for ordering, not for cleanup.
- Use compaction only when the key is the identity of the thing and the value is its full state.
  A compacted topic of partial updates loses information.
- Deleting with tombstones needs the consumer to handle `null` values.
- Both policies give a *minimum* guarantee in time. Neither runs at an exact moment.

Both demo topics were deleted after the test.

**Phase 1 is complete.**

### Phase 2: Reliable order flow

#### Step 1: PostgreSQL with one database per service

`docker-compose.yml` now also runs PostgreSQL 18. `infra/postgres/init.sql` creates one database
and one user per service:

| Database | Owner | Used by |
|---|---|---|
| `order_db` | `order_service` | Order service |
| `inventory_db` | `inventory_service` | Inventory service |

```bash
docker compose up -d postgres
docker exec -e PGPASSWORD=order_service postgres psql -h localhost -U order_service -d order_db
```

**Database per service:** each service owns its data, and other services can reach it only
through events or APIs. Shared tables would couple deployments, schemas and failures.
This is the reason the order flow needs Kafka and, soon, the outbox pattern: one service cannot
update another service's data in the same local transaction.

**Enforced, not just agreed:** `REVOKE CONNECT ... FROM PUBLIC` means a user can only connect to
the database it owns.

```
order_service -> order_db       OK
order_service -> inventory_db   FATAL: permission denied for database "inventory_db"
inventory_service -> order_db   FATAL: permission denied for database "order_db"
```

Locally, one PostgreSQL server hosts all databases to save resources. In production each service
would usually get its own instance (or at least its own logical database with separate credentials),
so load, upgrades and outages stay isolated.

**Details:**

- Data is in the named volume `postgres-data`. Removing and recreating the container kept a test
  row. Unlike Kafka, `docker compose down` keeps it (`down -v` deletes it).
- `init.sql` runs only when the data directory is empty, i.e. on the very first start. To rerun it,
  delete the volume. Changes to existing databases need a migration tool later (Flyway).
- PostgreSQL 18 images store data in a version-specific folder under `/var/lib/postgresql`, so the
  volume is mounted there, not at `/var/lib/postgresql/data` as in older images.
- The health check (`pg_isready`) lets other containers wait until the database accepts connections.
- Passwords are plain text in the repo because this is a local lab. Real deployments use secrets.

#### Step 2: order-service connected to order_db (JPA + Flyway)

New dependencies in `order-service/pom.xml`:

| Dependency | Purpose |
|---|---|
| `spring-boot-starter-data-jpa` | Hibernate, Spring Data repositories, HikariCP connection pool |
| `spring-boot-starter-flyway` + `flyway-database-postgresql` | Versioned SQL migrations (Spring Boot 4 split Flyway support into its own starter) |
| `postgresql` | JDBC driver |

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/order_db
    username: order_service
    password: order_service
  jpa:
    open-in-view: false
    hibernate:
      ddl-auto: validate
```

- **Flyway owns the schema.** Migrations live in `src/main/resources/db/migration` and run on startup
  in version order. Flyway records them in `flyway_schema_history`, so each runs once. A second start
  logged `Schema "public" is up to date. No migration necessary.`
- **`ddl-auto: validate`:** Hibernate never changes the schema. It only checks that entities match
  the tables and fails at startup if they do not. `update` or `create` would let Hibernate change a
  production schema silently.
- **`open-in-view: false`:** keeps database sessions out of the web layer, so lazy loading cannot
  run hidden queries during JSON rendering. Spring enables it by default and warns about it.

**`V1__create_orders.sql`:**

| Column | Type | Why |
|---|---|---|
| `id` | `UUID` | Generated by the service, not by the database, so the ID is known before the insert and can be used as the Kafka key |
| `customer_id`, `sku` | `VARCHAR(64)` | IDs owned by other services. No foreign keys across service boundaries |
| `quantity` | `INT`, `CHECK > 0` | One product line per order, enough for the scenario |
| `total_amount`, `currency` | `NUMERIC(12,2)`, `CHAR(3)` | Exact decimal for money, never floating point |
| `status` | `VARCHAR(32)` | Order state (`PENDING`, `CONFIRMED`, ...), stored as text for readability |
| `version` | `BIGINT` | Optimistic locking now, and the event version number later |
| `created_at`, `updated_at` | `TIMESTAMPTZ` | Always with time zone |

The index on `customer_id` supports "orders of a customer" queries.
There is no entity or repository yet. That comes with the first API.

#### Step 3: Place-order API (202 Accepted, status PENDING)

`order-service` is now a web app on port **8081** (`spring-boot-starter-webmvc`,
`spring-boot-starter-validation`). It no longer exits after startup.
`DemoOrderRunner` was removed: orders now come from the API. The `bench` profile turns the web
server off (`application-bench.yml`), so the producer benchmark still exits when done.

```bash
cd order-service
JAVA_HOME=$(/usr/libexec/java_home -v 25) ./mvnw spring-boot:run

curl -i -X POST localhost:8081/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"customer-42","sku":"laptop-pro-14","quantity":2,"unitPrice":1499.99,"currency":"EUR"}'

curl -i localhost:8081/orders/{orderId}
```

| Request | Response |
|---|---|
| `POST /orders` (valid) | `202 Accepted`, `Location: /orders/{id}`, body with `"status":"PENDING"` |
| `GET /orders/{id}` | `200 OK` with the current order |
| `GET /orders/{unknown}` | `404 Not Found` |
| `POST /orders` (blank customer, quantity 0, negative price, lowercase currency) | `400 Bad Request` as RFC 9457 Problem Details |

**Why 202 and not 201:** the order is stored, but it is not done. Stock must be reserved and payment
taken by other services, asynchronously. `202 Accepted` says "received, still processing".
`Location` points to where the client can check the status. Later steps push status changes to the
client (SSE or WebSocket) instead of making it poll.

**Code:**

| Class | Role |
|---|---|
| `OrderController` | `POST /orders`, `GET /orders/{id}`. Request and response are records, validated with Bean Validation |
| `OrderService` | `@Transactional` use cases: `placeOrder`, `findOrder` |
| `Order` | JPA entity. `Order.place(...)` creates a valid `PENDING` order and computes `totalAmount` |
| `OrderRepository` | Spring Data `JpaRepository<Order, UUID>` |
| `OrderStatus` | `PENDING`, `CONFIRMED`, `CANCELLED` |

**Details:**

- `ddl-auto: validate` now checks the entity against the Flyway table at startup, and it passed.
  `currency` needed `columnDefinition = "bpchar(3)"` because `CHAR(3)` is not Hibernate's default for a `String`.
- The ID is a `UUID` set in `Order.place(...)`. Normally Spring Data would treat an entity with an ID
  as existing and call `merge`, which runs a `SELECT` first. `@Version Long version` is `null` for a new
  entity, which tells Spring Data to `persist` instead. The SQL log showed one `INSERT` and no `SELECT`.
  Hibernate starts the version at 0.
- The entity is not exposed by the API. `OrderResponse` is a separate record, so the API contract
  and the table can change independently.
- `open-in-view: false` matters now that there is a web layer.

**Simplifications to revisit:**

- The client sends `unitPrice`. A real order service would take the price from a catalog or pricing
  service, never trust the client.
- A client that retries a timed-out `POST` creates a second order. The usual fix is an
  `Idempotency-Key` header stored with the order. This is the same at-least-once problem as in Kafka,
  at the API edge.
- The `400` response says "Invalid request content" without field details.
- No event is published yet. The next steps add that, reliably.

#### Step 4: Transactional outbox table

**The problem: dual write.** Placing an order must do two things: store the order in PostgreSQL and
publish `OrderCreated` to Kafka. These are two systems with no shared transaction.

| Approach | Failure | Result |
|---|---|---|
| Save order, then send to Kafka | Crash after the commit, before the send | Order exists, no event. Inventory never hears about it |
| Send to Kafka, then save order | Commit fails after the send | Event for an order that does not exist |
| Send inside `@Transactional` | Kafka send succeeds, DB rolls back | Same as above. Kafka is not part of the DB transaction |

**The outbox pattern:** write the event as a row in an `outbox_events` table **in the same database
transaction** as the order. Both commit or neither does. A separate relay reads the table and
publishes to Kafka later (next steps). The database becomes the single source of truth for
"what must be published".

`V2__create_outbox_events.sql`:

| Column | Purpose |
|---|---|
| `id` | Event ID (UUID). Consumers use it to detect duplicates |
| `position` | Identity column. The order in which the relay publishes |
| `aggregate_type`, `aggregate_id` | `Order` and the order ID. `aggregate_id` becomes the Kafka key, so one order's events stay ordered |
| `event_type` | `OrderCreated` |
| `payload` | The event as `JSONB` |
| `created_at`, `published_at` | `published_at` stays `NULL` until the relay has published it |

A partial index on `position WHERE published_at IS NULL` keeps the relay's query fast even when the
table holds millions of published rows.

`OrderService.placeOrder` now saves the order and an `OutboxEvent` in one `@Transactional` method.
`OrderCreatedEvent` is a record serialized with Jackson 3's `JsonMapper`. It carries `eventId`,
`eventType`, `occurredAt`, the order data, and `orderVersion` (the order's `version` column).

**Observed:**

```
insert into orders (...)
insert into outbox_events (...)
```

```
position | aggregate_type | aggregate_id                         | event_type   | published_at
       1 | Order          | f648986f-b3d5-4986-adc6-0e4960d51ed0 | OrderCreated | (null)
```

**Atomicity test:** `REVOKE INSERT ON outbox_events FROM order_service`, then `POST /orders`.

| | Orders | Outbox events |
|---|---|---|
| Before | 3 | 1 |
| `POST` with outbox insert failing: `500`, `permission denied for table outbox_events` | 3 | 1 |
| After `GRANT` again, `POST`: `202` | 4 | 2 |

The order insert was rolled back together with the failed outbox insert. There is no order without
its event. The error came at commit time, not at `save()`: Hibernate queues inserts and sends
them when the transaction flushes.

**Details:**

- `OutboxEvent` implements `Persistable<UUID>`. Its ID is preset and it has no `@Version`, so Spring
  Data could not tell it is new and would run a `SELECT` before every insert. A transient `isNew`
  flag, cleared after persist or load, fixes that. The SQL log shows two inserts and no select.
- `position` is filled by the database (`insertable = false, updatable = false`).
- The orders created in step 3 have no outbox events. They were placed before the outbox existed.

**Known limit of `position`:** identity values are assigned at insert, not at commit. Two concurrent
transactions can commit in the opposite order of their positions, so a relay that remembers "last
position seen" could skip a row that commits late. The relay in the next step avoids that by
selecting by `published_at IS NULL` instead of by position. Ordering per order is safe, because
events of one order are written one transaction at a time.

#### Step 5: Polling relay

`OutboxRelay` is a `@Scheduled` job in `order-service` (`@EnableScheduling` on the application class).
Every 500 ms, in one database transaction, it:

1. Tries `pg_try_advisory_xact_lock(4242001)`. If another relay holds it, it skips this round.
2. Selects up to 100 rows `WHERE published_at IS NULL ORDER BY position`.
3. Sends each to Kafka: topic from `aggregate_type` (`Order` -> `orders`), key = `aggregate_id`,
   value = `payload`, headers `eventId` and `eventType`.
4. Waits until Kafka acknowledged **every** record of the batch.
5. Sets `published_at` on those rows and commits.

If any send fails, the method throws, the transaction rolls back, and the rows are picked up again
on the next round.

```yaml
outbox:
  relay:
    poll-interval-ms: 500
    batch-size: 100
```

The relay is disabled in the `bench` profile, so the benchmark still exits.

**Why the advisory lock:** with two `order-service` instances, two relays could each take a batch.
If one order's events land in different batches, the second relay can publish the later event first.
A transaction-scoped advisory lock lets only one relay work at a time across all instances. It is
released automatically on commit or rollback, so a crashed instance cannot keep it.

**Tests:**

| Test | Result |
|---|---|
| Start with 2 unpublished rows from step 4 | Published on the first round after startup |
| Place 3 orders | Published 449-488 ms after creation (the poll interval) |
| All three brokers stopped, place 2 orders | `POST` still returned `202` in ~5 ms. Rows stayed unpublished |
| Brokers started again | Both rows published within 4 s |
| Two instances (ports 8081, 8082), 30 orders spread over both | 30 messages in Kafka, 30 distinct `eventId`s, 0 unpublished |
| Advisory lock held from `psql` for 5 s, order placed meanwhile | `published_at` stayed `NULL` while the lock was held, set right after release |

**The outage test is the point of the pattern:** Kafka being down did not stop customers from
placing orders. The events waited safely in the database.

**A bug found and fixed: duplicates after an outage.** The first version waited at most 30 s for
the batch (`get(30, SECONDS)`), while the producer keeps retrying for `delivery.timeout.ms` (120 s).
During the outage the relay gave up after 30 s and rolled back, but the records were still in the
producer's buffer. When Kafka came back, those buffered records were delivered **and** the relay
sent the rows again:

```
Offset:41  eventId:fdffe0cc...  customer-201
Offset:42  eventId:56c11e77...  customer-202
Offset:43  eventId:fdffe0cc...  customer-201   <- duplicate
Offset:44  eventId:56c11e77...  customer-202   <- duplicate
```

Producer idempotence did not help: the relay called `send()` twice, so they are two different records
to the producer. The fix: no timeout in the relay (`allOf(...).join()`). The producer guarantees every
send completes, with success or error, within `delivery.timeout.ms`. The same outage test afterwards
produced exactly one message per event.

**Still at-least-once.** If the relay crashes after Kafka acknowledged a batch but before the
`published_at` update commits, the batch is sent again. The `eventId` header and payload field let
consumers detect that. The next step makes the consumer idempotent.

**Other notes:**

- `payload` is `JSONB`, so PostgreSQL normalizes it: key order and spacing in Kafka differ from what
  Jackson wrote. The content is the same. Use `JSON` or `TEXT` if exact bytes matter (e.g. signatures).
- During a Kafka outage the relay holds one database connection and transaction open for up to
  `delivery.timeout.ms`. Fine for one relay, but worth knowing.
- Polling adds load and up to one poll interval of latency, even when nothing happens.
  CDC with Debezium (later step) reads the database log instead.
- Published rows stay in the table. A cleanup job (delete published rows older than N days) is
  needed in production.
- `inventory-service` now receives JSON events instead of plain strings.

#### Step 6: inventory-service connected to inventory_db, with an inbox table

`inventory-service` gets the same database stack as `order-service` (`spring-boot-starter-data-jpa`,
`spring-boot-starter-flyway`, `flyway-database-postgresql`, `postgresql`) and its own database:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/inventory_db
    username: inventory_service
    password: inventory_service
  jpa:
    hibernate:
      ddl-auto: validate
```

There is no `open-in-view` setting: the service has no web layer.

**`V1__create_processed_events.sql`** creates the inbox table:

| Column | Type | Why |
|---|---|---|
| `event_id` | `UUID PRIMARY KEY` | The `eventId` from the event. The primary key makes "already processed?" a single index lookup, and a second insert of the same ID fails |
| `event_type` | `VARCHAR(64)` | Useful when debugging which events were handled |
| `processed_at` | `TIMESTAMPTZ` | When it was handled. Also lets a cleanup job delete old rows later |

**Why an inbox table:** the relay delivers at-least-once, so the same event can arrive twice.
The consumer will insert the `eventId` into this table **in the same database transaction** as its
business change. A duplicate then hits the primary key and the business change is not repeated.
Offsets alone cannot do this: the commit of the offset and the database change are two separate
systems, so a crash between them always leaves a window for redelivery.

**Observed:** on first start Flyway created `flyway_schema_history` and applied V1. The consumer still
joined the group and got all three `orders` partitions. Connecting to `inventory_db` as
`order_service` fails with `permission denied for database "inventory_db"`, so the database-per-service
boundary holds.

The listener does not use the table yet. That is the next step.

#### Step 7: Idempotent consumer (inbox)

`inventory-service` now records every event it handles in `processed_events`, in the same database
transaction as the work itself.

| Class | Role |
|---|---|
| `OrderEventListener` | Reads the `eventId` and `eventType` headers set by the relay, calls the handler, then acknowledges the offset |
| `OrderEventHandler` | `@Transactional`: inserts the inbox row, and only if it is new, does the business work (a log line for now; stock reservation comes later) |
| `ProcessedEventRepository` | `INSERT ... ON CONFLICT (event_id) DO NOTHING`, which returns 1 for a new event and 0 for a duplicate |

```
poll record ──► BEGIN
                 INSERT inbox row ── 0 rows ──► duplicate: skip work
                     │ 1 row
                 business change
                COMMIT
           ──► ack offset
```

**Why `ON CONFLICT DO NOTHING`, not "check then insert":**

- `existsById` followed by `save` has a race: after a rebalance, an old and a new owner of a partition
  can both process the same record for a short time. Both see "not processed" and both do the work.
  The primary key decides atomically.
- A plain insert that hits the primary key throws, and in PostgreSQL that marks the whole transaction
  as failed. `ON CONFLICT DO NOTHING` turns the duplicate into a normal result (0 rows).

**Why the ack comes after the commit:** if the service crashes between commit and ack, Kafka
redelivers the record and the inbox skips it. The opposite order (ack first) can lose the event:
the offset moves on but the work was never committed.

**Dedup key:** the `eventId` header, so the payload does not need to be parsed just to find it.
Records without that header (none left in the topic now, older ones were removed by retention) are
logged and skipped.

**Observed:**

| Test | Result |
|---|---|
| Backlog of 42 events | 40 handled, 2 duplicates skipped. Those 2 were the real duplicates created by the relay bug in step 5 (`fdffe0cc...`, `56c11e77...`) |
| Offsets reset to earliest, whole topic replayed | 0 handled, 42 duplicates skipped, still 40 inbox rows |
| New order through the API | Handled once, inbox has 41 rows |

On startup the consumer took about 5 seconds after `partitions assigned` to deliver its first record.
A test that stopped the service after 2 seconds saw nothing. The record was not lost: it was handled on
the next start.

**Limits:**

- The inbox only protects work inside the same database transaction. A side effect outside it,
  such as an HTTP call or a Kafka send, can still happen twice and needs its own idempotency.
- The inbox grows with every event. Rows older than the topic retention can be deleted, because
  those events can no longer be redelivered.

#### Step 8: Stock reservation behind the inbox

The inbox now protects real work: `OrderCreated` reserves stock in the same transaction that records
the `eventId`.

**`V2__create_stock.sql`:** table `stock` (`sku` PK, `available`, `reserved`, both `CHECK >= 0`) with
three demo SKUs: `laptop-pro-14` (10), `phone-x` (3), `headphones-z` (0).

**Reservation is one conditional update:**

```sql
UPDATE stock SET available = available - :quantity, reserved = reserved + :quantity, updated_at = now()
WHERE sku = :sku AND available >= :quantity
```

1 row means reserved, 0 rows means not enough stock (or unknown SKU). Check and change happen in one
statement under the row lock, so two concurrent reservations cannot both read "8 left" and both
subtract. The alternatives are a read-modify-write with optimistic locking (`@Version`, retry on
conflict) or `SELECT ... FOR UPDATE`. Both need more code and more round trips.

**Event parsing:** `inventory-service` has its own `OrderCreatedEvent` record with only the fields it
needs (`orderId`, `sku`, `quantity`) and `@JsonIgnoreProperties(ignoreUnknown = true)`. It does not
share a class with `order-service`. The producer can add fields without breaking this consumer
("tolerant reader"). `spring-boot-starter-jackson` was added for the `JsonMapper`.

**Insufficient stock** is still recorded in the inbox. Retrying the event will not create stock, so it
is a business outcome, not a failure. Telling the order about it is the Saga work in Phase 3.

**Event types this service does not handle** are ignored **before** the inbox insert. The inbox means
"this work was done", not "this message was seen". If ignored events were recorded, adding a handler
for that type later and replaying the topic would skip all of them as duplicates.

**Observed:**

| Test | Result |
|---|---|
| `laptop-pro-14` x2, `phone-x` x5, `headphones-z` x1, `phone-x` x3 | Reserved, insufficient, insufficient, reserved. Stock: laptop 8/2, phone 0/3, headphones 0/0 |
| Two inventory instances, 12 parallel orders of 1 laptop (8 left) | Instance A reserved 6, instance B reserved 2 and rejected 4. `available` = 0, never negative |
| Offsets reset to earliest, whole topic replayed | 59 records, 59 duplicates skipped, 0 reservations. Stock unchanged. 57 inbox rows, because 2 of the 59 records were the step 5 relay duplicates |

**Limits:**

- `reserved` is a total per SKU. There is no row per order, so a reservation cannot be released for one
  cancelled order yet. That is needed for compensation in Phase 3.
- Demo stock is seeded by a migration. A real service would get it from a catalog or warehouse feed.

#### Step 9: PostgreSQL ready for CDC (logical replication)

The polling relay will be replaced by Debezium, which reads changes from the PostgreSQL WAL
(write-ahead log) instead of querying the table. This step only prepares the database.

| Change | Where | Why |
|---|---|---|
| `wal_level=logical` | `docker-compose.yml` (`command`) | The default `replica` WAL has only physical page changes. `logical` adds enough to decode row changes |
| User `debezium` with `REPLICATION`, `CONNECT` on `order_db` | `infra/postgres/init.sql` | A separate least-privilege user for CDC, not the service user |
| `CREATE PUBLICATION outbox_publication FOR TABLE outbox_events` | order-service `V3__publish_outbox_events.sql` | Debezium's `pgoutput` plugin streams only tables in the publication. `orders` stays private |
| `GRANT SELECT ON outbox_events TO debezium` | same migration | Needed for the initial snapshot. Nothing else in `order_db` is readable |

The publication and grant live in the order-service migration because the table owner
(`order_service`) must create them, and only after Flyway has created the table.

`init.sql` runs only when the data volume is empty, so on the existing volume the `debezium` user was
created by hand with the same SQL. Changing `wal_level` needs a server restart
(`docker compose up -d postgres` recreated the container, the data volume stayed).

**Observed:** `SHOW wal_level` returns `logical`. As `debezium`: `outbox_events` is readable,
`orders` returns `permission denied`.

A temporary slot with the `test_decoding` plugin showed what the WAL holds after one `POST /orders`:

```
918 | BEGIN 918
918 | table public.orders: INSERT: id[uuid]:'23400a87-...' customer_id:'c-cdc' sku:'phone-x' ...
918 | table public.outbox_events: INSERT: id[uuid]:'bf79e32e-...' "position":58 aggregate_type:'Order' ...
918 | COMMIT 918
919 | BEGIN 919
919 | table public.outbox_events: UPDATE: ... (the polling relay setting published_at)
919 | COMMIT 919
```

- The order and its outbox row are in **one transaction** (xid 918). CDC only sees committed
  transactions, in commit order.
- `test_decoding` ignores publications, so `orders` appears here. Debezium uses `pgoutput`, which does
  respect them.

**Replication slots are dangerous when forgotten.** A slot makes PostgreSQL keep all WAL the consumer
has not confirmed. A slot nobody reads fills the disk. The demo slot was dropped right away;
`pg_replication_slots` is empty. In production, monitor slot lag and set `max_slot_wal_keep_size`.

#### Step 10: Kafka Connect with Debezium (no connector yet)

New `connect` service in `docker-compose.yml`: `quay.io/debezium/connect:3.7.0.Final` (Debezium images
are published on quay.io, not Docker Hub). REST API on port **8083**.

**Kafka Connect** is a framework that runs inside its own worker processes, separate from the brokers. It moves data
between Kafka and other systems using **connectors**: a source connector writes into Kafka, a sink
connector reads from it. You configure connectors with JSON over REST instead of writing a producer.
Debezium is a set of source connectors (one per database) that run on Connect.

| Setting | Why |
|---|---|
| `BOOTSTRAP_SERVERS` | Internal listener names, because Connect runs inside the Docker network |
| `GROUP_ID: connect-cluster` | Workers with the same group form one Connect cluster and share the work (same group protocol idea as consumers) |
| `CONFIG_STORAGE_TOPIC`, `OFFSET_STORAGE_TOPIC`, `STATUS_STORAGE_TOPIC` | Connect is stateless itself: connector configs, source offsets (for Debezium: the WAL position) and task status live in Kafka |
| `*_REPLICATION_FACTOR: 3` | Losing these topics loses the connector state, so they get the same durability as business topics |

Connect creates these topics itself through the admin API, so `auto.create.topics.enable=false` on the
brokers does not block it.

```bash
curl -s localhost:8083/                    # worker version and Kafka cluster id
curl -s localhost:8083/connector-plugins   # installed connector classes
curl -s localhost:8083/connectors          # running connectors
```

**Observed:**

- `GET /` returned version `4.3.1` and cluster id `5L6g3nShT-eMCtK--X86sw`, the same id as our cluster.
  A 4.3 worker talks to 4.1 brokers without problems: clients and brokers negotiate API versions.
- `io.debezium.connector.postgresql.PostgresConnector` `3.7.0.Final` is installed, along with
  the other Debezium connectors and MirrorMaker 2.
- Internal topics, all RF 3, `min.insync.replicas=2`, `cleanup.policy=compact` (only the latest value per
  key matters):

| Topic | Partitions |
|---|---|
| `connect-configs` | 1 (must be 1, configs need a total order) |
| `connect-offsets` | 25 |
| `connect-status` | 5 |

- `GET /connectors` returned `[]`. The polling relay still publishes events until the next step.

#### Step 11: Debezium publishes the outbox (polling relay off)

**Relay off:** `OutboxRelay` is now `@ConditionalOnBooleanProperty("outbox.relay.enabled")`, and
`application.yml` sets `outbox.relay.enabled: false`. The class stays for comparison. Running both
would publish every event twice. The inbox would absorb that, but it is wasted work.

**Connector:** `infra/debezium/order-outbox-connector.json`, registered with:

```bash
curl -X PUT localhost:8083/connectors/order-outbox/config \
  -H 'Content-Type: application/json' -d @infra/debezium/order-outbox-connector.json
curl -s localhost:8083/connectors/order-outbox/status
```

`PUT .../config` creates or updates the connector, so it is safe to run again.

| Setting | Value | Why |
|---|---|---|
| `database.user` | `debezium` | The least-privilege user from step 9 |
| `plugin.name` | `pgoutput` | Built into PostgreSQL, respects publications |
| `publication.name`, `publication.autocreate.mode` | `outbox_publication`, `disabled` | Use the publication from the V3 migration. Debezium must not create its own (it has no rights to) |
| `slot.name` | `order_outbox_slot` | Fixed name, so it is easy to find and monitor |
| `snapshot.mode` | `no_data` | All 58 existing rows were already published by the relay. An initial snapshot would send them again |
| `key.converter`, `value.converter` | `StringConverter` | Key is the order ID, value is the JSON payload as-is, same format as the relay produced |
| `transforms.outbox.type` | `EventRouter` | Debezium's outbox SMT, described below |

**Outbox Event Router:** without it, Debezium emits a change event per row (`before`, `after`, `op`,
`source`...) to a topic named after the table (`order-db.public.outbox_events`). Consumers would then
be coupled to the outbox table layout. The SMT (single message transform) turns each **insert** into the
domain event itself:

| Router setting | Outbox column | Kafka record |
|---|---|---|
| `table.field.event.key` | `aggregate_id` | Key, so all events of one order go to one partition |
| `table.field.event.payload` | `payload` | Value |
| `table.field.event.id` | `id` | Header `id` |
| `table.fields.additional.placement` | `id`, `event_type` | Headers `eventId`, `eventType`, which `inventory-service` already reads |
| `route.by.field` + `route.topic.replacement` | `aggregate_type` | Topic `orders` |

`route.topic.replacement` is fixed to `orders` because there is only one aggregate type. The replacement
can use `${routedByValue}` but cannot change `Order` into `orders`. With more aggregates, use
`outbox.event.${routedByValue}` topic names, or store the topic name in `aggregate_type`.

**Observed:**

- Connector and task `RUNNING`. `pg_replication_slots` shows `order_outbox_slot`, plugin `pgoutput`, active.
- `order-service` started without the relay (no `OutboxRelay` log lines). `POST /orders` produced:

```
key:     fd1e75ea-965e-44fc-9a7e-fcb2b511d81c
headers: __debezium.context.*, id:6578dfec-..., eventId:6578dfec-..., eventType:OrderCreated
value:   {"sku": "laptop-pro-14", "eventId": "6578dfec-...", "orderId": "fd1e75ea-...", ...}
```

- `inventory-service` processed it with no code change (insufficient stock, laptop was sold out in step 8).
- **Connect down:** `docker stop connect`, three orders placed, all `202`. The slot became inactive
  and kept 32 kB of WAL. After `docker start connect`, all three events reached inventory about 9 seconds
  later, mostly worker startup time.
- After the catch-up the slot still showed 39 kB behind. Debezium confirms the LSN to PostgreSQL only
  when Connect commits its offsets (`offset.flush.interval.ms`, 60 s by default), not after every record.

**Delivery is still at-least-once.** If Connect crashes after sending records but before committing
offsets, it resends from the last committed position. The inbox handles that.

**Notes:**

- `published_at` stays `NULL` now: Debezium only reads. Because the WAL already carries the event,
  the outbox row could be deleted in the same transaction right after the insert. The table would then
  stay empty. That cleanup is not done yet.
- The WAL is shared by all databases on the server. If `order_db` is quiet while `inventory_db` is busy,
  the slot does not move and PostgreSQL keeps more and more WAL. Debezium's `heartbeat.interval.ms` with
  `heartbeat.action.query` (a small write in `order_db`) prevents that. Not set here yet.

#### Step 12: Debezium heartbeat and outbox cleanup

Two fixes for running the outbox on CDC: keep the replication slot moving, and stop keeping outbox rows.

##### Heartbeat: keep the replication slot moving

**Problem, reproduced:** the WAL is shared by all databases on the server, but the slot only moves when
Debezium confirms a change from `order_db`. 200 WAL messages of 100 kB written in `inventory_db`
(`pg_logical_emit_message`) left the `order_outbox_slot` **19 MB** behind, and it stayed there.
A quiet `order_db` next to a busy database means PostgreSQL keeps WAL until the disk is full.

**Fix:** Debezium writes to `order_db` itself on a timer, so there is always a recent change to confirm.

`V4__create_debezium_heartbeat.sql` (order-service): a one-row table `debezium_heartbeat (id, ts)`,
`UPDATE` granted to `debezium`, and the table added to `outbox_publication`.

Connector additions:

```json
"table.include.list": "public.outbox_events,public.debezium_heartbeat",
"heartbeat.interval.ms": "10000",
"heartbeat.action.query": "UPDATE debezium_heartbeat SET ts = now() WHERE id = 1",
"transforms.outbox.predicate": "isOutbox",
"predicates": "isOutbox",
"predicates.isOutbox.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
"predicates.isOutbox.pattern": "order-db\\.public\\.outbox_events"
```

- The predicate runs the `EventRouter` only on outbox records. Heartbeat table changes have no
  outbox columns and would fail in the router.
- Two topics are created by hand (auto-create is off), RF 3, 1 partition, compacted, because only the
  latest value matters: `__debezium-heartbeat.order-db` (Debezium's own heartbeat records) and
  `order-db.public.debezium_heartbeat` (the table changes).

**First attempt failed.** I first dropped heartbeat records with the `Filter` SMT, so no extra topics
were needed. Debezium received the updates (logs showed `messageType=UPDATE`, txId 933 → 947), but
`connect-offsets` got no new entry and the slot did not move. Kafka Connect does not commit the
source offset of a record that a transform dropped, so Debezium never confirmed the new LSN to
PostgreSQL. The heartbeat records must actually be written to Kafka.

**Observed with the fix:** the same 19 MB of noise was released within 90 seconds (680 bytes behind).
The delay is up to `heartbeat.interval.ms` plus Connect's `offset.flush.interval.ms` (60 s).

**Monitor in production:**

```sql
SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn))
FROM pg_replication_slots;
```

`max_slot_wal_keep_size` is the last line of defence: PostgreSQL invalidates a slot that falls further
behind, which protects the disk but forces a new snapshot of the connector.

##### Outbox rows deleted in the same transaction

With CDC the event is read from the WAL, not from the table. So `OrderService` deletes the outbox row
right after inserting it, in the same transaction:

```java
OutboxEvent outboxEvent = outboxEventRepository.saveAndFlush(OutboxEvent.create(...));
if (!relayEnabled) {
    outboxEventRepository.delete(outboxEvent);
}
```

- `saveAndFlush`, not `save`, and this was tested. `OutboxEvent` clears its `isNew` flag in
  `@PostPersist`, which Hibernate fires only when the `INSERT` actually runs (at flush).
  `SimpleJpaRepository.delete()` returns without doing anything for an entity that reports `isNew()`.
  With a plain `save`, the SQL log showed the `INSERT` and no `DELETE`, and the row stayed in the table.
  The event was still published, so nothing broke visibly. Only the cleanup silently failed.
- The `EventRouter` publishes the insert and ignores the delete (`tombstones.on.delete` is `false`).
- Only when the polling relay is off. The relay reads the table, so its rows must stay.
- The table stays empty, so no cleanup job, no table bloat, no index on unpublished rows to maintain.

**Observed:** two orders, `orders` topic offsets went from 159 to 161, inventory reserved both
(`headphones-z` stock set to 5 by hand for the test), `outbox_events` row count did not change.
The 62 old rows from the relay era were then deleted once by hand (`DELETE 62`). No messages were
produced and Connect logged no warnings.

**Trade-off:** the table no longer shows what was sent. The `orders` topic, the Connect offsets and
Debezium metrics are now the only record. Some teams keep rows for a few days for debugging and
delete them with a scheduled job instead.

### Phase 2 summary

The order flow is now reliable end to end:

```
POST /orders ─► order_db: orders + outbox INSERT (+ DELETE) in one transaction ─► 202
                   │ WAL
                   ▼
             Debezium (slot, pgoutput, EventRouter) ─► topic orders (key = orderId)
                                                          │
inventory_db: inbox INSERT + stock UPDATE in one transaction ◄─ inventory-service ─► ack
```

- No dual write: the order and its event commit together.
- At-least-once delivery from Debezium, effectively-once effect through the inbox.
- Kafka or Connect outages delay events but do not lose them and do not fail the API.

Pushing order status to clients (SSE) moves to Phase 3. The status only changes once the Saga
confirms or cancels orders.

### Phase 3: Saga and failures

An order crosses several services, each with its own database. There is no shared transaction, so a
failure in a later step must be undone by **compensating** earlier steps (release stock, cancel order).

**Two ways to coordinate a Saga:**

| | Choreography | Orchestration |
|---|---|---|
| Control | No coordinator. Each service reacts to the others' events | An orchestrator (in `order-service`) sends commands and waits for replies |
| The flow | Spread across services | Written in one place, one state row per order |
| Coupling | Low, services only know events | The orchestrator knows every participant |
| Compensation | Each service decides when to undo | The orchestrator decides explicitly |
| Debugging "why is this order stuck?" | Needs tracing across services | One table lookup |
| Good for | Short flows, 2–3 steps | Longer flows, many failure paths |

A concrete problem with choreography here: `payment-service` needs the amount, which
`InventoryReserved` does not carry. Either inventory adds it (inventory now depends on payment's needs),
or payment also reads `OrderCreated` and keeps a copy. Those are different topics, and Kafka gives no
ordering across topics, so `InventoryReserved` can arrive before payment has seen `OrderCreated`.
With orchestration the `ChargePayment` command simply contains the amount.

**Plan:** build both and compare.

1. Shared parts on `main`: `payment-service`, a reservations table, an outbox in inventory and payment.
   Every service that publishes needs an outbox, and every service that consumes needs an inbox,
   whichever style is used.
2. Branch `saga-choreography` and branch `saga-orchestration` from that point.
3. Retry topics and DLT on the chosen branch, then merge it to `main` for Phase 4.

#### Step 1: payment-service connected to payment_db

A new Spring Boot service, `payment-service` (package `com.ecommerce.payment`), with the same stack
as `inventory-service`: Kafka, Jackson, JPA, Flyway, PostgreSQL driver. No web layer.

- `infra/postgres/init.sql`: user `payment_service`, database `payment_db`, `REVOKE CONNECT FROM PUBLIC`.
  Applied by hand on the existing volume, because `init.sql` only runs on an empty one.
- `V1__create_processed_events.sql`: the same inbox table as inventory, ready for its first listener.
- Consumer group `payment-service`, `group.protocol: consumer`, `manual_immediate` acks, the same settings
  as inventory.

**Observed:** Flyway applied V1 to `payment_db`. Connecting to `payment_db` as `inventory_service`
fails with `permission denied for database "payment_db"`. The service exits right after startup:
it has no listener yet, so nothing keeps it running.

#### Step 2: Reservations per order in inventory

`stock.reserved` was only a total per SKU. To release the stock of one cancelled order (the Saga's
compensation), inventory must know what each order holds.

`V3__create_reservations.sql`:

| Column | Why |
|---|---|
| `order_id UUID PRIMARY KEY` | One reservation per order (one product line per order). A second reservation for the same order fails on the key, a second safety net after the inbox |
| `sku` → `stock (sku)` | Foreign key inside the same service's database is fine |
| `quantity` | How much to give back on release |
| `status` | `RESERVED` now, `RELEASED` after compensation. The row is kept, not deleted, for history |

The handler inserts the row in the same transaction as the inbox row and the stock update, only when the
conditional `UPDATE stock` succeeded.

**Existing data:** the 15 units reserved before this step had no owning row and could never be released.
The migration returns them to `available` (`available = available + reserved, reserved = 0`).
From now on this invariant holds:

```sql
stock.reserved = SUM(reservations.quantity) WHERE status = 'RESERVED'   -- per SKU
```

**Observed:** on startup inventory processed two `phone-x` events that had waited in the topic (from the
`save` vs `saveAndFlush` test), then four new orders: laptop x2 reserved, phone x5 insufficient,
headphones x1 reserved, `unknown-sku` insufficient (the conditional update matches no row, so the
foreign key is never reached). Four reservation rows, and `reserved` matched the row sum for every SKU.

#### Step 3: Outbox and Debezium connector for inventory

Inventory will publish Saga results (reserved, rejected, released), so it gets the same CDC outbox as
order-service. Which events it writes is decided on the Saga branches.

**`V4__create_outbox_and_heartbeat.sql`:**

- `outbox_events` without `position` and `published_at`. Inventory never had a polling relay, so rows
  are deleted in the same transaction and those columns have no use.
- `debezium_heartbeat`, the same one-row table as in `order_db`. With two slots on one server, each
  slot needs its own heartbeat.
- `outbox_publication` on both tables (publication names are per database, so the same name is fine),
  and grants for `debezium`.

`init.sql` grants `debezium` `CONNECT` on `inventory_db` (applied by hand on the existing volume).

**`OutboxWriter`:**

```java
@Transactional(propagation = Propagation.MANDATORY)
void write(UUID eventId, String aggregateType, String aggregateId, String eventType, Object event)
```

- `MANDATORY` throws if there is no surrounding transaction. Writing an event outside the business
  transaction would bring back the dual write problem, so the code refuses it.
- Native `INSERT` and `DELETE`, no JPA entity lifecycle. This avoids the `isNew`/`delete()` trap from
  Phase 2 step 12. `OutboxEvent` is mapped only so `ddl-auto: validate` checks the table.
- Not called yet. The Saga branches call it.

**`infra/debezium/inventory-outbox-connector.json`:** a copy of the order connector with
`database.dbname: inventory_db`, `topic.prefix: inventory-db`, `slot.name: inventory_outbox_slot`
(slot names are unique per server), topic `inventory`.

**Topics created by hand** (RF 3): `inventory` (3 partitions, key = order ID), and compacted, 1 partition:
`__debezium-heartbeat.inventory-db`, `inventory-db.public.debezium_heartbeat`.

```bash
curl -X PUT localhost:8083/connectors/inventory-outbox/config \
  -H 'Content-Type: application/json' -d @infra/debezium/inventory-outbox-connector.json
```

**Observed:**

- Flyway applied V4. Connector and task `RUNNING`. `pg_replication_slots` shows both slots active.
- I registered the connector before the two heartbeat topics existed (my topic script passed the options in one shell variable, which zsh does not split into words, and a `grep` hid the error).
  The task stayed `RUNNING`, and heartbeats started arriving once the topics were created.
- A smoke test as `inventory_service`, `INSERT` and `DELETE` of one outbox row in one transaction,
  produced on `inventory`:

```
headers: id:1111...,eventId:1111...,eventType:OutboxSmokeTest
key:     order-test-1
value:   {"hello": "cdc"}
```

  `outbox_events` stayed empty. The smoke test message stays in the topic until retention removes it,
  so consumers of `inventory` must ignore unknown event types (the inventory handler already does).

#### Step 4: Outbox and Debezium connector for payment

The same setup as inventory (step 3), for `payment_db`:

- `V2__create_outbox_and_heartbeat.sql`: `outbox_events`, `debezium_heartbeat`, `outbox_publication`,
  grants for `debezium`.
- `OutboxWriter`, `OutboxEventRepository`, `OutboxEvent`: copies of inventory's classes. Each service owns
  its code; there is no shared library between services.
- `infra/debezium/payment-outbox-connector.json`: `payment_db`, `topic.prefix: payment-db`,
  `slot.name: payment_outbox_slot`, topic `payments`.
- `init.sql`: `GRANT CONNECT ON DATABASE payment_db TO debezium`.
- Topics (RF 3): `payments` (3 partitions), `__debezium-heartbeat.payment-db` and
  `payment-db.public.debezium_heartbeat` (1 partition, compacted).

**Observed:** Flyway applied V2. Three connectors `RUNNING`, three slots active, heartbeats arriving.
The smoke test (`INSERT` + `DELETE` in one transaction as `payment_service`) arrived on `payments` with key
`order-test-2` and headers `eventId`, `eventType`. The table stayed empty.

**State of the shared part:**

| Service | Database | Publishes to | Inbox | Connector |
|---|---|---|---|---|
| order-service | `order_db` | `orders` | – | `order-outbox` |
| inventory-service | `inventory_db` | `inventory` | `processed_events` | `inventory-outbox` |
| payment-service | `payment_db` | `payments` | `processed_events` | `payment-outbox` |

This is the branch point. Both Saga styles start from here.

#### Orchestration 0: a fresh environment per branch (branch `saga-orchestration`)

Both saga branches use the same Postgres and Kafka, but their schemas grow apart. Choreography had
already applied order `V5`/`V6` and payment `V3`/`V4`. A different `V5` on this branch would fail
Flyway's checksum check. The data of one branch would also blur the comparison. So each branch starts
from an empty environment.

The Kafka containers have no volume: topics, offsets and connector configs live in the containers. The
Postgres data lives in the `postgres-data` volume. `docker compose down -v` removes both.

`infra/setup.sh` rebuilds what used to be typed by hand. It is safe to run again: `--if-not-exists` for
topics, and `PUT` for connectors.

```bash
docker compose down -v && docker compose up -d
infra/setup.sh topics        # orders, inventory, payments + Debezium heartbeat topics
# start each service once: Flyway creates the tables and the publications
infra/setup.sh connectors    # fails if a publication does not exist yet
```

The connectors come after the services because `publication.autocreate.mode` is `disabled`. Each
publication is created by that service's own migration. Run this whenever you switch saga branch.

**Observed:** containers and volume removed. 13 topics after `setup.sh topics` (3 business, 6
heartbeat, 4 internal). Migrations order v4, inventory v4, payment v2. 3 connectors `RUNNING`, 3 slots
active. A test order was reserved by inventory, as on `main`.

#### Orchestration 1: one outbox, several topics (branch `saga-orchestration`)

The orchestrator sends commands to other services, so order-service must now write to more than one
topic. Commands get their own topics:

| Topic | Direction | Content |
|---|---|---|
| `orders` | order → anyone | events about orders |
| `inventory-commands` | orchestrator → inventory | `ReserveStock`, `ReleaseStock` |
| `payment-commands` | orchestrator → payment | `ChargePayment` |
| `inventory`, `payments` | service → orchestrator | replies |

**Why separate command topics.** An event says "this happened" and the producer does not care who reads
it. A command says "do this" and has exactly one receiver. Separate topics keep that visible, and they
let the two be sized and secured differently.

Changes:

- `V5__add_outbox_destination.sql`: `outbox_events.destination`. The default only fills existing rows and
  is then dropped, so every new write must choose a topic.
- Connector: `route.by.field: destination`, `route.topic.replacement: ${routedByValue}`. The topic now
  comes from the row. It was fixed to `orders` before.
- `OutboxEvent.create` takes the destination. `OutboxRelay` reads it too, so its aggregate-to-topic map is gone.
- `infra/setup.sh topics` also creates the two command topics (3 partitions, RF 3).

**Why a new column and not `aggregate_type`.** Routing by `aggregate_type` would work only if its value
were the topic name. But a `ReserveStock` command is about an `Order` and goes to `inventory-commands`.
The two values answer different questions.

**Deployment order matters.** The migration must run before the new connector config. The other way
round, the router looks for a column that does not exist yet.

**Risk: a wrong destination.** Topic auto-creation is off. A row with an unknown topic would likely fail the
connector task, and that stops every later event of this database, not just the bad one. Not tested. The
destinations are constants in the code for this reason.

**Observed:**

- Migration v5 applied, connector config updated with `PUT`, task `RUNNING`.
- A real order → `OrderCreated` on `orders`, as before.
- Two test rows (inserted and deleted in one transaction): one arrived on `inventory-commands`, one on
  `payment-commands`, each with its `aggregate_id` as the key. `outbox_events` empty. The two
  `OutboxSmokeTest` records stay on the command topics, so their consumers must ignore unknown types.

**Observed by accident: CDC delivers at least once.** `orders` already had 2 records for 1 order, with the
same `eventId`. The first was written when the order was placed. The second was written 15 s after Connect
came back from a Docker restart: Connect resumed from its last stored offset and read the same WAL
entry again. Why the stored offset was behind that entry was not investigated. This is why the inbox
exists: the outbox gives at-least-once delivery, and only the consumer's deduplication makes it
effectively once.

#### Orchestration 2: the saga starts with `placeOrder` (branch `saga-orchestration`)

The orchestrator lives inside order-service. The order is what the saga is about, so the saga state and
the order status share one database and one transaction, and there is no extra service to deploy. A
larger system might use a separate service or an engine such as Temporal or Camunda.

- `V6__create_order_sagas.sql`: `order_sagas`, one row per order, with the step the saga is waiting for.
  This is the orchestrator's memory. In choreography this knowledge was spread over three databases.
- `OrderSagaOrchestrator.start(order)`: writes the saga row in state `RESERVING_STOCK` and the
  `ReserveStock` command. `Propagation.MANDATORY`, so it can only join `placeOrder`'s transaction.
- `ReserveStockCommand`: `orderId`, `sku`, `quantity`. Key = order ID, topic `inventory-commands`.
- `OutboxWriter`: extracted from `OrderService`, because two classes now write to the outbox. Inventory
  and payment already had one.

One transaction now holds the order row, `OrderCreated`, the saga row and `ReserveStock`.

**`ReserveStock` carries no amount.** In choreography, `InventoryReserved` carried `customerId`,
`totalAmount` and `currency` only for payment. Here the orchestrator owns the order data and will build
`ChargePayment` itself. Inventory receives only what inventory needs.

**`OrderCreated` stays.** It is a fact about the order and costs little. It no longer drives the saga. Until
the next step, inventory still reacts to it, because the `ReserveStock` consumer does not exist yet.

**Observed:**

- Migration v6 applied. A new order: saga row `RESERVING_STOCK`, 1 record on `orders`, 1 `ReserveStock` on
  `inventory-commands` with the order ID as key and `eventId` and `eventType` headers.
- Forced failure: a temporary `CHECK` constraint made the saga insert fail, after `OrderCreated` had
  already been flushed. API returned 500. The `orders` table, `order_sagas`, both topics and the outbox
  did not change. Debezium reads only committed changes, so the flushed row never left the database.
  The constraint was removed after the test.
- The 2 orders from before this step have no saga row. They were test orders and are left as they are.

#### Orchestration 3: inventory obeys `ReserveStock` (branch `saga-orchestration`)

Inventory no longer reacts to `OrderCreated`. It reads `inventory-commands` and answers on `inventory`.

- `StockCommandListener` + `StockCommandHandler` replace `OrderEventListener` + `OrderEventHandler`.
  `OrderCreatedEvent` is gone from inventory. It no longer knows that orders exist as events.
- In one transaction: inbox row, stock update, reservation row, reply through `OutboxWriter`.
- The command ID is used as the inbox key, so a repeated command has no effect.

| Reply | When | Payload |
|---|---|---|
| `StockReserved` | Conditional `UPDATE stock` matched | `orderId` |
| `StockRejected` | No row matched | `orderId`, `reason: INSUFFICIENT_STOCK` |

**Compare with choreography.** `InventoryReserved` carried `sku`, `quantity`, `customerId`, `totalAmount`
and `currency`, because the next service read it directly. `StockReserved` carries only `orderId`.
The reply goes to the orchestrator, which already has the order. Inventory does not know that payment
exists, or what comes next.

**A limit to remember.** If the orchestrator ever resends `ReserveStock`, it must reuse the same command
ID. With a new ID the inbox would not catch it: stock would be taken, and the insert into `reservations`
would then fail on the primary key, roll back, and be retried as a failing message. The orchestrator
does not resend yet.

**Observed:**

- On start: `OutboxSmokeTest` ignored, the waiting `ReserveStock` of step 2 → 2 laptops reserved,
  `StockReserved` on `inventory`.
- Live: phone x5 → `StockRejected` with `INSUFFICIENT_STOCK`, phone x1 → `StockReserved`, both keyed by
  order ID. Stock laptop 7/3, phone 2/1. `OrderCreated` of these orders reserved nothing.
- Replay of `inventory-commands` from earliest: 3 duplicates, 1 ignored, stock unchanged, `inventory`
  topic still 3 records.
- Nobody reads the replies yet, so these sagas stay in `RESERVING_STOCK`. That is the next step.

#### Orchestration 4: the orchestrator reads inventory's reply (branch `saga-orchestration`)

Order-service now also consumes.

- `V7__create_processed_events.sql`: the inbox. Consumer config: group `order-service`,
  `group.protocol: consumer`, `manual_immediate` acks.
- `InventoryReplyListener` + `InventoryReplyHandler` on `inventory`: type check, inbox, then the
  orchestrator, all in one transaction.
- `StockReply(orderId, reason)`: one tolerant record for both replies.

| Reply | Saga state | Order status | Next |
|---|---|---|---|
| `StockReserved` | `RESERVING_STOCK` → `CHARGING_PAYMENT` | stays `PENDING` | `ChargePayment` on `payment-commands` |
| `StockRejected` | `RESERVING_STOCK` → `CANCELLED` | `CANCELLED` | nothing to compensate |

**The state machine is one conditional update.** `OrderSagaRepository.transition(orderId, from, to)`
runs `UPDATE ... WHERE order_id = :orderId AND state = :from`. A reply is valid only in the state that
expects it. 0 rows means a late or repeated reply: it is logged and changes nothing. So all flow logic
now sits in one class, `OrderSagaOrchestrator`: reading it shows the whole saga. In choreography that
required reading three services.

**`ChargePayment` is built from the order.** `customerId`, `amount` and `currency` come from the `orders`
table, in the same database. Inventory did not carry them.

**Observed:**

- On start, migration v7 applied, and the 3 waiting replies were handled:
  - c-saga (2999.98) and c-ok (699.00): `CHARGING_PAYMENT`, order `PENDING`, `ChargePayment` with the
    right amount, keyed by order ID.
  - c-nostock: saga and order `CANCELLED`, order `version` 1.
- Replay of `inventory` from earliest: 2 duplicate `StockReserved`, 1 duplicate `StockRejected`.
- Replay again after deleting the inbox rows: 3 times "saga is not in RESERVING_STOCK, reply ignored".
  `payment-commands` stayed at 3 records (1 smoke test + 2 commands), statuses and versions unchanged. The
  state guard works without the inbox.

#### Orchestration 5: payment obeys `ChargePayment` (branch `saga-orchestration`)

Payment reads `payment-commands` and answers on `payments`.

- `V3__create_payments.sql`: `payments`, one row per order, the same table as on the choreography branch
  at this point.
- `PaymentCommandListener` + `PaymentCommandHandler`: type check, inbox, payment row, reply, in one
  transaction.
- `payment.max-amount: 2000`: the same simulated gateway rule as in choreography.
- `insertIfAbsent` (`ON CONFLICT DO NOTHING`): a second charge for one order changes nothing and sends
  no reply.

| Reply | When | Payload |
|---|---|---|
| `PaymentCompleted` | `amount <= payment.max-amount` | `orderId` |
| `PaymentFailed` | Over the limit | `orderId`, `reason: AMOUNT_LIMIT_EXCEEDED` |

**Compare with choreography.** Payment reads one topic and knows only `ChargePayment`. On the choreography
branch it read `inventory` and `orders`, and had to know what `InventoryReserved` and `OrderCancelled` mean.
Here it does not know that inventory exists. The replies also lost `amount` and `currency`: the
orchestrator already has them.

**Observed:**

- On start: migration v3 applied, `OutboxSmokeTest` ignored, the 2 waiting commands handled:
  - c-ok, 699.00 → `COMPLETED`, `PaymentCompleted`
  - c-saga, 2999.98 → `FAILED`, `PaymentFailed` with `AMOUNT_LIMIT_EXCEEDED`
- Both replies keyed by order ID. `outbox_events` empty.
- Replay of `payment-commands`: 2 duplicates, 1 ignored.
- Replay again after deleting the inbox rows: 2 times "already has a payment, not charged", 1 ignored.
  `payments` table and topic unchanged. The primary key works as a guard without the inbox.
- Nobody reads these replies yet, so both sagas stay in `CHARGING_PAYMENT`. c-saga's 2 laptops also stay
  reserved. That is the next step.

#### Orchestration 6: the saga ends, with compensation (branch `saga-orchestration`)

Order-service now also reads `payments`. Inventory also handles `ReleaseStock`. The full state machine:

```
RESERVING_STOCK ──StockRejected──────────────────────────────► CANCELLED
      │ StockReserved / ChargePayment
      ▼
CHARGING_PAYMENT ──PaymentCompleted──► COMPLETED (order CONFIRMED)
      │ PaymentFailed / ReleaseStock
      ▼
RELEASING_STOCK ──StockReleased──────────────────────────────► CANCELLED
```

- `PaymentReplyListener` + `PaymentReplyHandler`, the same shape as the inventory replies.
- `OrderSagaOrchestrator`: `onPaymentCompleted`, `onPaymentFailed`, `onStockReleased`.
- Inventory: `ReleaseStockCommand`, `StockReleasedEvent`, and the same one-statement `release` as on the
  choreography branch.

**The order stays `PENDING` until the stock is back.** `CANCELLED` then means the compensation has
finished. A customer who sees it can trust that nothing is held. It costs one more round trip. In
choreography, order-service cancelled on `PaymentFailed` while inventory released on its own, so the order
could be `CANCELLED` while the stock was still reserved.

**`ReleaseStock` always gets a reply, even with nothing to release.** The orchestrator is waiting. In
choreography a compensation could stay silent, because nobody waited for it. Here silence would leave the
saga in `RELEASING_STOCK`. This is a new duty that commands bring.

**Observed:**

- Waiting replies: c-ok → `COMPLETED` / `CONFIRMED`. c-saga → `RELEASING_STOCK`, 2 laptops released, then
  `CANCELLED`. Laptop stock 7/3 → 9/1.
- Live: laptop x1 → `CONFIRMED`. Laptop x2 (2999.98) → `CANCELLED`, 7 steps in 3.1 s:
  started, reserved, charging, failed, releasing, released, cancelled. Laptop 9/1 → 8/2.
  `stock.reserved = SUM(RESERVED rows)` held for all SKUs.
- `inventory-commands` replayed after deleting the `ReleaseStock` inbox rows: 2 releases with 0
  reservations, stock unchanged. Each sent a new `StockReleased` with a new event ID, so the inbox could
  not catch it. The orchestrator ignored both: "saga is not in RELEASING_STOCK".
- Order-service replayed both reply topics: duplicates only. The fingerprint (md5) of sagas and orders
  was the same before and after.

#### Orchestration 7: timeout while reserving and releasing (branch `saga-orchestration`)

`SagaTimeout`, a `@Scheduled` job (every 5 s), finds sagas that stayed in one state longer than
`order.saga-timeout.after` (30 s). `updated_at` is reset by every transition, so the time is per step, not
per saga. `V8__index_sagas_by_state.sql` adds an index on `(state, updated_at)`.

| Stuck in | Meaning | Action |
|---|---|---|
| `RESERVING_STOCK` | Unknown if inventory reserved | move to `RELEASING_STOCK`, send `ReleaseStock` |
| `RELEASING_STOCK` | Compensation not confirmed | send `ReleaseStock` again |
| `CHARGING_PAYMENT` | Unknown if payment charged | **not handled yet**, next step |

**Compare with choreography.** There the timeout knew only "this order is `PENDING`", so one
`OrderCancelled` went to everybody, and each service had to work out what it meant. Here the saga row says
which step is unanswered, so the orchestrator sends only the compensation that step needs.

**"Release before reserve" cannot happen.** `ReleaseStock` has the same key and topic as `ReserveStock`, so
it is in the same partition, behind it. A slow inventory reserves first and releases second. The late
`StockReserved` finds the saga in `RELEASING_STOCK` and is ignored. In choreography payment read two
topics, so the same race needed a marker row and an upsert. One command topic per service, keyed by order,
removes that case.

**A compensation cannot give up.** Moving forward can stop at any point, but once compensation has started
it must finish, or stock stays locked. So `ReleaseStock` is sent again each timeout until `StockReleased`
arrives. It uses a new command ID on purpose: the inbox would swallow the old ID and no reply would
come. This is safe because `release` is idempotent by `status = 'RESERVED'`. The Orchestration 3 note about
reusing the command ID applies to `ReserveStock`, which is not idempotent by itself.

**Costs:**

- While inventory is down, one more `ReleaseStock` is written per timeout, and the order stays `PENDING`.
  There is no limit or backoff yet. A real system would alert after N attempts.
- Two order-service instances could both resend. That would be harmless because of the idempotent release.
  Not tested.

**Observed** (inventory down, phone x1):

- 20 s: `RESERVING_STOCK`. 40 s: `RELEASING_STOCK`. `ReleaseStock` was sent again at 60 s and 90 s.
- `inventory-commands`: `ReserveStock`, then the `ReleaseStock`s, all in partition 0, offsets in order.
- Inventory started: within about 75 ms it reserved 1, released 1, then released 0 twice.
- Orchestrator: late `StockReserved` ignored, first `StockReleased` → `CANCELLED`, the other two ignored.
- Phone stock 2/1 before and after. Order `CANCELLED`, version 1. Reservation `RELEASED`.

#### Orchestration 8: timeout while charging, refund (branch `saga-orchestration`)

The last unanswered state. A new state `REFUNDING_PAYMENT` and a new command `RefundPayment`:

```
CHARGING_PAYMENT ──timeout / RefundPayment──► REFUNDING_PAYMENT ──PaymentRefunded / ReleaseStock──► RELEASING_STOCK
```

- Order-service: `SagaTimeout` also checks `CHARGING_PAYMENT` and `REFUNDING_PAYMENT`. The orchestrator gets
  `onPaymentTimeout`, `onRefundTimeout` and `onPaymentRefunded`.
- Payment: `RefundPayment` runs `UPDATE payments SET status = 'REFUNDED' ... WHERE status = 'COMPLETED'` and
  always replies `PaymentRefunded`. `V4__add_payment_updated_at.sql` adds `updated_at`.

**Compensations run in reverse order.** The money first, then the stock. That is the rule from the original
Sagas paper: undo the last step first. In choreography both reacted to `OrderCancelled` at the same time, with
no order between them.

**No marker row, no upsert.** On the choreography branch, the cancel could reach payment before the charge,
so payment needed a `CANCELLED` marker row, nullable columns and `INSERT ... ON CONFLICT DO UPDATE`. Here
`RefundPayment` is behind `ChargePayment` in one partition, so the charge has always been handled first. Refund
is a plain conditional `UPDATE`.

**A timed-out charge is an unknown, not a failure.** The orchestrator cannot tell if the money moved. So it does
not skip to `ReleaseStock`. It asks for a refund and waits. A late `PaymentCompleted` is ignored, and the
refund has already undone it. No saga has isolation: for a moment the customer really was charged.

**Observed** (payment down, two orders: 1499.99 and 2999.98):

- 15 s: both `CHARGING_PAYMENT`. 40 s: both `REFUNDING_PAYMENT`. `RefundPayment` sent again until payment
  came back. Laptop stock 5/5 while waiting.
- In each partition `ChargePayment` was ahead of the `RefundPayment`s.
- Payment started:
  - 1499.99: charged, refunded (1 row), then 2 refunds of 0 rows. Row `REFUNDED`.
  - 2999.98: failed, then 3 refunds of 0 rows. Row `FAILED`.
- Orchestrator: the late `PaymentCompleted` and `PaymentFailed` ignored, first `PaymentRefunded` → `RELEASING_STOCK`,
  the others ignored, `StockReleased` → `CANCELLED`.
- Laptop stock back to 8/2, `stock.reserved = SUM(RESERVED rows)` for all SKUs. No saga left in a
  middle state: 6 `CANCELLED`, 2 `COMPLETED`.

#### Orchestration summary and comparison (branch `saga-orchestration`)

**Messages on this branch:**

| Topic | Message | From | To |
|---|---|---|---|
| `orders` | `OrderCreated` | order | nobody in the saga |
| `inventory-commands` | `ReserveStock`, `ReleaseStock` | orchestrator | inventory |
| `payment-commands` | `ChargePayment`, `RefundPayment` | orchestrator | payment |
| `inventory` | `StockReserved`, `StockRejected`, `StockReleased` | inventory | orchestrator |
| `payments` | `PaymentCompleted`, `PaymentFailed`, `PaymentRefunded` | payment | orchestrator |

**Measured comparison.** Both branches were run by the same script, each from an empty environment, on one
laptop. 20 orders per path, one at a time, after 3 warm-up orders. Time is `orders.updated_at - created_at`.
For the payment failure, "stock back" uses the reservation's `updated_at`.

| Path | Choreography median (min-max) | Orchestration median (min-max) | Messages per order (chor / orch) |
|---|---|---|---|
| Success, order `CONFIRMED` | 1412 ms (1315-3864) | 1920 ms (1843-2450) | 3 / 5 |
| No stock, order `CANCELLED` | 915 ms (723-1926) | 942 ms (823-1911) | 2 / 3 |
| Payment failed, order `CANCELLED` | 1675 ms (1250-2204) | 3275 ms (2708-3789) | 3 / 7 |
| Payment failed, stock back | 1677 ms (1256-2180) | 2773 ms (2208-3286) | same run |

Message counts include `OrderCreated`, which no saga step reads on this branch.

**Reading the numbers.** The time follows the number of hops through the outbox and CDC, at roughly 0.5 s each:
2 hops in both for no stock, 3 vs 4 for success, 3 vs 6 for payment failure. The 0.5 s is an estimate from
these numbers. Its cause was not measured. On a laptop with an idle system these numbers show the shape,
not production latency, and throughput was not measured at all.

In orchestration the failed order becomes `CANCELLED` 0.5 s after the stock is back. In choreography both
happen at about the same time, but independently, so nothing guarantees the stock is back first.

**Scenarios:**

| # | Scenario | Choreography | Orchestration |
|---|---|---|---|
| 1 | laptop x1 | `CONFIRMED` | `CONFIRMED` |
| 2 | no stock | `CANCELLED` | `CANCELLED` |
| 3 | payment limit | `CANCELLED`, stock released | `CANCELLED`, stock released |
| 4 | a service down | cancelled by timeout, stock released, money refunded | the same, with compensation in reverse order |
| 5 | cancel before / after the charge | both needed code: marker row and upsert | "before" cannot happen: same partition |
| 6 | replay | no change, per topic | no change, all topics at once |

Scenarios 1-3 were run by the script on both branches. Scenarios 4 and 5 come from Choreography 5-7 and
Orchestration 7-8. Scenario 6 on this branch: all consumer groups reset to earliest, 276 duplicates, no
orchestrator action, and fingerprints of orders, stock, reservations and payments identical. On the
choreography branch it was done per topic in each step.

**Structure:**

| | Choreography | Orchestration |
|---|---|---|
| Topics | 3 | 5 |
| Message types | 6 | 11 |
| Topics a service reads | 2 each (full mesh) | 1 for inventory and payment, 2 for order |
| Lines added since the branch point (Java + SQL) | 869 | 1182 |
| of which in order-service | 268 | 689 |
| Where the flow can be read | three services | `OrderSagaOrchestrator`, 155 lines |
| Where the state lives | three tables in three databases | `order_sagas`, one row per order |
| Timeout | one `OrderCancelled` to everybody | per step, only the needed compensation |
| Compensation order | none | reverse, explicit |
| Adding shipping (thought test) | changes order, payment, inventory | changes the orchestrator, plus the new service |
| New failure point | none | order-service: if it is down, every saga stops moving |

Orchestration also added duties that did not exist before: every command needs a reply, even with nothing to do,
and a compensation must be resent until it is confirmed.

**Known limitations, on both branches:**

- The timeout job has no per-saga error handling. On this branch an exception in `stuck.forEach(action)` stops
  that run. Because the rollback keeps `updated_at`, the same saga is first again on the next run, so one bad saga can block
  all timeouts. Choreography's `PendingOrderTimeout` has the same loop shape. Found by reading the code, not tested.
- Resending has no limit and no backoff, and there is no state for a compensation that can never succeed.
- Refunds are status changes, not gateway calls.
- A consumer gives up after 10 quick attempts and drops the message. That is what retry topics and the DLT are for.

**When to choose which:**

- Choreography: few steps, little compensation, teams that must stay independent, and lowest latency.
- Orchestration: many steps, real compensation, timeouts, and a need to answer "where is this order".

#### Failure handling 1: blocking retry and a DLT for inventory

Orchestration won the comparison and is merged to `main`. Until now a failing record was retried 10 times with no
delay and then dropped: only a log line remained.

**The plan said retry topics. That would break this design.** With retry topics, a failed record moves to another
topic and the next record of the same key passes it. `ReleaseStock` could then run before `ReserveStock`, and the
late reservation would never be released. The saga relies on order inside a partition. So:

| | Blocking retry (used here) | Retry topics |
|---|---|---|
| Order per key | kept | lost |
| While retrying | the partition waits | the partition keeps moving |
| Fits | ordered commands | independent messages |

`KafkaErrorConfig` defines a `DefaultErrorHandler`, which Spring Boot gives to the listener container:

- Exponential backoff: attempts after 1 s, 2 s and 4 s, so 4 attempts and about 7 s in total.
- `DeadLetterPublishingRecoverer`: after the last attempt the record goes to `inventory-commands-dlt`, in the same
  partition number, with `kafka_dlt-exception-*` headers. `infra/setup.sh topics` creates that topic with the same
  3 partitions.
- `JacksonException` and `IllegalArgumentException` are not retried. A broken payload stays broken, and a retry
  would only block the partition.
- `setCommitRecovered(true)`: with manual acks nobody else commits a record that went to the DLT. If it is the last
  in its partition, a restart would send it to the DLT again.

**The DLT and the timeout work together.** A command in the DLT gets no reply, so the saga stays where it is, and
the timeout compensates it. Nobody has to act for the customer to get an answer.

**Replaying from the DLT is dangerous.** Taking a `ReserveStock` from the DLT back to its topic after the saga was
cancelled would reserve stock for a dead order, and nothing would release it. A DLT is for looking at, then deciding
case by case. It is not an automatic second queue.

**Cost:** a failing record blocks its partition for about 7 s, so other orders on that partition wait too.

**Observed:**

- Broken payload (`not-json`): on the DLT at once, no retry.
- Failure that heals: a temporary `CHECK` made the reservation insert fail, and it was removed after 3 s. A retry
  succeeded, the order was `CONFIRMED`, nothing on the DLT.
- Failure that does not heal: attempts at 0, 1, 3 and 7 s, then the DLT. Each failed attempt rolled back, so stock
  stayed 9/1. The saga stayed `RESERVING_STOCK`, and the timeout cancelled it: `CANCELLED`, stock 9/1. The test
  constraint was removed.
- `setCommitRecovered`: a broken record as the last in its partition. Offset 3 of 3, lag 0, with no later ack to
  move it. A plain restart earlier left the DLT unchanged.

## Failure handling 2: retry and a DLT for payment and order-service

The same `KafkaErrorConfig` now runs in payment and order-service, with the same backoff and the same non-retryable
exceptions. `infra/setup.sh topics` creates the missing DLTs, each with 3 partitions:

| Service | Listens to | DLT |
|---|---|---|
| inventory | `inventory-commands` | `inventory-commands-dlt` |
| payment | `payment-commands` | `payment-commands-dlt` |
| order-service | `inventory` | `inventory-dlt` |
| order-service | `payments` | `payments-dlt` |

Order-service has two listeners and one error handler. The DLT name comes from the record's topic, so one bean is
enough. The class is copied three times, because the services share no code. That is a deliberate choice: a shared
library would couple their releases for 20 lines.

**A lost reply is worse than a lost command.** A command in the DLT means the work was not done. A reply in the DLT
means the work was done and order-service does not know. `PaymentCompleted` in `payments-dlt` means the customer paid
for an order that looks unpaid. The timeout covers this too, because it does not need to know what happened: it
refunds, and the refund works on what the payment row says.

| Lost message | State after the DLT | What the timeout did |
|---|---|---|
| `ChargePayment` | no payment, stock reserved | sent a refund with nothing to refund, released the stock |
| `StockReserved` | stock reserved, saga still `RESERVING_STOCK` | released the stock |
| `PaymentCompleted` | money taken, saga still `CHARGING_PAYMENT` | refunded, released the stock |

So a retry and a DLT protect the partition, and the saga timeout protects the customer.

**Known limitation:** a `RefundPayment` or `ReleaseStock` that always fails goes to the DLT, the timeout sends a new
one, and that goes to the DLT too, every 30 s or so. The order stays open and the DLT grows. This was found by reading
the code, not tested. It is the same gap as the timeout job limitation above, and it needs an alert (Phase 4).

**Observed:** temporary `CHECK` constraints caused the failures, and all of them were removed afterwards.

- Broken payloads on `payment-commands`, `inventory` and `payments`, and a bad UUID in the `eventId` header: each
  reached its DLT in under 3 s including tool startup, well inside the 7 s of retries.
- Failures that heal, in payment and in order-service: attempts at 0 and 1 s failed, the one at 3 s succeeded. The
  order was `CONFIRMED`, nothing on a DLT.
- Failures that do not heal, in both services: attempts at 0, 1, 3 and 7 s, then the DLT.
- The three lost messages in the table above: each order ended `CANCELLED` with its stock back. For the lost
  `PaymentCompleted` the payment went from `COMPLETED` to `REFUNDED`.
- `setCommitRecovered`: a broken record as the last in its partition, on all three topics. Lag 0.

## Monitoring 1: metrics from inventory

Phase 4 starts with visibility, because nobody sees a DLT fill up today. This step only exposes raw metrics from one
service. Prometheus and Grafana come later.

- `spring-boot-starter-actuator` and `micrometer-registry-prometheus` add `/actuator/prometheus`, the text format
  Prometheus reads.
- `spring-boot-starter-webmvc` was added too, because inventory had no HTTP server. It now listens on port 8082.
- Only `health` and `prometheus` are exposed. Actuator endpoints can leak internals, so the list is explicit.

Spring Boot connects the Kafka clients to Micrometer by itself, so no Kafka metric needed code:

| Metric | Meaning |
|---|---|
| `kafka_consumer_fetch_manager_records_consumed_total` | records read by this consumer |
| `kafka_consumer_fetch_manager_records_lag_max` | the largest lag this consumer sees |
| `kafka_consumer_coordinator_rebalance_total` | rebalances so far |
| `spring_kafka_listener_seconds` | listener calls and their time, with `result` and `exception` tags |
| `spring_kafka_template_seconds` | sends through `KafkaTemplate`, which includes DLT publishing |

**Client metrics are not enough for lag.** This one comes from inside the consumer. If the service dies, the metric
disappears exactly when lag starts to grow. Lag has to be measured from outside too, from the offsets on the broker.

**Observed:**

- `/actuator/health` returned `UP`, and `/actuator/prometheus` returned 97 metric families starting with `kafka_`.
- One order: `records_consumed_total` 1 (its `ReserveStock`), listener `success` 1, `lag_max` 0.
- One `not-json` command: listener `failure` 1 with `exception="StreamReadException"`, and `spring_kafka_template_seconds`
  `success` 1, which is the DLT publish. No HTTP or Kafka code was written for either.
- The per-partition `records_lag` metric did not appear, only `lag_max`. The reason was not investigated.

## Monitoring 2: metrics from payment and order-service

The same metrics now come from every service, so Prometheus has three targets to read:

| Service | Port | Note |
|---|---|---|
| order-service | 8081 | already had an HTTP server |
| inventory | 8082 | |
| payment | 8084 | got an HTTP server in this step; 8083 is Kafka Connect |

**Order-service has two consumers.** Each `@KafkaListener` gets its own Kafka consumer, so metrics appear twice, with
`client_id` `consumer-order-service-1` and `-2`. Queries must sum over them.

**Listener names are poor.** A failure shows up as `KafkaListenerEndpointContainer#1-0`, which does not say the topic.
An `id` on `@KafkaListener` would fix that. It was left for the step that builds the dashboard.

**Observed:**

- `/actuator/health` was `UP` on all three ports, and the saga finished `CONFIRMED` with metrics on.
- A `not-json` reply on `payments` and a `not-json` command on `payment-commands`: each service showed listener
  `failure` 1 with `StreamReadException`, and `spring_kafka_template_seconds` `success` 1 for the DLT publish.
- `/actuator/env` returned 404, so only the listed endpoints are open.

## Monitoring 3: Prometheus reads the services

Prometheus runs in Docker Compose on port 9090, with its config in `infra/prometheus/prometheus.yml`. It reads
`/actuator/prometheus` from each service every 5 s. Each service is its own `job`, so queries can group by service.

The services run on the host, so the targets use `host.docker.internal`, the name a Docker Desktop container uses to
reach the host. Prometheus has no volume, so its history is lost when the container is recreated. That is fine for
practice.

**Pull, not push.** Prometheus goes to the services. The result is a free health signal: every scrape writes an `up`
series, 1 or 0. A service that pushes its metrics and then dies simply goes quiet.

Queries used in this step:

```promql
up
sum by (job) (rate(kafka_consumer_fetch_manager_records_consumed_total[1m]))
max by (job) (kafka_consumer_fetch_manager_records_lag_max)
```

`rate` is the per-second speed of a counter over the window, and it handles a counter going back to 0 after a restart.
A raw counter only grows, so it says little alone.

**Observed:**

- All three targets were `up`.
- Five orders for `phone-x` with 3 in stock: 3 confirmed and 2 rejected. The counters matched the saga exactly:
  inventory 5 commands, payment 3 commands, order-service 8 replies.
- Payment killed: `up` went to 0 for `payment-service`, and its lag series disappeared from the query. It did not show
  0 or a growing number. So an alert on this metric would stay silent exactly when the consumer is gone. This confirms
  the warning from Monitoring 1, and the next step measures lag from outside.

## Monitoring 4: lag from outside the consumer

`kafka-exporter` runs in Docker Compose on port 9308, and Prometheus reads it as a fourth target. It asks the brokers
for two numbers per partition, the end of the log and the group's committed offset. Lag is the difference:

```text
lag = log end offset - committed offset
```

Neither number needs the consumer to be alive, so this lag keeps working when the service is down. That closes the gap
found in Monitoring 3.

```promql
sum by (consumergroup, topic) (kafka_consumergroup_lag)
```

**Lag counts records, not time.** A lag of 1000 is nothing on a fast topic and a disaster on a slow one. It also says
nothing about a consumer that is alive and stuck on a record, until the records behind it pile up. Its trend matters
more than one number.

**Do not trust `kafka_consumergroup_members` here.** It showed 0 for all three services while they were running, and 1
for `connect-cluster`. The likely reason is that our consumers use `group.protocol=consumer`, the new protocol, and
the exporter asks with the old describe call. This reason was not verified. Lag is correct, because it only needs
offsets. So "no members" cannot be an alert here, and `up` covers that case.

**Observed**, with payment killed and 3 orders sent:

| Signal | Payment dead | After restart |
|---|---|---|
| Exporter lag on `payment-commands` | 3 (1 on partition 0, 2 on partition 2) | 0 |
| Client `lag_max` for payment | series gone | not checked |
| `up` for payment | 0 | not checked |
| `kafka_consumergroup_members` | 0 | 0 (wrong) |

Payment came back and caught up before the 30 s saga timeout, so all three orders ended `CONFIRMED`. The commands had
waited in Kafka, which is the point of a log: an outage of the consumer becomes lag, not lost work.

## Monitoring 5: alert rules

`infra/prometheus/alerts.yml` holds three rules. They can be seen at `http://localhost:9090/alerts`.

| Alert | Condition | Answers |
|---|---|---|
| `TargetDown` | `up == 0` for 15 s | is a service dead? |
| `ConsumerLag` | total lag above 0 for 30 s | is a group falling behind? |
| `DeadLetter` | a DLT grew in the last 5 min | does a human need to decide something? |

The three cover each other. Lag alone misses a dead service with no traffic, and `up` alone misses a live service that
is stuck. A record on a DLT shows neither, because the consumer moved on and lag is 0.

**States:** an alert is `pending` while its `for` is running, then `firing`. `for` stops a short spike from waking
anyone. A DLT record is not a spike, so `DeadLetter` has no `for`.

**`evaluation_interval` matters.** The first test used the default of 1 minute, so the alerts were still `pending`
80 s after the failure, even after the service came back. Detection time is scrape interval, plus evaluation interval,
plus `for`. It is now 5 s.

**`DeadLetter` is an event, not a state.** It clears after 5 min, while the record is still on the DLT. So it says
"something new arrived", not "the DLT is empty". A real team would track each record until someone decides.

**Firing is not notifying.** Prometheus only decides that an alert is active. Sending it to Slack or a pager, grouping
and silencing is the job of Alertmanager, which is not installed here.

`ConsumerLag` uses a threshold of 0 because this project has almost no traffic. On a busy topic a healthy lag is above
0 all the time, so the threshold has to come from that topic's normal behavior.

**Observed:**

- All healthy: no `TargetDown` or `ConsumerLag`.
- Payment killed and one order sent: `TargetDown` was `firing` at about 25 s, `ConsumerLag` was `firing` by 49 s. The
  saga timeout passed while payment was down, so after the restart the order was `CANCELLED` and the payment
  `REFUNDED`. Both alerts cleared within seconds of the restart catching up.
- A `not-json` command on `payment-commands`: `DeadLetter` for `payment-commands-dlt` was `firing` within 9 s. It
  cleared at about 305 s with the record still on the DLT.
- `ConsumerLag` did not fire for a short lag: on `inventory-commands` it went `pending` and cleared within seconds.

## Monitoring 6: a Grafana dashboard

Grafana runs in Docker Compose on port 3000. The dashboard is at `http://localhost:3000/d/kafka-saga`, and viewing it
needs no login. Grafana stores no metrics, it only asks Prometheus.

Everything is provisioned from files in `infra/grafana`: the data source and the dashboard JSON. A recreated container
gets the same dashboard, and a change to it shows up in a Git diff. Edits made in the UI are not kept in the repo.

| Panel | Question |
|---|---|
| Targets | who is up? |
| Lag by group and topic | who is falling behind? |
| Records consumed per second | is work moving? |
| Listener failures since service start | what is failing, and with which exception? |
| New DLT records in 5 min | what needs a human? |

**Listener names.** Each `@KafkaListener` now has an `id`, so a failure says `payment-replies-0` and not
`KafkaListenerEndpointContainer#1-0`. `idIsGroup = false` is required. By default the `id` replaces the group id, which
would create a new group with no committed offsets. With `auto-offset-reset: earliest` that group would read every
topic from the start. After the change the groups were still `inventory-service`, `order-service` and
`payment-service`.

**`rate` missed single failures.** The first version of the failures panel used `rate`, and it showed 0 after three
real failures. A counter with these labels does not exist until the first failure, so Prometheus first sees it at 1
and has no earlier sample to compare with. The panel now shows the raw count. `DeadLetter` does not have this problem,
because the end offset of a DLT partition exists before any record arrives.

**Observed:** seven orders and three broken messages. All panels showed data, checked with queries through Grafana and
with a screenshot of the page. All targets `UP`, a short lag of 1 on `payments`, DLT panels at 1 on the three topics
used, and failures named `inventory-replies-0`, `payment-replies-0` and `stock-commands-0`.

## Tracing 1: Jaeger and a first trace

Metrics describe the whole system. A trace describes one request: a tree of spans that share a `traceId`. Each span is
a piece of work with a start, an end and a parent.

Jaeger runs in Docker Compose. Its UI is at `http://localhost:16686`, and it receives spans over OTLP on port 4318. It
keeps them in memory, so a restart empties it. Services depend on OpenTelemetry and not on Jaeger, so the backend can
change without touching their code. Traces are pushed, unlike Prometheus metrics.

Order-service got `spring-boot-starter-opentelemetry`, and the rest is configuration:

- `management.opentelemetry.tracing.export.otlp.endpoint` is where spans go.
- `management.tracing.sampling.probability: 1.0` keeps every request. The default is 0.1. Sampling is a cost decision:
  a busy system cannot keep every trace, so it keeps a share of them.

**Observed:**

- One `POST /orders`: a trace with one span, `http post /orders`, 87 ms.
- The saga finished, but its Kafka replies created no spans. Listeners are not observed yet, so this trace ends when
  the HTTP response is sent, while the order is still `PENDING`.
- Noise: Prometheus scrapes (`http get /actuator/prometheus`) and the saga timeout job
  (`task sagaTimeout.checkStuckSagas`) each create a trace. At sampling 1.0 they outnumber real orders.

## Tracing 2: all services, and the chain breaks

Inventory and payment got the same OpenTelemetry starter and configuration. `spring.kafka.listener.observation-enabled`
is now on in all three services, because spring-kafka has the instrumentation but leaves it off by default.

Every listener call now creates a span. The result shows the problem this phase has to solve.

**One order, five separate traces.** A successful order produced `http post /orders`, then one trace per consumed
message: `inventory-commands process`, `inventory process`, `payment-commands process`, `payments process`. Each has its
own `traceId` and a single span.

**Why it breaks.** The listener looks for a `traceparent` header and finds none, so it starts a new trace. No one set
that header, because the sender did not use `KafkaTemplate`. It wrote an outbox row, and Debezium produced the Kafka
record in another process, where the context of the original thread does not exist.

So instrumenting every service is not enough. A trace is only as continuous as the path its context can travel, and the
next step carries it through the outbox.

## Tracing 3: the trace crosses the outbox

The context now travels the same way as the message: through the database.

1. `OutboxWriter` reads the current span and asks the `Propagator` for its W3C `traceparent`.
2. The value goes into a new `outbox_events.traceparent` column, in the same transaction as the business change.
3. Debezium copies the column to a Kafka header, with `traceparent:header:traceparent` in
   `table.fields.additional.placement`.
4. The listener observation finds the header and makes its span a child of the sender's span. No listener code changed.

This is the outbox rule again: whatever must travel with a message has to be committed with it. The `Propagator`
is used instead of building the string by hand, so the format stays in one place.

**Deployment order matters.** The migration has to run before the connector config changes. `infra/setup.sh connectors`
updates a running connector, so no reset was needed.

**A row with no context still works.** A row with `traceparent` null reached order-service, was ignored as expected,
and nothing went to a DLT. So rows written before the migration are harmless. They simply start a new trace.

**What the trace shows now.** Successful order, one trace, each span a child of the one before it:

| Start | Service | Span | Time |
|---|---|---|---|
| 0 ms | order | `http post /orders` | 83 ms |
| 904 ms | inventory | `inventory-commands process` | 67 ms |
| 1509 ms | order | `inventory process` | 30 ms |
| 1859 ms | payment | `payment-commands process` | 71 ms |
| 2424 ms | order | `payments process` | 21 ms |

Work inside the services was about 270 ms of 2.4 s. The rest is the gaps between spans, which hold the transaction
commit, Debezium and Kafka. This is one order, and the gaps cannot be split further, because Debezium has no span here.

**Observed:** a failed payment gave one trace of 7 spans, with the compensation (`ReleaseStock` and `StockReleased`) in
the same tree. Before this step it would have been 7 traces. The timeout path is not covered: the scheduler has no
order context, which is the next step.

## Tracing 4: the timeout joins the order's trace

`SagaTimeout` runs from a scheduler, so no request stands behind it. Its work used to land in the job's own trace, and
the compensation of an order was invisible from that order's trace.

The fix is the same idea as the outbox: the context is kept in the database, this time for later.

- `order_sagas.traceparent` stores the context of the request that started the saga.
- `TraceContext.current()` reads the context, and `OutboxWriter` uses it too.
- `TraceContext.runIn()` restores a stored context: it extracts the parent, starts a span and makes it current.
- `SagaTimeout` runs each stuck saga in a `saga timeout <state>` span. Everything written inside it, like `ReleaseStock`,
  takes that span as its parent.

Stored context needs the span to be started by hand, because no framework restores it. The parent span ended long ago.
That is fine: a parent is only an id, and it does not need to be alive.

**Observed**, with inventory stopped so that the saga could not move, and restarted after the timeout:

```text
http post /orders                      0 ms
  inventory-commands process      50068 ms    ReserveStock, after the restart
    inventory process             50734 ms    StockReserved, a late reply
  saga timeout RESERVING_STOCK    33725 ms
    inventory-commands process    50130 ms    ReleaseStock
      inventory process           50772 ms    StockReleased
```

One trace shows the whole incident: the request, a 33 s wait, the timeout, and the late command with its compensation
as two branches. The order ended `CANCELLED` and the reservation `RELEASED`.

A saga with `traceparent` null, like one started before the migration, still worked. Its order ended `CANCELLED`, and
its compensation stayed in the scheduler's trace, which is the old behavior.

## Testing 1: inventory against real Postgres and Kafka

Testcontainers starts Docker containers from inside a test and removes them afterwards. The tests so far were shell
scripts against Docker Compose. This is the first automated one: `StockCommandIntegrationTest` in inventory-service.

- `postgres:18.1` and `apache/kafka:4.1.0`, the same images as Docker Compose, so behavior like `ON CONFLICT` and
  partition order is the real one.
- `@ServiceConnection` points the datasource and Kafka properties at the containers. No URL is written by hand.
- `create-debezium-role.sql` creates the `debezium` role, because a migration grants it access to the outbox. In
  Compose that role comes from `infra/postgres/init.sql`.
- Each test inserts its own SKU, so the tests do not depend on each other or on the demo stock.
- Debezium is not in the test, so the reply messages are not checked. The outbox row is deleted in the same
  transaction, so the test can only see the stock, the reservation and the Kafka offsets.

`mvn test` needs Docker running. Startup is about 4 s for the containers, and the whole class takes about 11 s.

**Tests:**

- `reserveStockCommandReservesStock`: one `ReserveStock`, then the reservation is `RESERVED` and stock is `3/2`.
- `sameCommandTwiceReservesOnce`: the same command id twice. The group commits both records, the DLT stays empty, and
  stock is `3/2`.

**The first duplicate test proved nothing.** It only checked the stock. With the inbox check removed from the code, it
still passed: the second insert hit the primary key of `reservations` and rolled back, so the stock stayed right. The
difference is in how the copy ends. With the inbox it is skipped. Without it, it fails, is retried for about 7 s and
lands on the DLT. The test now checks that, and with the inbox removed it fails with `expected: 0L but was: 1L` on the
DLT. A test is only trusted after it has been seen failing.

The listener metrics were tried first as the signal, but they stayed at 0 in the test context. The cause was not
investigated. The test asks Kafka for the committed offsets instead, which needs no test configuration.

## Testing 2: payment

`PaymentCommandIntegrationTest` uses the same setup as inventory: real Postgres and Kafka, the `debezium` role, and the
committed offsets plus an empty DLT as the sign that every command ended cleanly.

| Test | Commands, same key | Expected |
|---|---|---|
| `chargeCompletesPayment` | `ChargePayment` | `COMPLETED` |
| `refundAfterChargeRefundsPayment` | `ChargePayment`, `RefundPayment` | `REFUNDED` |
| `refundWithoutChargeChangesNothing` | `RefundPayment` | no payment row, nothing on the DLT |

The third case is real: after a timeout the saga sends a refund without knowing whether a charge happened.

**Each test was seen failing.** With the refund turned into a no-op, the second test failed with `COMPLETED`. With a
refund that throws when there is nothing to refund, the third test failed with one record on the DLT.

**Found: a charge after its refund takes the money.** As an experiment, `RefundPayment` was sent before
`ChargePayment` for the same order. The refund found nothing, and the charge then created a `COMPLETED` payment. That
order is already cancelled, so nothing would ever refund it.

In the normal flow this cannot happen. Both commands have the order id as key and go to one partition, and the saga
sends the refund only after the charge. But it can happen when a `ChargePayment` is replayed by hand from
`payment-commands-dlt` after the saga gave up. The experiment was not kept as a test, because it would assert the bug.

## Fix: a charge after its refund takes no money

Written test first. `chargeAfterRefundTakesNoMoney` sends `RefundPayment` and then `ChargePayment` for one order. It
failed with `COMPLETED`, which was the bug from Testing 2, and passes after this change.

- `voided_orders` (migration `V6`) holds every order the saga asked to refund.
- A refund always inserts its order there, whether or not it found a payment. A refund means the order is over.
- A charge first checks that table. For a voided order it takes no money and sends no reply.

**Why a separate table.** A refund carries only the order id, and `payments` needs an amount, a currency and a
customer. Making those columns nullable would weaken every real payment row to cover one rare case.

**Why no reply.** The saga that sent the refund has already finished, so nobody waits for this charge. A reply would
also be harmless, since the state machine ignores a reply it does not expect.

**Why the check cannot race.** Check and insert are two statements. They are safe only because both commands of an
order have the same key and are handled one after the other in one partition. The comment in the code says so.

## Testing 3: order-service and the saga's two guards

`OrderSagaIntegrationTest` places an order through `OrderService`, then plays inventory and payment by sending their
replies to `inventory` and `payments`. Postgres and Kafka are real, Debezium is not.

**Seeing what the service sends.** The outbox row is deleted in the transaction that writes it, so the test cannot read
it. The test adds a trigger that copies every outbox insert into a `sent_messages` table. The trigger exists only in the
test database.

| Test | Replies | Expected |
|---|---|---|
| `sagaCompletesWhenStockAndPaymentSucceed` | `StockReserved`, `PaymentCompleted` | `ReserveStock`, `ChargePayment`; saga `COMPLETED`, order `CONFIRMED` |
| `repeatedAndLateRepliesChangeNothing` | `StockReserved` twice with one id, once with a new id; `PaymentCompleted`; a late `StockRejected` | still one `ChargePayment`; saga `COMPLETED`, order `CONFIRMED`; no DLT record |

**Each guard removed in turn:**

| Code change | Result |
|---|---|
| none | pass |
| inbox check removed | pass |
| `AND state = :from` removed from `transition` | fail: `ReserveStock, ChargePayment, ChargePayment` |
| both removed | fail |

In the orchestrator the state machine is the guard that matters. It catches a repeat whatever its id, and the inbox
only catches a repeat with the same id. The inbox still saves work: a repeat stops before the payload is read and before
any saga query. Without the state guard, the copy with a new id sent a second `ChargePayment`. Payment's
`insertIfAbsent` would then refuse the second charge, a third guard in another service, but the saga should not rely on
it.

Replies with a new id for the same step are real. A timeout resends `ReleaseStock` or `RefundPayment` with a new command
id, and each one gets its own `StockReleased` or `PaymentRefunded`. The test uses `StockReserved` because it is the
shortest path to show the guard.

## Schemas 1: Schema Registry

Confluent Schema Registry runs in Docker Compose, on host port 8085 because 8081 is order-service. It keeps every
schema in the compacted Kafka topic `_schemas`, so it has no database of its own. It creates that topic itself, with
replication factor 3.

**Where the schema is enforced in this project.** The services do not produce to Kafka, Debezium does. Three options:

| Option | Who registers the schema | Problem |
|---|---|---|
| Avro converter in Debezium | Debezium, from a schema guessed per message with `table.expand.json.payload` | the contract is inferred from data, not designed; a `null` or a whole number gives a new version |
| Serialize in the service | the service, when it writes the outbox row | the payload in the database becomes binary |
| Check in CI only | nobody at run time | nothing is stopped at run time |

The plan is the second option, with Avro, and first for one message type only: `StockReserved`. The outbox payload
becomes bytes that Debezium passes through unchanged.

**Several types on one topic.** `inventory` carries `StockReserved`, `StockRejected` and `StockReleased`. The default
subject naming, `TopicNameStrategy`, allows one schema per topic. `RecordNameStrategy` gives each record type its own
subject instead.

**Compatibility.** The default level is `BACKWARD`: a consumer with the new schema must be able to read data written
with the old one. Checked by hand on a probe subject, deleted afterwards:

- Adding a field with a default: `is_compatible: true`.
- Adding a field without a default: `is_compatible: false`, `READER_FIELD_MISSING_DEFAULT_VALUE`. Registering it
  returned HTTP 409.

So a breaking change is refused when it is registered, before any message is written with it.

## Schemas 2: StockReserved in Avro

The first message with a registered schema. Written and read in one step, because a written-only change would leave
order-service unable to read `StockReserved` and stop every saga.

**Inventory writes it.**

- `avro/StockReserved.avsc` is the contract: `eventId`, `occurredAt`, `orderId`.
- `AvroEncoder` builds a record and runs `KafkaAvroSerializer`. The result is the standard wire format: byte `0`, a
  4-byte schema id, then the Avro data. The serializer registers the schema on first use, with
  `RecordNameStrategy`, so the subject is `com.ecommerce.inventory.StockReserved`.
- Migration `V6` turns `outbox_events.payload` into `BYTEA`. JSON replies keep their text as UTF-8 bytes, so their
  consumers see the same bytes as before.

**Debezium passes the bytes through.** The inventory connector uses `ByteArrayConverter`, so the payload reaches Kafka
unchanged. That converter accepts only bytes, and the heartbeat records are structs, so a `Filter` drops every record
that is not from the outbox table. The slot still advances: its `confirmed_flush_lsn` moved while the service was idle.
The heartbeat topics of inventory are no longer written.

**Order-service reads it.** The inventory listener now takes bytes. For `StockReserved` it uses `AvroDecoder`, which
reads with the writer's schema from the registry, found by the id in the message. Order-service keeps no copy of the
schema and reads `orderId` by name, so a field added later does not break it. The other replies are still read as
JSON.

**A registry call inside a database transaction.** The first `StockReserved` needs the registry, to
register the schema and get its id. After that the id is cached. If the registry is down at the first use, the
transaction should fail and the command be retried, then go to the DLT. This was not tested. In production `auto.register.schemas` is usually off, and the schema is
registered by the build, so a service cannot publish a schema nobody reviewed.

**Tests.** `mock://` is an in-memory registry from Confluent's library, so the tests need no registry container. The
order-service test now sends `StockReserved` in Avro. All 8 tests pass.

**Observed:**

- Success, payment failure and no stock: `CONFIRMED`, `CANCELLED`, `CANCELLED`, with nothing new on any DLT.
- One subject, `com.ecommerce.inventory.StockReserved`, version 1.
- A `StockReserved` record on the topic started with byte `0` and schema id `2`. The `StockReleased` records next to it
  were still plain JSON.

## Schemas 3: evolving StockReserved, and a bug from Schemas 2

Each change below was made in inventory only. Order-service was not rebuilt or restarted between them.

| Change | Compatibility | Registered | Order |
|---|---|---|---|
| none | `BACKWARD` (default) | version 1 | `CONFIRMED` |
| add `warehouse` with default `"main"` | `BACKWARD` | version 2 | `CONFIRMED` |
| remove `orderId` | `BACKWARD` | version 3, accepted | stuck, then `CANCELLED` by the timeout |
| remove `orderId` | `FULL_TRANSITIVE` | refused | `CANCELLED` by the timeout |

**`BACKWARD` protects the wrong side here.** It asks: can a reader with the new schema read old data? A removed field
passes that test. But the reader that matters is order-service, which still expects `orderId`. It got
`AvroRuntimeException` and the reply went to the DLT. For events read by other services, which upgrade on their own
schedule, the question is also the reverse: can an old reader read new data? That is `FORWARD`. `FULL` is both, and
`_TRANSITIVE` checks against every earlier version, not only the last one.

The registry is now started with `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: full_transitive`. With it, the same
removal failed in inventory with `Error registering Avro schema`, the command went to `inventory-commands-dlt`, and no
message with the bad schema was written.

**Version 3 had to be deleted first.** The registry answers a schema it already has with its existing id, without a
compatibility check. Versions 2 and 3 were test changes and are not in the code: the schema file is back to the
original, and version 3 was deleted from the registry.

`AvroEncoder` now uses `GenericRecordBuilder`, which fills every field not set with its default from the schema. With
`GenericData.Record`, a new field with a default would still have been null and failed to serialize.

**Bug found: order-service could not write dead letters.** Since Schemas 2 the inventory listener reads `byte[]`, but
the dead letter publisher used the string template. Publishing failed with `Can't convert value of class [B`, the
record was never recovered, and it was retried again and again. Its partition was blocked, so the timeout's
`StockReleased` waited behind it too.

- Fix: `DeadLetterPublishingRecoverer` now gets one template per value type, `byte[]` and everything else.
- `AvroRuntimeException` is not retried. A record that does not match the reader stays that way.
- New test `unreadableAvroReplyGoesToTheDlt`. With the old configuration it failed, and the other two tests failed too,
  because the blocked partition held their replies.
- The DLT checks in the order-service test now compare with the count before each test, so a test may write to a DLT
  on purpose.

**Lesson:** changing a listener's value type also changes what its error handler has to write back.

