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
