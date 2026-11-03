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

#### Choreography 1: inventory publishes the reservation result (branch `saga-choreography`)

The flow on this branch:

```
order:     OrderCreated       ─► orders
inventory: OrderCreated       ─► InventoryReserved | InventoryRejected   ─► inventory
payment:   InventoryReserved  ─► PaymentCompleted  | PaymentFailed       ─► payments
inventory: PaymentFailed      ─► release the reservation (compensation)
order:     PaymentCompleted   ─► CONFIRMED,  InventoryRejected | PaymentFailed ─► CANCELLED
```

This step is the inventory line. In the same transaction as the inbox row, the stock update and the
reservation row, `OrderEventHandler` writes one event through `OutboxWriter`:

| Event | When | Payload |
|---|---|---|
| `InventoryReserved` | Conditional `UPDATE stock` matched | `orderId`, `sku`, `quantity`, `customerId`, `totalAmount`, `currency` |
| `InventoryRejected` | No row matched | `orderId`, `sku`, `quantity`, `reason: INSUFFICIENT_STOCK` |

- Key = order ID (`aggregate_id`), so all events of one order are in one partition of `inventory`.
- `aggregate_type` is `Reservation`. The topic is fixed to `inventory` by the connector.
- Inventory's `OrderCreatedEvent` record now also reads `customerId`, `totalAmount`, `currency`.

**Decision: `InventoryReserved` carries the amount (event-carried state transfer).** Payment needs the
amount to charge. The alternative is that payment also consumes `OrderCreated` and keeps its own copy.
Those events are on two topics, and Kafka does not order across topics, so `InventoryReserved` could
arrive before payment has seen `OrderCreated`. Payment would have to park one event and wait for the
other. Carrying the data avoids that, at a cost: inventory now forwards fields it does not use, only
because payment needs them. Each new need of a later step changes an earlier step. This is the
choreography coupling the orchestration branch avoids, because its `ChargePayment` command is built by
the orchestrator.

**Observed:**

- laptop x1 → `InventoryReserved` with `totalAmount: 1499.99`, phone x5 → `InventoryRejected` with
  `reason: INSUFFICIENT_STOCK`. Both keyed by the order ID. `inventory` topic offsets went from 1 to 3
  (1 is the step 3 smoke test).
- Offsets of `inventory-service` reset to earliest, whole `orders` topic replayed: 74 duplicates, 0
  reservations, `inventory` topic still at 3. Because the event is written in the business transaction,
  the inbox also stops duplicate outgoing events.
