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
