package com.ecommerce.order;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

// Replaced by Debezium CDC. Kept so the two approaches can be compared.
@Component
@Profile("!bench")
@ConditionalOnBooleanProperty("outbox.relay.enabled")
class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final long RELAY_LOCK_KEY = 4_242_001L;
    private static final Map<String, String> TOPIC_BY_AGGREGATE = Map.of("Order", "orders");

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int batchSize;

    OutboxRelay(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate,
                @Value("${outbox.relay.batch-size:100}") int batchSize) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.poll-interval-ms:500}")
    @Transactional
    public void publishPending() {
        // Only one relay at a time across all instances, so events leave in commit order
        if (!outboxEventRepository.tryAdvisoryLock(RELAY_LOCK_KEY)) {
            return;
        }

        List<OutboxEvent> events = outboxEventRepository.findUnpublished(batchSize);
        if (events.isEmpty()) {
            return;
        }

        CompletableFuture<?>[] sends = events.stream()
                .map(event -> kafkaTemplate.send(toRecord(event)))
                .toArray(CompletableFuture[]::new);

        // No timeout of our own: the producer completes every send within delivery.timeout.ms.
        // Giving up earlier would leave records in flight that may still be delivered.
        CompletableFuture.allOf(sends).join();
        outboxEventRepository.markPublished(events.stream().map(OutboxEvent::getId).toList(), Instant.now());

        log.info("Published {} outbox events, positions {}..{}",
                events.size(), events.getFirst().getPosition(), events.getLast().getPosition());
    }

    private static ProducerRecord<String, String> toRecord(OutboxEvent event) {
        String topic = TOPIC_BY_AGGREGATE.get(event.getAggregateType());
        if (topic == null) {
            throw new IllegalStateException("No topic for aggregate type " + event.getAggregateType());
        }
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, event.getAggregateId(), event.getPayload());
        record.headers().add("eventId", event.getId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("eventType", event.getEventType().getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
