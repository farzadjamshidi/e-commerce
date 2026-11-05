package com.ecommerce.order;

import java.time.Instant;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Component
class OutboxWriter {

    private static final String AGGREGATE_TYPE = "Order";

    private final OutboxEventRepository outboxEventRepository;
    private final JsonMapper jsonMapper;
    private final boolean relayEnabled;

    OutboxWriter(OutboxEventRepository outboxEventRepository, JsonMapper jsonMapper,
                 @Value("${outbox.relay.enabled:false}") boolean relayEnabled) {
        this.outboxEventRepository = outboxEventRepository;
        this.jsonMapper = jsonMapper;
        this.relayEnabled = relayEnabled;
    }

    // Must join the caller's transaction, or the message could be sent without the business change
    @Transactional(propagation = Propagation.MANDATORY)
    void write(UUID messageId, String destination, UUID orderId, String messageType, Object message,
               Instant createdAt) {
        // Flush runs the INSERT now, which fires @PostPersist and clears isNew.
        // With a plain save, delete() would see a "new" entity and silently do nothing.
        OutboxEvent outboxEvent = outboxEventRepository.saveAndFlush(OutboxEvent.create(messageId, destination,
                AGGREGATE_TYPE, orderId.toString(), messageType, jsonMapper.writeValueAsString(message), createdAt));

        // Debezium reads the INSERT from the WAL, so the row itself is no longer needed.
        // The polling relay reads the table, so the row must stay while it is in use.
        if (!relayEnabled) {
            outboxEventRepository.delete(outboxEvent);
        }
    }
}
