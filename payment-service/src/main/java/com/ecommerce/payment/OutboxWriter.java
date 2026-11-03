package com.ecommerce.payment;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

// Debezium reads the INSERT from the WAL. The DELETE in the same transaction keeps the table empty.
@Component
class OutboxWriter {

    private final OutboxEventRepository outboxEventRepository;
    private final JsonMapper jsonMapper;

    OutboxWriter(OutboxEventRepository outboxEventRepository, JsonMapper jsonMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.jsonMapper = jsonMapper;
    }

    // Must join the caller's transaction, or the event could be sent without the business change
    @Transactional(propagation = Propagation.MANDATORY)
    void write(UUID eventId, String aggregateType, String aggregateId, String eventType, Object event) {
        outboxEventRepository.insert(eventId, aggregateType, aggregateId, eventType, jsonMapper.writeValueAsString(event));
        outboxEventRepository.delete(eventId);
    }
}
