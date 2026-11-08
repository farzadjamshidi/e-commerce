package com.ecommerce.inventory;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

// Debezium reads the INSERT from the WAL. The DELETE in the same transaction keeps the table empty.
@Component
class OutboxWriter {

    private final OutboxEventRepository outboxEventRepository;
    private final JsonMapper jsonMapper;
    private final Tracer tracer;
    private final Propagator propagator;

    OutboxWriter(OutboxEventRepository outboxEventRepository, JsonMapper jsonMapper, Tracer tracer,
                 Propagator propagator) {
        this.outboxEventRepository = outboxEventRepository;
        this.jsonMapper = jsonMapper;
        this.tracer = tracer;
        this.propagator = propagator;
    }

    // Must join the caller's transaction, or the event could be sent without the business change
    @Transactional(propagation = Propagation.MANDATORY)
    void write(UUID eventId, String aggregateType, String aggregateId, String eventType, Object event) {
        writeBytes(eventId, aggregateType, aggregateId, eventType, jsonMapper.writeValueAsBytes(event));
    }

    // For a payload already serialized, such as Avro
    @Transactional(propagation = Propagation.MANDATORY)
    void writeBytes(UUID eventId, String aggregateType, String aggregateId, String eventType, byte[] payload) {
        outboxEventRepository.insert(eventId, aggregateType, aggregateId, eventType, payload, traceparent());
        outboxEventRepository.delete(eventId);
    }

    // Debezium produces the Kafka record in another process, so the context has to travel in the row
    private String traceparent() {
        Span span = tracer.currentSpan();
        if (span == null) {
            return null;
        }
        Map<String, String> headers = new HashMap<>();
        propagator.inject(span.context(), headers, Map::put);
        return headers.get("traceparent");
    }
}
