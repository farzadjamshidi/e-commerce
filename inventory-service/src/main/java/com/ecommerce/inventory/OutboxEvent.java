package com.ecommerce.inventory;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

// Mapped only so the schema is validated. Rows are written with native SQL by OutboxWriter.
@Entity
@Table(name = "outbox_events")
class OutboxEvent {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String aggregateType;

    @Column(nullable = false)
    private String aggregateId;

    @Column(nullable = false)
    private String eventType;

    @Column(nullable = false)
    private byte[] payload;

    @Column(nullable = false)
    private Instant createdAt;

    private String traceparent;

    protected OutboxEvent() {
    }
}
