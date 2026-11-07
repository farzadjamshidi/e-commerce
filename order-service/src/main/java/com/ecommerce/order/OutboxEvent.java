package com.ecommerce.order;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.springframework.data.domain.Persistable;

@Entity
@Table(name = "outbox_events")
class OutboxEvent implements Persistable<UUID> {

    @Id
    private UUID id;

    // Filled by the database (identity column), never written by Hibernate
    @Column(insertable = false, updatable = false)
    private Long position;

    @Column(nullable = false)
    private String destination;

    @Column(nullable = false)
    private String aggregateType;

    @Column(nullable = false)
    private String aggregateId;

    @Column(nullable = false)
    private String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false)
    private String payload;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    private Instant publishedAt;

    private String traceparent;

    // The id is preset, so Spring Data cannot tell a new event from a stored one by itself
    @Transient
    private boolean isNew = true;

    protected OutboxEvent() {
    }

    static OutboxEvent create(UUID eventId, String destination, String aggregateType, String aggregateId,
                              String eventType, String payload, Instant createdAt, String traceparent) {
        OutboxEvent event = new OutboxEvent();
        event.id = eventId;
        event.destination = destination;
        event.aggregateType = aggregateType;
        event.aggregateId = aggregateId;
        event.eventType = eventType;
        event.payload = payload;
        event.createdAt = createdAt;
        event.traceparent = traceparent;
        return event;
    }

    @Override
    public UUID getId() { return id; }

    @Override
    public boolean isNew() { return isNew; }

    @PostLoad
    @PostPersist
    void markNotNew() { isNew = false; }

    Long getPosition() { return position; }
    String getDestination() { return destination; }
    String getAggregateType() { return aggregateType; }
    String getAggregateId() { return aggregateId; }
    String getEventType() { return eventType; }
    String getPayload() { return payload; }
    Instant getCreatedAt() { return createdAt; }
    Instant getPublishedAt() { return publishedAt; }
}
