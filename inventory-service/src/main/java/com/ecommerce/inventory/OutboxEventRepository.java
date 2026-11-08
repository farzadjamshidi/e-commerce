package com.ecommerce.inventory;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;

import java.util.UUID;

interface OutboxEventRepository extends Repository<OutboxEvent, UUID> {

    @Modifying
    @Query(value = """
            INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, traceparent)
            VALUES (:id, :aggregateType, :aggregateId, :eventType, :payload, :traceparent)""",
            nativeQuery = true)
    void insert(UUID id, String aggregateType, String aggregateId, String eventType, byte[] payload,
                String traceparent);

    @Modifying
    @Query(value = "DELETE FROM outbox_events WHERE id = :id", nativeQuery = true)
    void delete(UUID id);
}
