package com.ecommerce.payment;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;

import java.util.UUID;

interface OutboxEventRepository extends Repository<OutboxEvent, UUID> {

    @Modifying
    @Query(value = """
            INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload)
            VALUES (:id, :aggregateType, :aggregateId, :eventType, CAST(:payload AS jsonb))""", nativeQuery = true)
    void insert(UUID id, String aggregateType, String aggregateId, String eventType, String payload);

    @Modifying
    @Query(value = "DELETE FROM outbox_events WHERE id = :id", nativeQuery = true)
    void delete(UUID id);
}
