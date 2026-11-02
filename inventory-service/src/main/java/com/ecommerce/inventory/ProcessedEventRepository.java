package com.ecommerce.inventory;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.UUID;

interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {

    // Returns 1 for a new event, 0 for a duplicate. No exception, so the transaction stays usable.
    @Modifying
    @Query(value = """
            INSERT INTO processed_events (event_id, event_type) VALUES (:eventId, :eventType)
            ON CONFLICT (event_id) DO NOTHING""", nativeQuery = true)
    int markProcessed(UUID eventId, String eventType);
}
