package com.ecommerce.order;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    // Transaction-scoped lock, released automatically on commit or rollback
    @Query(value = "SELECT pg_try_advisory_xact_lock(:key)", nativeQuery = true)
    boolean tryAdvisoryLock(long key);

    @Query(value = "SELECT * FROM outbox_events WHERE published_at IS NULL ORDER BY position LIMIT :limit",
            nativeQuery = true)
    List<OutboxEvent> findUnpublished(int limit);

    @Modifying
    @Query("UPDATE OutboxEvent e SET e.publishedAt = :publishedAt WHERE e.id IN :ids")
    int markPublished(Collection<UUID> ids, Instant publishedAt);
}
