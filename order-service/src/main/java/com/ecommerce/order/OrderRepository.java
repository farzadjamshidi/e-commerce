package com.ecommerce.order;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

interface OrderRepository extends JpaRepository<Order, UUID> {

    // Only PENDING can move: a final status is never overwritten by a late or repeated event.
    // The version is bumped so JPA's optimistic locking still sees the change.
    @Modifying
    @Query(value = """
            UPDATE orders SET status = :status, version = version + 1, updated_at = now()
            WHERE id = :id AND status = 'PENDING'""", nativeQuery = true)
    int completePending(UUID id, String status);

    @Query(value = """
            SELECT id FROM orders WHERE status = 'PENDING' AND created_at < :before
            ORDER BY created_at LIMIT :limit""", nativeQuery = true)
    List<UUID> findPendingCreatedBefore(Instant before, int limit);
}
