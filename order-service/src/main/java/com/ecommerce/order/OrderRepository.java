package com.ecommerce.order;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

interface OrderRepository extends JpaRepository<Order, UUID> {

    // Only PENDING can move, so a final status is never overwritten.
    // The version is bumped by hand, because a native update bypasses @Version.
    @Modifying
    @Query(value = """
            UPDATE orders SET status = :status, version = version + 1, updated_at = now()
            WHERE id = :id AND status = 'PENDING'""", nativeQuery = true)
    int completePending(UUID id, String status);
}
