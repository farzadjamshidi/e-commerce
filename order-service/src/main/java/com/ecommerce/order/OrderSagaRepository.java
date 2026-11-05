package com.ecommerce.order;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

interface OrderSagaRepository extends Repository<OrderSaga, UUID> {

    @Modifying
    @Query(value = "INSERT INTO order_sagas (order_id, state) VALUES (:orderId, :state)", nativeQuery = true)
    void insert(UUID orderId, String state);

    // The saga's state machine: a step is taken only from the state that expects it.
    // Returns 0 for a late or repeated reply, which then changes nothing.
    @Modifying
    @Query(value = """
            UPDATE order_sagas SET state = :to, updated_at = now()
            WHERE order_id = :orderId AND state = :from""", nativeQuery = true)
    int transition(UUID orderId, String from, String to);

    @Query(value = """
            SELECT order_id FROM order_sagas WHERE state = :state AND updated_at < :before
            ORDER BY updated_at LIMIT :limit""", nativeQuery = true)
    List<UUID> findStuck(String state, Instant before, int limit);
}
