package com.ecommerce.inventory;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;

import java.util.UUID;

interface ReservationRepository extends Repository<Reservation, UUID> {

    @Modifying
    @Query(value = """
            INSERT INTO reservations (order_id, sku, quantity, status)
            VALUES (:orderId, :sku, :quantity, 'RESERVED')""", nativeQuery = true)
    void insertReserved(UUID orderId, String sku, int quantity);

    // Compensation in one statement: only a RESERVED row is released, so a second release changes nothing.
    // Returns 1 if stock was given back, 0 if there was nothing to release.
    @Modifying
    @Query(value = """
            WITH released AS (
                UPDATE reservations SET status = 'RELEASED', updated_at = now()
                WHERE order_id = :orderId AND status = 'RESERVED'
                RETURNING sku, quantity)
            UPDATE stock s SET available = s.available + r.quantity, reserved = s.reserved - r.quantity,
                               updated_at = now()
            FROM released r WHERE s.sku = r.sku""", nativeQuery = true)
    int release(UUID orderId);
}
