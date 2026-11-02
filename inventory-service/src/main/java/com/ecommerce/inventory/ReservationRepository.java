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
}
