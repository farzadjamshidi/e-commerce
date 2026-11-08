package com.ecommerce.payment;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;

import java.util.UUID;

interface VoidedOrderRepository extends Repository<VoidedOrder, UUID> {

    @Modifying
    @Query(value = "INSERT INTO voided_orders (order_id) VALUES (:orderId) ON CONFLICT (order_id) DO NOTHING",
            nativeQuery = true)
    void insert(UUID orderId);

    boolean existsByOrderId(UUID orderId);
}
