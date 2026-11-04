package com.ecommerce.order;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;

import java.util.UUID;

interface OrderSagaRepository extends Repository<OrderSaga, UUID> {

    @Modifying
    @Query(value = "INSERT INTO order_sagas (order_id, state) VALUES (:orderId, :state)", nativeQuery = true)
    void insert(UUID orderId, String state);
}
