package com.ecommerce.payment;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.math.BigDecimal;
import java.util.UUID;

interface PaymentRepository extends JpaRepository<Payment, UUID> {

    @Modifying
    @Query(value = """
            INSERT INTO payments (order_id, customer_id, amount, currency, status, reason)
            VALUES (:orderId, :customerId, :amount, :currency, :status, :reason)""", nativeQuery = true)
    void insert(UUID orderId, String customerId, BigDecimal amount, String currency, String status, String reason);
}
