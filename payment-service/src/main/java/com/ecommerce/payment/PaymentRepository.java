package com.ecommerce.payment;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.math.BigDecimal;
import java.util.UUID;

interface PaymentRepository extends JpaRepository<Payment, UUID> {

    // Returns 0 if the order already has a payment: a second charge changes nothing
    @Modifying
    @Query(value = """
            INSERT INTO payments (order_id, customer_id, amount, currency, status, reason)
            VALUES (:orderId, :customerId, :amount, :currency, :status, :reason)
            ON CONFLICT (order_id) DO NOTHING""", nativeQuery = true)
    int insertIfAbsent(UUID orderId, String customerId, BigDecimal amount, String currency, String status,
                       String reason);

    // Only a COMPLETED payment is refunded, so a second refund changes nothing
    @Modifying
    @Query(value = """
            UPDATE payments SET status = 'REFUNDED', updated_at = now()
            WHERE order_id = :orderId AND status = 'COMPLETED'""", nativeQuery = true)
    int refund(UUID orderId);
}
