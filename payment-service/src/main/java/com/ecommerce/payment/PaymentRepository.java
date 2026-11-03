package com.ecommerce.payment;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

interface PaymentRepository extends JpaRepository<Payment, UUID> {

    // Returns 0 if the order already has a row: charged before, or cancelled before the charge
    @Modifying
    @Query(value = """
            INSERT INTO payments (order_id, customer_id, amount, currency, status, reason)
            VALUES (:orderId, :customerId, :amount, :currency, :status, :reason)
            ON CONFLICT (order_id) DO NOTHING""", nativeQuery = true)
    int insertIfAbsent(UUID orderId, String customerId, BigDecimal amount, String currency, String status,
                       String reason);

    // One statement for both orders of arrival. No row yet: insert CANCELLED, so a later charge is blocked.
    // COMPLETED: mark REFUNDED. FAILED, REFUNDED or CANCELLED: nothing to do, no row returned.
    // A concurrent insert of the same order waits on the primary key, so the two cannot both win.
    @Query(value = """
            INSERT INTO payments (order_id, status, reason) VALUES (:orderId, 'CANCELLED', :reason)
            ON CONFLICT (order_id) DO UPDATE SET status = 'REFUNDED', reason = :reason, updated_at = now()
            WHERE payments.status = 'COMPLETED'
            RETURNING status""", nativeQuery = true)
    Optional<String> cancel(UUID orderId, String reason);
}
