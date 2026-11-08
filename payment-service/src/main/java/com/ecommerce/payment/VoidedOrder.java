package com.ecommerce.payment;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

// Mapped only so the schema is validated. Rows are written with native SQL by VoidedOrderRepository.
@Entity
@Table(name = "voided_orders")
class VoidedOrder {

    @Id
    private UUID orderId;

    @Column(nullable = false, insertable = false, updatable = false)
    private Instant voidedAt;

    protected VoidedOrder() {
    }
}
