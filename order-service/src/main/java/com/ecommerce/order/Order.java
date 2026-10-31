package com.ecommerce.order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "orders")
class Order {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String customerId;

    @Column(nullable = false)
    private String sku;

    @Column(nullable = false)
    private int quantity;

    @Column(nullable = false)
    private BigDecimal totalAmount;

    @Column(nullable = false, columnDefinition = "bpchar(3)")
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;

    // Wrapper type: null tells Spring Data the entity is new, even though the id is set
    @Version
    private Long version;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    protected Order() {
    }

    static Order place(String customerId, String sku, int quantity, BigDecimal unitPrice, String currency) {
        Order order = new Order();
        order.id = UUID.randomUUID();
        order.customerId = customerId;
        order.sku = sku;
        order.quantity = quantity;
        order.totalAmount = unitPrice.multiply(BigDecimal.valueOf(quantity));
        order.currency = currency;
        order.status = OrderStatus.PENDING;
        order.createdAt = Instant.now();
        order.updatedAt = order.createdAt;
        return order;
    }

    UUID getId() { return id; }
    String getCustomerId() { return customerId; }
    String getSku() { return sku; }
    int getQuantity() { return quantity; }
    BigDecimal getTotalAmount() { return totalAmount; }
    String getCurrency() { return currency; }
    OrderStatus getStatus() { return status; }
    Long getVersion() { return version; }
    Instant getCreatedAt() { return createdAt; }
    Instant getUpdatedAt() { return updatedAt; }
}
