package com.ecommerce.inventory;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "stock")
class Stock {

    @Id
    private String sku;

    @Column(nullable = false)
    private int available;

    @Column(nullable = false)
    private int reserved;

    @Column(nullable = false)
    private Instant updatedAt;

    protected Stock() {
    }

    String getSku() {
        return sku;
    }

    int getAvailable() {
        return available;
    }

    int getReserved() {
        return reserved;
    }

    Instant getUpdatedAt() {
        return updatedAt;
    }
}
