package com.ecommerce.inventory;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;

interface StockRepository extends Repository<Stock, String> {

    // Check and change in one statement: no lost update between concurrent reservations
    @Modifying
    @Query(value = """
            UPDATE stock SET available = available - :quantity, reserved = reserved + :quantity, updated_at = now()
            WHERE sku = :sku AND available >= :quantity""", nativeQuery = true)
    int reserve(String sku, int quantity);
}
