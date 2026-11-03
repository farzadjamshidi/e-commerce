package com.ecommerce.inventory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.util.UUID;

// Only the fields this service needs. Unknown fields are ignored, so the producer can add new ones.
@JsonIgnoreProperties(ignoreUnknown = true)
record OrderCreatedEvent(UUID orderId, String customerId, String sku, int quantity, BigDecimal totalAmount,
                         String currency) {

    static final String TYPE = "OrderCreated";
}
